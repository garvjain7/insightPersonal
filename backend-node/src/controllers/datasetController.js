/**
 * datasetController.js
 * =====================
 * All dataset-related HTTP handlers for DataInsights.ai.
 *
 * Cleaning flow (new architecture):
 *   POST /api/datasets/:id/cleaning/init        → initCleaningWorkspace
 *   POST /api/datasets/:id/cleaning/preview     → previewCleaningStep
 *   POST /api/datasets/:id/cleaning/apply       → applyCleaningStep
 *   POST /api/datasets/:id/cleaning/finalize    → finalizeDataset
 *   GET  /api/datasets/:id/cleaning/state       → getCleaningState
 *   POST /api/datasets/:id/cleaning/pause       → pauseCleaning
 *
 * Dataset CRUD / access:
 *   GET  /api/datasets                          → getAllDatasets
 *   GET  /api/datasets/admin                    → getAllDatasetsAdmin
 *   GET  /api/datasets/:id                      → getDatasetById
 *   GET  /api/datasets/:id/status               → getDatasetStatus
 *   POST /api/datasets/:id/assign               → assignDataset
 *   POST /api/datasets/:id/unassign             → unassignDataset
 *   GET  /api/datasets/:id/preview              → getDatasetPreview
 *   GET  /api/datasets/:id/download             → downloadDataset
 *   GET  /api/datasets/:id/assignments          → getDatasetAssignments
 *   PATCH /api/datasets/:id/status              → updateDatasetStatus
 *   GET  /api/datasets/:id/analysis             → getAnalysis
 *   GET  /api/datasets/:id/metrics              → getMetrics
 *   GET  /api/datasets/:id/dashboard            → getDashboardConfig
 *   DELETE /api/datasets/:id                    → deleteDataset
 *   POST /api/datasets/:id/train                → trainDataset
 *   GET  /api/datasets/available-to-request     → getAvailableDatasetsToRequest
 */

import path from "path";
import fs from "fs/promises";
import { createReadStream } from "fs";
import { parse } from "csv-parse";
import { pool } from "../config/db.js";
import {
  validateDatasetAccess,
  getDatasetPaths,
} from "../utils/accessUtils.js";
import { logCleaningActivity } from "./activityController.js";

// ── Uploads root (shared by all helpers) ─────────────────────────────────────
const UPLOADS_ROOT = path.resolve(process.cwd(), "..", "uploads");

// ── Path builders ─────────────────────────────────────────────────────────────

/**
 * All paths related to a dataset's cleaning workspace.
 * Raw files live at:  uploads/raw/{datasetId}_{datasetName}.csv
 * Temp workspace:     uploads/temp/{datasetId}/
 * Cleaned output:     uploads/cleaned/cleaned_{datasetId}.csv
 * ML artifacts:       uploads/artifacts/{datasetId}/
 */
function buildWorkspacePaths(datasetId) {
  const tempDir = path.join(UPLOADS_ROOT, "temp", String(datasetId));
  return {
    tempDir,
    working: path.join(tempDir, "current_working.csv"),
    preview: path.join(tempDir, "preview_output.csv"),
    state: path.join(tempDir, "state.json"),
    metadata: path.join(tempDir, "metadata.json"),
    cleaned: path.join(UPLOADS_ROOT, "cleaned", `cleaned_${datasetId}.csv`),
    artifacts: path.join(UPLOADS_ROOT, "artifacts", String(datasetId)),
  };
}

/**
 * Build the raw file path from the stored file_name column.
 * The file_name in DB already includes the datasetId prefix from the upload phase.
 */
function buildRawPath(datasetId, fileName) {
  if (!fileName) return null;
  // If fileName already looks like a full path, just return it (safety)
  if (path.isAbsolute(fileName)) return fileName;
  // Raw files are stored in uploads/raw/
  return path.join(UPLOADS_ROOT, "raw", fileName);
}

// ── Python runner ─────────────────────────────────────────────────────────────

/**
 * Spawn a Python child process, collect stdout/stderr, parse JSON result.
 * Returns the parsed JSON object (which always has a "status" field).
 */
async function runPython(scriptPath, args) {
  const { spawn } = await import("child_process");

  return new Promise((resolve) => {
    let stdout = "";
    let stderr = "";

    const proc = spawn("python", [scriptPath, ...args]);
    proc.stdout.on("data", (d) => (stdout += d.toString()));
    proc.stderr.on("data", (d) => (stderr += d.toString()));

    proc.on("close", (code) => {
      if (stderr) {
        console.error(`[Python stderr] ${scriptPath}:\n${stderr}`);
      }
      try {
        const result = JSON.parse(stdout.trim());
        resolve(result);
      } catch {
        resolve({
          status: "error",
          message:
            code !== 0
              ? `Python process exited with code ${code}`
              : "Failed to parse Python output",
          raw_output: stdout.slice(0, 500),
        });
      }
    });

    proc.on("error", (err) => {
      resolve({ status: "error", message: `spawn error: ${err.message}` });
    });
  });
}

const CLEANER_SCRIPT = path.resolve(
  process.cwd(),
  "..",
  "ml_engine",
  "pipeline",
  "cleaner.py"
);
const TRANSFORMER_SCRIPT = path.resolve(
  process.cwd(),
  "..",
  "ml_engine",
  "pipeline",
  "transformer.py"
);
const PIPELINE_SCRIPT = path.resolve(
  process.cwd(),
  "..",
  "ml_engine",
  "run_pipeline.py"
);

// ── Misc helpers ──────────────────────────────────────────────────────────────

const pathExists = async (p) => {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
};

/**
 * Determines dataset status based on real file system truth.
 * Rules:
 *   1. Cleaned: If /uploads/cleaned/cleaned_{id}.csv exists
 *   2. Cleaning: If /uploads/temp/{id}/ exists (and not cleaned)
 *   3. Not Cleaned: Otherwise
 */
const getDatasetStatusFromFile = async (datasetId) => {
  const wp = buildWorkspacePaths(datasetId);
  if (await pathExists(wp.cleaned)) return "cleaned";
  if (await pathExists(wp.tempDir)) return "cleaning";
  return "not_cleaned";
};

const countLines = (filePath) =>
  new Promise((resolve) => {
    let count = 0;
    createReadStream(filePath)
      .on("data", (chunk) => {
        for (let i = 0; i < chunk.length; ++i) if (chunk[i] === 10) ++count;
      })
      .on("end", () => resolve(count))
      .on("error", () => resolve(0));
  });

const getCSVHeaders = (filePath) =>
  new Promise((resolve) => {
    let settled = false;
    const finish = (h = []) => {
      if (!settled) {
        settled = true;
        resolve(h);
      }
    };
    const stream = createReadStream(filePath);
    stream.on("error", () => finish([]));
    const parser = stream.pipe(parse({ to_line: 1 }));
    parser.on("error", () => finish([]));
    (async () => {
      try {
        for await (const row of parser) {
          finish(row);
          return;
        }
        finish([]);
      } catch {
        finish([]);
      }
    })();
  });

/** Get the best file to display for a given dataset (cleaned > working > raw). */
async function resolveDisplayPath(datasetId, fileName) {
  const wp = buildWorkspacePaths(datasetId);
  const rawPath = buildRawPath(datasetId, fileName);

  if (await pathExists(wp.cleaned)) return { path: wp.cleaned, source: "cleaned" };
  if (await pathExists(wp.preview)) return { path: wp.preview, source: "preview" };
  if (await pathExists(wp.working)) return { path: wp.working, source: "working" };
  if (await pathExists(rawPath)) return { path: rawPath, source: "raw" };

  // Legacy: try getDatasetPaths from accessUtils for backward-compat
  try {
    const legacyPaths = getDatasetPaths(datasetId, fileName);
    if (legacyPaths?.cleaned && (await pathExists(legacyPaths.cleaned)))
      return { path: legacyPaths.cleaned, source: "cleaned" };
    if (legacyPaths?.raw && (await pathExists(legacyPaths.raw)))
      return { path: legacyPaths.raw, source: "raw" };
  } catch {}

  return { path: null, source: null };
}

/** Get user record by email (throws if not found). */
async function getUserByEmail(email) {
  const res = await pool.query(
    "SELECT user_id, role, full_name, company_id FROM users WHERE email = $1",
    [email]
  );
  if (res.rows.length === 0) throw Object.assign(new Error("User not found"), { statusCode: 404 });
  return res.rows[0];
}

/** Assert user has access to the dataset. Throws 403 if not. */
async function assertAccess(userId, datasetId, role) {
  const ok = await validateDatasetAccess(userId, datasetId, role);
  if (!ok) throw Object.assign(new Error("Access denied"), { statusCode: 403 });
}

/** Map a raw DB dataset row to the shape the React app expects. */
export function mapDatasetRow(row, statusOverride = null) {
  if (!row) return row;
  const schema =
    row.schema_json && typeof row.schema_json === "object"
      ? row.schema_json
      : {};
  return {
    ...row,
    name: row.dataset_name,
    filename: row.file_name,
    status: statusOverride || row.upload_status,
    size: row.file_size,
    uploaded_by:
      row.uploaded_by_name || row.uploaded_by_email || row.uploaded_by,
    rows_count:
      schema.rows_count ?? schema.total_rows ?? null,
    columns_count:
      schema.columns_count ?? schema.total_columns ?? null,
    version: 1,
    has_access: row.has_access ?? true,
  };
}

// ═════════════════════════════════════════════════════════════════════════════
// CLEANING FLOW
// ═════════════════════════════════════════════════════════════════════════════

/**
 * POST /api/datasets/:id/cleaning/init
 *
 * Called once when the user opens the cleaning wizard for the first time
 * (or after re-uploading).
 *
 * 1. Locates the raw CSV
 * 2. Calls cleaner.py to: detect header, strip whitespace, write current_working.csv,
 *    compute quality score, initialise state.json + metadata.json
 * 3. Updates DB status to "cleaning"
 * 4. Returns quality score and column profile to React
 *
 * Idempotent: if workspace already exists, returns its current state instead
 * of reinitialising (unless force=true in request body).
 */
export const initCleaningWorkspace = async (req, res) => {
  const datasetId = req.params.id;
  const { force = false } = req.body || {};
  const userEmail = req.user?.email;

  try {
    const user = await getUserByEmail(userEmail);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT file_name, dataset_name, upload_status FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const { file_name, dataset_name, upload_status } = dsRes.rows[0];
    const wp = buildWorkspacePaths(datasetId);

    // ── Idempotency: if workspace already exists, return its state ────────────
    if (!force && (await pathExists(wp.working)) && (await pathExists(wp.state))) {
      const stateRaw = await fs.readFile(wp.state, "utf-8");
      const metaRaw = await fs.readFile(wp.metadata, "utf-8").catch(() => "{}");
      return res.json({
        success: true,
        resumed: true,
        message: "Resumed existing cleaning session",
        state: JSON.parse(stateRaw),
        metadata: JSON.parse(metaRaw),
      });
    }

    // ── Locate raw file ───────────────────────────────────────────────────────
    const rawPath = buildRawPath(datasetId, file_name);
    if (!(await pathExists(rawPath))) {
      // Fallback: try legacy paths
      const legacyPaths = getDatasetPaths(datasetId, file_name);
      const legacyRaw = legacyPaths?.raw;
      if (!legacyRaw || !(await pathExists(legacyRaw))) {
        return res.status(404).json({
          success: false,
          message: "Raw dataset file not found on disk. Please re-upload the dataset.",
        });
      }
      // Copy to canonical raw path
      await fs.mkdir(path.join(UPLOADS_ROOT, "raw"), { recursive: true });
      await fs.copyFile(legacyRaw, rawPath);
    }

    // ── Create temp directory ─────────────────────────────────────────────────
    await fs.mkdir(wp.tempDir, { recursive: true });
    await fs.mkdir(path.join(wp.tempDir, "logs"), { recursive: true });

    // ── Call cleaner.py ───────────────────────────────────────────────────────
    const result = await runPython(CLEANER_SCRIPT, [
      "--raw_path", rawPath,
      "--dataset_id", String(datasetId),
      "--uploads_root", UPLOADS_ROOT,
    ]);

    if (result.status !== "success") {
      return res.status(500).json({
        success: false,
        message: result.message || "Workspace initialisation failed",
      });
    }

    // ── Update DB ─────────────────────────────────────────────────────────────
    const newSchema = {
      raw_stats: {
        totalRows: result.quality?.total_rows,
        totalNulls: result.quality?.total_nulls,
        totalDuplicates: result.quality?.total_duplicates,
        columnNulls: result.quality?.column_nulls,
        qualityScore: result.quality?.score,
      },
    };

    await pool.query(
      `UPDATE datasets
       SET upload_status = 'cleaning', schema_json = schema_json || $1::jsonb, updated_at = NOW()
       WHERE dataset_id = $2`,
      [JSON.stringify(newSchema), datasetId]
    );

    // ── Log activity ──────────────────────────────────────────────────────────
    await logCleaningActivity(
      user.user_id, user.full_name, userEmail,
      datasetId, dataset_name,
      "CLEAN_START", "ok", "Cleaning workspace initialised"
    );

    return res.json({
      success: true,
      resumed: false,
      message: "Cleaning workspace ready",
      dataset_id: datasetId,
      header_row_detected: result.header_row_detected,
      quality: result.quality,
      columns: result.columns,
    });
  } catch (err) {
    console.error("initCleaningWorkspace error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message || "Server error" });
  }
};

/**
 * POST /api/datasets/:id/cleaning/preview
 *
 * Body: { step: 1-5, mode: "preview"|"skip", config: { type, params, ai: bool } }
 *
 * Runs the transform (or no-op for skip) against current_working.csv,
 * writes result to preview_output.csv, updates state to "previewed".
 * Does NOT touch current_working.csv.
 *
 * Returns preview statistics + decisions (for AI mode) to the UI.
 */
export const previewCleaningStep = async (req, res) => {
  const datasetId = req.params.id;
  const { step, mode = "preview", config = {} } = req.body;
  const userEmail = req.user?.email;

  if (!step || step < 1 || step > 5) {
    return res.status(400).json({ success: false, message: "step must be 1-5" });
  }
  if (!["preview", "skip"].includes(mode)) {
    return res.status(400).json({ success: false, message: "mode must be 'preview' or 'skip'" });
  }

  try {
    const user = await getUserByEmail(userEmail);
    await assertAccess(user.user_id, datasetId, user.role);

    const wp = buildWorkspacePaths(datasetId);

    if (!(await pathExists(wp.working))) {
      return res.status(400).json({
        success: false,
        message: "Cleaning workspace not initialised. Call /cleaning/init first.",
      });
    }

    const result = await runPython(TRANSFORMER_SCRIPT, [
      "--dataset_dir", wp.tempDir,
      "--mode", mode,
      "--step", String(step),
      "--config", JSON.stringify(config),
    ]);

    if (result.status !== "success") {
      return res.status(500).json({ success: false, message: result.message });
    }

    return res.json({ success: true, ...result });
  } catch (err) {
    console.error("previewCleaningStep error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

/**
 * POST /api/datasets/:id/cleaning/apply
 *
 * Body: { step: 1-5 }
 *
 * This is the ONLY place where preview_output.csv overwrites current_working.csv.
 * Invalidates all downstream steps (N+1 … 5) and returns the list so the
 * UI can show a warning and ask the user to redo them.
 *
 * ⚠  If step N was previously committed and the user re-applies, we show the
 *    upstream-change warning (handled by the invalidated_steps list in response).
 */
export const applyCleaningStep = async (req, res) => {
  const datasetId = req.params.id;
  const { step } = req.body;
  const userEmail = req.user?.email;

  if (!step || step < 1 || step > 5) {
    return res.status(400).json({ success: false, message: "step must be 1-5" });
  }

  try {
    const user = await getUserByEmail(userEmail);
    await assertAccess(user.user_id, datasetId, user.role);

    const wp = buildWorkspacePaths(datasetId);

    if (!(await pathExists(wp.preview))) {
      return res.status(400).json({
        success: false,
        message: "No preview to apply. Generate a preview or skip the step first.",
      });
    }

    const result = await runPython(TRANSFORMER_SCRIPT, [
      "--dataset_dir", wp.tempDir,
      "--mode", "apply",
      "--step", String(step),
    ]);

    if (result.status !== "success") {
      return res.status(500).json({ success: false, message: result.message });
    }

    // If downstream steps were invalidated, include a UI-friendly warning
    const hasInvalidations = result.invalidated_steps?.length > 0;

    return res.json({
      success: true,
      ...result,
      warning: hasInvalidations
        ? `Changing step ${step} has invalidated the following steps: ` +
          result.invalidated_steps.join(", ") +
          ". Please reapply them before finalising."
        : null,
    });
  } catch (err) {
    console.error("applyCleaningStep error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

/**
 * POST /api/datasets/:id/cleaning/finalize
 *
 * Copies current_working.csv → /uploads/cleaned/cleaned_{id}.csv.
 * Updates DB status to "cleaned".
 * This is the ONLY place the final cleaned file is written.
 *
 * After this, run_pipeline.py can be triggered.
 */
export const finalizeDataset = async (req, res) => {
  const datasetId = req.params.id;
  const userEmail = req.user?.email;

  try {
    const user = await getUserByEmail(userEmail);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT file_name, dataset_name FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const { file_name, dataset_name } = dsRes.rows[0];
    const wp = buildWorkspacePaths(datasetId);

    if (!(await pathExists(wp.working))) {
      return res.status(400).json({
        success: false,
        message: "No working dataset found. Please complete the cleaning wizard first.",
      });
    }

    // ── Call transformer finalize ─────────────────────────────────────────────
    const result = await runPython(TRANSFORMER_SCRIPT, [
      "--dataset_dir", wp.tempDir,
      "--mode", "finalize",
      "--dataset_id", String(datasetId),
      "--uploads_root", UPLOADS_ROOT,
    ]);

    if (result.status !== "success") {
      return res.status(500).json({ success: false, message: result.message });
    }

    // ── Update DB ─────────────────────────────────────────────────────────────
    await pool.query(
      "UPDATE datasets SET upload_status = 'cleaned', updated_at = NOW() WHERE dataset_id = $1",
      [datasetId]
    );

    // ── Log activity ──────────────────────────────────────────────────────────
    await logCleaningActivity(
      user.user_id, user.full_name, userEmail,
      datasetId, dataset_name,
      "CLEAN_DONE", "ok",
      `Cleaning finalised. Rows: ${result.final_rows}, Cols: ${result.final_cols}`
    );

    return res.json({
      success: true,
      message: "Dataset finalised and ready for ML pipeline",
      cleaned_path: result.cleaned_path,
      final_rows: result.final_rows,
      final_cols: result.final_cols,
    });
  } catch (err) {
    console.error("finalizeDataset error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

/**
 * GET /api/datasets/:id/cleaning/state
 *
 * Returns state.json + metadata.json for session recovery.
 * Called when the user re-opens the cleaning wizard on an in-progress dataset.
 * Also returns live stats from current_working.csv.
 */
export const getCleaningState = async (req, res) => {
  const datasetId = req.params.id;
  const userEmail = req.user?.email;

  try {
    const user = await getUserByEmail(userEmail);
    await assertAccess(user.user_id, datasetId, user.role);

    const wp = buildWorkspacePaths(datasetId);

    if (!(await pathExists(wp.tempDir))) {
      return res.json({
        success: true,
        initialised: false,
        message: "No cleaning session found. Start cleaning to initialise workspace.",
      });
    }

    // get_state from Python (reads state.json + metadata.json)
    const stateResult = await runPython(TRANSFORMER_SCRIPT, [
      "--dataset_dir", wp.tempDir,
      "--mode", "get_state",
    ]);

    if (stateResult.status !== "success") {
      return res.status(500).json({ success: false, message: stateResult.message });
    }

    // Also get live stats from working dataset
    let liveStats = null;
    if (stateResult.working_exists) {
      // Use cached stats from get_state result if available
      if (stateResult.stats) {
        liveStats = stateResult.stats;
      } else {
        const statsResult = await runPython(TRANSFORMER_SCRIPT, [
          "--dataset_dir", wp.tempDir,
          "--mode", "get_stats",
        ]);
        if (statsResult.status === "success") {
          liveStats = statsResult;
        }
      }
    }

    return res.json({
      success: true,
      initialised: true,
      state: stateResult.state,
      metadata: stateResult.metadata,
      working_exists: stateResult.working_exists,
      preview_exists: stateResult.preview_exists,
      live_stats: liveStats,
    });
  } catch (err) {
    console.error("getCleaningState error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

/**
 * POST /api/datasets/:id/cleaning/pause
 *
 * Logs that the user left the cleaning page.
 * The workspace (current_working.csv, state.json) is preserved for session recovery.
 * Does NOT change any files.
 */
export const pauseCleaning = async (req, res) => {
  const datasetId = req.params.id;
  const userEmail = req.user?.email;

  try {
    const userRes = await pool.query(
      "SELECT user_id, full_name, role FROM users WHERE email = $1",
      [userEmail]
    );
    const user = userRes.rows[0];
    if (!user) return res.status(401).json({ success: false, message: "User not found" });

    const dsRes = await pool.query(
      "SELECT dataset_name, upload_status FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    const dsName = dsRes.rows[0]?.dataset_name;
    const status = dsRes.rows[0]?.upload_status;

    if (status === "cleaning") {
      await logCleaningActivity(
        user.user_id, user.full_name, userEmail,
        datasetId, dsName,
        "CLEAN_PAUSE", "ok",
        "User left cleaning wizard — session preserved for recovery"
      );
    }

    return res.json({ success: true });
  } catch (err) {
    console.error("pauseCleaning error:", err);
    return res.status(500).json({ success: false });
  }
};

// ═════════════════════════════════════════════════════════════════════════════
// DATASET CRUD / ACCESS
// ═════════════════════════════════════════════════════════════════════════════

export const getAllDatasets = async (req, res) => {
  const userEmail = req.user?.email;
  if (!userEmail)
    return res.status(401).json({ success: false, message: "Authentication required" });

  try {
    const user = await getUserByEmail(userEmail);
    const isEmployee = user.role === "employee";

    let query = `SELECT d.*, u.full_name AS uploaded_by_name, u.email AS uploaded_by_email`;
    const params = [user.company_id];

    if (isEmployee) {
      query += `, TRUE AS has_access
        FROM datasets d
        LEFT JOIN users u ON d.uploaded_by = u.user_id
        INNER JOIN permissions p ON d.dataset_id = p.dataset_id
        WHERE d.company_id = $1 AND p.user_id = $2 AND p.can_view = TRUE
          AND d.upload_status != 'failed'`;
      params.push(user.user_id);
    } else {
      query += `
        FROM datasets d
        LEFT JOIN users u ON d.uploaded_by = u.user_id
        WHERE d.company_id = $1`;
    }

    query += " ORDER BY d.created_at DESC";

    const result = await pool.query(query, params);
    
    // Map with real file-system status
    const data = await Promise.all(result.rows.map(async (row) => {
      const realStatus = await getDatasetStatusFromFile(row.dataset_id);
      return mapDatasetRow({ ...row, has_access: isEmployee ? row.has_access : true }, realStatus);
    }));

    return res.json({
      success: true,
      count: result.rows.length,
      data,
    });
  } catch (err) {
    console.error("getAllDatasets error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};

export const getAllDatasetsAdmin = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT d.*, u.full_name AS uploaded_by_name, u.email AS uploaded_by_email, c.company_name
       FROM datasets d
       LEFT JOIN users u ON d.uploaded_by = u.user_id
       LEFT JOIN companies c ON d.company_id = c.company_id
       WHERE d.upload_status != 'failed'
       ORDER BY d.created_at DESC`
    );

    const data = await Promise.all(result.rows.map(async (row) => {
      const realStatus = await getDatasetStatusFromFile(row.dataset_id);
      return mapDatasetRow(row, realStatus);
    }));

    return res.json({
      success: true,
      count: result.rows.length,
      data,
    });
  } catch (err) {
    console.error("getAllDatasetsAdmin error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};

export const getDatasetById = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT d.*, u.full_name AS uploaded_by_name, u.email AS uploaded_by_email
       FROM datasets d
       LEFT JOIN users u ON d.uploaded_by = u.user_id
       WHERE d.dataset_id = $1`,
      [req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, req.params.id, user.role);

    const realStatus = await getDatasetStatusFromFile(req.params.id);

    return res.json({
      success: true,
      data: mapDatasetRow({ ...result.rows[0], has_access: true }, realStatus),
    });
  } catch (err) {
    console.error("getDatasetById error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

export const getDatasetStatus = async (req, res) => {
  const datasetId = req.params.id;
  try {
    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT upload_status FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    return res.json({ success: true, status: dsRes.rows[0].upload_status });
  } catch (err) {
    console.error("getDatasetStatus error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

export const assignDataset = async (req, res) => {
  const datasetId = req.params.id;
  const { userIds } = req.body;

  if (!Array.isArray(userIds) || userIds.length === 0)
    return res.status(400).json({ success: false, message: "userIds array is required" });

  try {
    const admin = await getUserByEmail(req.user?.email);

    for (const uid of userIds) {
      await pool.query(
        `INSERT INTO permissions (company_id, user_id, dataset_id, can_view, granted_by)
         VALUES ($1, $2, $3, TRUE, $4)
         ON CONFLICT (user_id, dataset_id)
         DO UPDATE SET can_view = TRUE, updated_at = NOW()`,
        [admin.company_id, uid, datasetId, admin.user_id]
      );
    }

    return res.json({
      success: true,
      message: `Successfully assigned ${userIds.length} users`,
    });
  } catch (err) {
    console.error("assignDataset error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};

export const unassignDataset = async (req, res) => {
  const { userId } = req.body;
  if (!userId)
    return res.status(400).json({ success: false, message: "userId is required" });

  try {
    await pool.query(
      "UPDATE permissions SET can_view = FALSE, updated_at = NOW() WHERE user_id = $1 AND dataset_id = $2",
      [userId, req.params.id]
    );
    return res.json({ success: true, message: "User access revoked successfully" });
  } catch (err) {
    console.error("unassignDataset error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};

/**
 * GET /api/datasets/:id/preview?page=1&pageSize=50
 *
 * Shows paginated rows from the "best available" file:
 *   - During cleaning: current_working.csv (so user sees live changes)
 *   - After finalise : cleaned_{id}.csv
 *   - Before cleaning: raw file
 *
 * Returns rawStats (baseline, cached in schema_json) and currentStats (live).
 */
export const getDatasetPreview = async (req, res) => {
  const datasetId = req.params.id;
  const userEmail = req.user?.email;
  const page = Math.max(1, parseInt(req.query.page) || 1);
  const pageSize = Math.min(200, Math.max(1, parseInt(req.query.pageSize) || 50));

  try {
    const user = await getUserByEmail(userEmail);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT file_name, upload_status, schema_json FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const { file_name, upload_status, schema_json } = dsRes.rows[0];
    const wp = buildWorkspacePaths(datasetId);

    // ── Resolve best file to display ──────────────────────────────────────────
    let targetPath = null;
    let dataSource = req.query.source; // Allow frontend to override (working | preview | cleaned | raw)

    if (dataSource === "preview" && (await pathExists(wp.preview))) targetPath = wp.preview;
    else if (dataSource === "working" && (await pathExists(wp.working))) targetPath = wp.working;
    else if (dataSource === "cleaned" && (await pathExists(wp.cleaned))) targetPath = wp.cleaned;
    else if (dataSource === "raw") {
      const resolved = await resolveDisplayPath(datasetId, file_name);
      targetPath = resolved.path;
    }

    // Auto-resolve if no valid source requested
    if (!targetPath) {
      if (["cleaning"].includes(upload_status)) {
        if (await pathExists(wp.preview)) {
          targetPath = wp.preview;
          dataSource = "preview";
        } else if (await pathExists(wp.working)) {
          targetPath = wp.working;
          dataSource = "working";
        }
      } 
      
      if (!targetPath) {
        if (["cleaned", "completed"].includes(upload_status) && (await pathExists(wp.cleaned))) {
          targetPath = wp.cleaned;
          dataSource = "cleaned";
        } else {
          const resolved = await resolveDisplayPath(datasetId, file_name);
          targetPath = resolved.path;
          dataSource = resolved.source;
        }
      }
    }

    if (!targetPath) {
      return res.status(404).json({
        success: false,
        message: "Dataset file not found on disk. Please re-upload.",
      });
    }

    // ── Read paginated rows ───────────────────────────────────────────────────
    const totalRows = await countLines(targetPath);
    const headers = await getCSVHeaders(targetPath);
    if (headers.length === 0)
      return res.status(404).json({ success: false, message: "Dataset file is empty or unreadable" });

    const fromLine = (page - 1) * pageSize + 2;
    const toLine = page * pageSize + 1;
    const rows = [];

    const parser = createReadStream(targetPath).pipe(
      parse({
        columns: headers,
        trim: true,
        skip_empty_lines: true,
        from_line: fromLine,
        to_line: toLine,
      })
    );
    for await (const record of parser) {
      rows.push(record);
    }

    // ── Raw stats (baseline — cached in schema_json) ───────────────────────────
    const rawStats = schema_json?.raw_stats || null;

    // ── Live stats from current file ──────────────────────────────────────────
    let currentStats = null;
    if (await pathExists(wp.working) || await pathExists(wp.cleaned)) {
      // Try to get cached stats from state.json first
      if (await pathExists(wp.state)) {
        try {
          const stateData = JSON.parse(await fs.readFile(wp.state, "utf-8"));
          // If we are looking at preview file, use preview_stats
          if (dataSource === "preview" && stateData.preview_stats) {
            currentStats = {
              totalRows: stateData.preview_stats.rows,
              totalCols: stateData.preview_stats.cols,
              totalNulls: stateData.preview_stats.total_nulls,
              totalDuplicates: stateData.preview_stats.total_duplicates || 0,
              columnNulls: stateData.preview_stats.column_nulls || {},
              is_preview: true
            };
          } else if (stateData.stats) {
            currentStats = {
              totalRows: stateData.stats.rows,
              totalCols: stateData.stats.cols,
              totalNulls: stateData.stats.total_nulls,
              totalDuplicates: stateData.stats.total_duplicates || 0,
              columnNulls: stateData.stats.column_nulls || {},
              is_preview: false
            };
          }
        } catch (e) {
          console.warn("Failed to read cached stats from state.json", e.message);
        }
      }

      // Fallback: only run get_stats if cache is missing and it's not a heavy pagination request
      if (!currentStats) {
        const statsResult = await runPython(TRANSFORMER_SCRIPT, [
          "--dataset_dir", wp.tempDir,
          "--mode", "get_stats",
        ]);
        if (statsResult.status === "success") {
          currentStats = {
            totalRows: statsResult.total_rows,
            totalNulls: statsResult.total_nulls,
            totalDuplicates: statsResult.total_duplicates,
            totalOutliers: statsResult.total_outliers,
            columnNulls: statsResult.column_nulls,
            columns: statsResult.columns,
          };
        }
      }
    }

    return res.json({
      success: true,
      data: rows,
      totalRows,
      currentPage: page,
      pageSize,
      totalRowsPreviewed: rows.length,
      dataSource,
      rawStats,
      currentStats,
    });
  } catch (err) {
    console.error("getDatasetPreview error:", err);
    return res.status(500).json({ success: false, message: "Failed to load preview" });
  }
};

/**
 * GET /api/datasets/:id/download
 * Downloads the best available file (cleaned > working > raw).
 */
export const downloadDataset = async (req, res) => {
  const datasetId = req.params.id;

  try {
    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT file_name, dataset_name, upload_status FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const { file_name, dataset_name, upload_status } = dsRes.rows[0];
    const { path: filePath } = await resolveDisplayPath(datasetId, file_name);

    if (!filePath) {
      return res.status(404).json({
        success: false,
        message: "Dataset file not found on disk.",
      });
    }

    res.download(filePath, dataset_name || file_name, (err) => {
      if (err && !res.headersSent)
        res.status(500).json({ success: false, message: "Download failed" });
    });
  } catch (err) {
    console.error("downloadDataset error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

export const getDatasetAssignments = async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT u.user_id, u.full_name, u.email, u.department, u.designation
       FROM users u
       JOIN permissions p ON u.user_id = p.user_id
       WHERE p.dataset_id = $1 AND p.can_view = TRUE`,
      [req.params.id]
    );
    return res.json({ success: true, users: result.rows });
  } catch (err) {
    console.error("getDatasetAssignments error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};

export const updateDatasetStatus = async (req, res) => {
  const { status } = req.body;
  const allowed = [
    "not_cleaned", "cleaning", "cleaned", "failed",
    "processing", "completed", "trained",
  ];

  if (!allowed.includes(status))
    return res.status(400).json({ success: false, message: `Invalid status: ${status}` });

  try {
    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, req.params.id, user.role);

    // "trained" is a legacy alias → store as "completed"
    const uploadStatus = status === "trained" ? "completed" : status;
    const result = await pool.query(
      "UPDATE datasets SET upload_status = $1, updated_at = NOW() WHERE dataset_id = $2 RETURNING dataset_id",
      [uploadStatus, req.params.id]
    );
    if (result.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    return res.json({ success: true, message: "Status updated" });
  } catch (err) {
    console.error("updateDatasetStatus error:", err);
    const status = err.statusCode || 500;
    return res.status(status).json({ success: false, message: err.message });
  }
};

/**
 * GET /api/datasets/:id/analysis
 *
 * Returns column-level analysis, quality score, and cleaning report.
 * Reads from cleaned file if available, then working, then raw.
 * Also reads profile_report.json and dataset_metadata.json from artifacts dir.
 */
export const getAnalysis = async (req, res) => {
  const datasetId = req.params.id;

  try {
    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT file_name, dataset_name FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const { file_name, dataset_name } = dsRes.rows[0];
    const wp = buildWorkspacePaths(datasetId);

    // ── Resolve analysis file ─────────────────────────────────────────────────
    let analysisPath = null;
    if (await pathExists(wp.cleaned)) analysisPath = wp.cleaned;
    else if (await pathExists(wp.working)) analysisPath = wp.working;
    else {
      const { path: p } = await resolveDisplayPath(datasetId, file_name);
      analysisPath = p;
    }

    if (!analysisPath) {
      return res.json({
        success: true,
        dataset_name: dataset_name || datasetId,
        row_count: 0, column_count: 0, quality_score: null,
        total_nulls: 0, duplicate_rows: 0, cleaning_report: [], columns: [],
      });
    }

    // ── Column profile from CSV ───────────────────────────────────────────────
    let columns = [];
    try {
      const csv = await fs.readFile(analysisPath, "utf-8");
      const lines = csv.split("\n").filter((l) => l.trim());
      if (lines.length > 1) {
        const headers = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
        const sampleLines = lines.slice(1, Math.min(101, lines.length));

        columns = headers.map((colName, idx) => {
          const colValues = sampleLines
            .map((line) => {
              const cells = line.split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
              return cells[idx];
            })
            .filter((v) => v !== "" && v !== undefined);

          const nullCount = sampleLines.length - colValues.length;
          const numericCount = colValues.filter(
            (v) => !isNaN(parseFloat(v)) && isFinite(Number(v))
          ).length;
          const isNumeric = numericCount > colValues.length * 0.7;
          const dateCount = colValues.filter(
            (v) => !isNaN(Date.parse(v)) && v.length > 6
          ).length;
          const isDateTime = dateCount > colValues.length * 0.5;
          const uniqueCount = new Set(colValues).size;

          return {
            name: colName,
            type: isNumeric ? "float64" : isDateTime ? "datetime" : "string",
            null_count: nullCount,
            null_pct: parseFloat(((nullCount / sampleLines.length) * 100).toFixed(2)),
            nunique: uniqueCount,
            sample: colValues.slice(0, 3),
            inferred_type: isNumeric
              ? "numeric"
              : isDateTime
              ? "datetime"
              : uniqueCount < 10
              ? "categorical"
              : "text",
          };
        });
      }
    } catch (err) {
      console.warn("getAnalysis: could not read CSV:", err.message);
    }

    // ── Metadata / quality score ──────────────────────────────────────────────
    const metadataPath = path.join(wp.artifacts, "dataset_metadata.json");
    let qualityScore = null;
    let cleaningStats = {
      missing_values_handled: 0,
      duplicates_removed: 0,
      outliers_removed: 0,
      data_type_fixes: 0,
    };

    try {
      const meta = JSON.parse(await fs.readFile(metadataPath, "utf-8"));
      qualityScore = meta.data_quality_score;
      cleaningStats.missing_values_handled = meta.missing_values_handled || 0;
      cleaningStats.duplicates_removed = meta.duplicates_removed || 0;
      cleaningStats.outliers_removed = meta.outliers_removed || 0;
      cleaningStats.data_type_fixes = meta.data_type_fixes || 0;
    } catch {}

    // Also try schema_json in DB for quality score (set during init)
    if (qualityScore === null) {
      const schemaRes = await pool.query(
        "SELECT schema_json FROM datasets WHERE dataset_id = $1",
        [datasetId]
      );
      qualityScore = schemaRes.rows[0]?.schema_json?.raw_stats?.qualityScore ?? null;
    }

    const totalNulls = columns.reduce((s, c) => s + (c.null_count || 0), 0);

    const cleaningReport = [
      {
        category: "Missing Values",
        count: cleaningStats.missing_values_handled,
        action: "Filled using MEAN/MEDIAN/MODE",
        reason: "Columns with missing values were imputed",
      },
      {
        category: "Duplicates",
        count: cleaningStats.duplicates_removed,
        action: "Removed duplicate rows",
        reason: "Identical rows detected in dataset",
      },
      {
        category: "Outliers",
        count: cleaningStats.outliers_removed,
        action: "Removed / capped using IQR method",
        reason: "Values outside 1.5×IQR range",
      },
      {
        category: "Data Types",
        count: cleaningStats.data_type_fixes,
        action: "Converted to standard format",
        reason: "Columns auto-converted to correct type",
      },
    ].filter((item) => item.count > 0);

    return res.json({
      success: true,
      dataset_name: dataset_name || datasetId,
      row_count: columns.length > 0 ? (await countLines(analysisPath)) - 1 : 0,
      column_count: columns.length,
      quality_score: qualityScore,
      total_nulls: totalNulls,
      duplicate_rows: cleaningStats.duplicates_removed,
      cleaning_report: cleaningReport,
      columns,
    });
  } catch (err) {
    console.error("getAnalysis error:", err);
    return res.status(500).json({ success: false, message: err.message });
  }
};

export const getMetrics = async (req, res) => {
  const datasetId = req.params.id;

  try {
    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT schema_json, dataset_name FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    if (dsRes.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found" });

    const { schema_json, dataset_name } = dsRes.rows[0];

    return res.json({
      success: true,
      dataset_name,
      rawStats: schema_json?.raw_stats || null,
      qualityScore: schema_json?.raw_stats?.qualityScore || null,
      cleaningSteps: schema_json?.cleaning_steps || [],
    });
  } catch (err) {
    console.error("getMetrics error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
};

export const getDashboardConfig = async (req, res) => {
  const datasetId = req.params.id;

  try {
    const user = await getUserByEmail(req.user?.email);
    await assertAccess(user.user_id, datasetId, user.role);

    const dsRes = await pool.query(
      "SELECT schema_json FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    const schemaJson = dsRes.rows[0]?.schema_json || {};

    if (schemaJson.dashboard_config) {
      return res.json({ success: true, ...schemaJson.dashboard_config });
    }

    return res.json({ success: true, charts: [], insights: [], executive_summary: "" });
  } catch (err) {
    console.error("getDashboardConfig error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
};

export const deleteDataset = async (req, res) => {
  const datasetId = req.params.id;

  try {
    const user = await getUserByEmail(req.user?.email);

    const dsCheck = await pool.query(
      "SELECT dataset_id, dataset_name FROM datasets WHERE dataset_id = $1 AND company_id = $2",
      [datasetId, user.company_id]
    );
    if (dsCheck.rows.length === 0)
      return res.status(404).json({ success: false, message: "Dataset not found or access denied" });

    await pool.query("DELETE FROM datasets WHERE dataset_id = $1", [datasetId]);

    // Remove all files: raw, temp workspace, cleaned, artifacts
    const wp = buildWorkspacePaths(datasetId);
    const rawDir = path.join(UPLOADS_ROOT, "raw");
    const dsRes = await pool.query(
      "SELECT file_name FROM datasets WHERE dataset_id = $1",
      [datasetId]
    ).catch(() => ({ rows: [] }));

    const toDelete = [
      wp.tempDir,
      wp.cleaned,
      wp.artifacts,
    ];

    for (const p of toDelete) {
      await fs.rm(p, { recursive: true, force: true }).catch(() => {});
    }

    // Also clean up any legacy ml_engine data dir
    const legacyDir = path.resolve(
      process.cwd(), "..", "ml_engine", "data", "users",
      req.user?.email || "unknown", datasetId
    );
    await fs.rm(legacyDir, { recursive: true, force: true }).catch(() => {});

    return res.json({ success: true, message: "Dataset deleted successfully" });
  } catch (err) {
    console.error("deleteDataset error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};

export const trainDataset = async (_req, res) => {
  return res.json({
    success: true,
    message: "Model training is automatically handled by the background ML pipeline after dataset finalization.",
  });
};

export const getAvailableDatasetsToRequest = async (req, res) => {
  try {
    const user = await getUserByEmail(req.user?.email);

    const result = await pool.query(
      `SELECT d.*, u.full_name AS uploaded_by_name
       FROM datasets d
       LEFT JOIN users u ON d.uploaded_by = u.user_id
       WHERE d.company_id = $1
         AND d.upload_status != 'failed'
         AND d.dataset_id NOT IN (
           SELECT dataset_id FROM permissions WHERE user_id = $2 AND can_view = TRUE
         )
       ORDER BY d.created_at DESC`,
      [user.company_id, user.user_id]
    );

    return res.json({ success: true, data: result.rows.map(mapDatasetRow) });
  } catch (err) {
    console.error("getAvailableDatasetsToRequest error:", err);
    return res.status(500).json({ success: false, message: "Server error" });
  }
};