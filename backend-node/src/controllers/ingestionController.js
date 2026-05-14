import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { insertRawDatasetRecord } from "../services/datasetRegistration.js";
import { logActivity } from "./activityController.js";
import { runConnectorPython } from "../utils/connectorPython.js";
import { validateFileSafety } from "../utils/sandboxValidator.js";
import { sanitizeFilename } from "../utils/fileUtils.js";

/**
 * ingestionController.js
 * =======================
 * The unified orchestrator for all data entry into DataInsights.ai.
 * This replaces both uploadController.js and connectorController.js.
 */

function buildPythonPayload(req, extra = {}) {
  let credentials = {};
  try {
    credentials = JSON.parse(req.body?.credentials || "{}");
  } catch {
    throw new Error("Invalid credentials JSON");
  }
  const payload = {
    ...extra,
    credentials,
  };
  const files = req.files;
  // Note: 'dataset' is used for direct uploads, 'file' is used for connector-based file imports
  const uploadFile = files?.dataset?.[0] || files?.file?.[0];
  
  if (uploadFile) {
    payload.file_base64 = uploadFile.buffer.toString("base64");
    payload.filename = uploadFile.originalname;
  }
  
  if (files?.service_account_file?.[0]) {
    payload.service_account_base64 = files.service_account_file[0].buffer.toString("base64");
  }
  return payload;
}

/**
 * Fetch the connector catalog with dynamic schemas.
 * Only Admins can access.
 */
export const getCatalog = async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ success: false, message: "Only administrators can view connectors." });
    }
    const result = await runConnectorPython({ action: "catalog" });
    if (!result.success) {
      return res.status(502).json({ success: false, message: result.message || "Failed to fetch catalog" });
    }
    return res.json({ success: true, data: result.data });
  } catch (e) {
    return res.status(502).json({ success: false, message: e.message });
  }
};

/**
 * Validate connector credentials and fetch available sources (e.g. tables, sheets).
 */
export const validate = async (req, res) => {
  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ success: false, message: "Only administrators can perform this action." });
    }
    
    // Safety check for uploaded files
    const uploadFile = req.files?.dataset?.[0] || req.files?.file?.[0];
    if (uploadFile && uploadFile.path) {
      await validateFileSafety(uploadFile.path, uploadFile.originalname);
    }

    const payload = buildPythonPayload(req, {
      action: "validate",
      connector: req.body.connector,
    });
    
    const result = await runConnectorPython(payload);
    if (!result.success) {
      return res.status(400).json({ success: false, message: result.message });
    }
    return res.json({ success: true, data: result.data });
  } catch (e) {
    return res.status(400).json({ success: false, message: e.message });
  }
};

/**
 * Final Import: Fetches data, saves to uploads/raw, and registers in DB.
 */
export const importData = async (req, res) => {
  const userEmail = req.user?.email || "default_user";
  const userId = req.user?.id || req.user?.userId || req.user?.user_id || null;
  const userName = req.user?.full_name || "Admin User";

  try {
    if (req.user?.role !== "admin") {
      return res.status(403).json({ success: false, message: "Admin access required." });
    }

    const datasetId = crypto.randomUUID();
    const connector = req.body.connector;
    const source = req.body.source || "default";

    // Hint for naming: connector_source.ext
    const hintBase = `${connector}_${source}`.replace(/[^\w.\-]+/g, "_").slice(0, 50);
    
    const payload = buildPythonPayload(req, {
      action: "fetch",
      connector,
      source,
      dataset_id: datasetId,
      output_name_hint: `${hintBase}.csv`,
    });

    // Python handles the fetch and writing the CSV to /uploads/raw/
    const py = await runConnectorPython(payload);
    if (!py.success) {
      throw new Error(py.message || "Python connector failed to fetch data.");
    }

    const finalFileName = py.file_name;
    const datasetName = py.dataset_display_name || source;
    
    // Stat file to get size
    const rawDir = path.resolve(process.cwd(), "..", "uploads", "raw");
    const absPath = path.join(rawDir, finalFileName);
    let fileSize = null;
    try {
      const st = await fs.stat(absPath);
      fileSize = st.size;
    } catch {
      console.warn("[INGEST] Could not stat file:", absPath);
    }

    // 1. Register in Database
    await insertRawDatasetRecord({
      userEmail,
      datasetId,
      datasetName,
      finalFileName,
      fileSize,
      uploadStatus: "not_cleaned",
    });

    // 2. Log Activity
    await logActivity({
      userId,
      userName,
      userEmail,
      eventType: "UPLOAD",
      eventDescription: `Imported via ${connector} (${source})`,
      datasetId,
      datasetName,
      status: "ok",
    });

    return res.status(200).json({
      success: true,
      datasetId,
      message: "Import successful.",
      redirect: "/admin/datasets", // Standard redirect target
      data: {
        id: datasetId,
        name: datasetName,
        fileName: finalFileName,
        size: fileSize
      }
    });

  } catch (e) {
    console.error("[INGEST ERROR]", e);
    return res.status(500).json({ success: false, message: e.message });
  }
};
