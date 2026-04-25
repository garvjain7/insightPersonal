import path from "path";
import fs from "fs/promises";
import { pool } from "../config/db.js";
import { validateDatasetAccess, getDatasetPaths } from "../utils/accessUtils.js";
import { parse } from "csv-parse";
import { createReadStream } from "fs";

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/visualization/:id
// Returns dashboard config from the DB, or a graceful empty response.
// Previously searched for a dead ml_engine/data/users/... path.
// ─────────────────────────────────────────────────────────────────────────────
export const getVisualization = async (req, res) => {
  try {
    const datasetId = req.params.id;
    if (!datasetId) return res.status(400).json({ success: false, message: "Dataset ID required" });

    const userEmail = req.user?.email;
    if (!userEmail) return res.status(401).json({ success: false, message: "Authentication required" });

    // Access control via DB
    const userResult = await pool.query("SELECT user_id, role FROM users WHERE email = $1", [userEmail]);
    const user = userResult.rows[0];
    if (!user) return res.status(401).json({ success: false, message: "User not found" });

    if (!(await validateDatasetAccess(user.user_id, datasetId, user.role))) {
      return res.status(403).json({ success: false, message: "Access denied" });
    }

    // Read dashboard config from DB schema_json (set by cleaning/finalization pipeline)
    const dsResult = await pool.query(
      "SELECT schema_json, dataset_name FROM datasets WHERE dataset_id = $1",
      [datasetId]
    );
    const row = dsResult.rows[0];
    if (!row) return res.status(404).json({ success: false, message: "Dataset not found" });

    const schemaJson = row.schema_json || {};

    if (schemaJson.dashboard_config) {
      return res.json({ success: true, dataset_name: row.dataset_name, ...schemaJson.dashboard_config });
    }

    // No config yet — return empty gracefully (avoids frontend crash)
    return res.json({ success: true, dataset_name: row.dataset_name, charts: [], insights: [], executive_summary: "" });

  } catch (err) {
    console.error("[VISUALIZATION CONTROLLER]", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/chart-data/:id
// Returns chart data based on parameters
// ─────────────────────────────────────────────────────────────────────────────
export const getChartData = async (req, res) => {
  try {
    const datasetId = req.params.id;
    if (!datasetId) return res.status(400).json({ success: false, message: "Dataset ID required" });

    const userEmail = req.user?.email;
    if (!userEmail) return res.status(401).json({ success: false, message: "Authentication required" });

    // Access control
    const userResult = await pool.query("SELECT user_id, role FROM users WHERE email = $1", [userEmail]);
    const user = userResult.rows[0];
    if (!user) return res.status(401).json({ success: false, message: "User not found" });

    if (!(await validateDatasetAccess(user.user_id, datasetId, user.role))) {
      return res.status(403).json({ success: false, message: "Access denied" });
    }

    const { xAxis, yAxis, aggregation = 'sum', filters = {}, limit = 10 } = req.body;

    // Get cleaned data
    const cleanedPath = path.join(path.resolve(process.cwd(), '..', 'uploads'), 'cleaned', `cleaned_${datasetId}.csv`);
    try {
      await fs.access(cleanedPath);
    } catch {
      return res.status(404).json({ success: false, message: "Cleaned data not found" });
    }

    const data = [];
    const parser = createReadStream(cleanedPath).pipe(parse({ columns: true, skip_empty_lines: true }));
    for await (const row of parser) {
      data.push(row);
    }

    // Apply filters
    let filteredData = data;
    for (const [col, filterVal] of Object.entries(filters)) {
      if (Array.isArray(filterVal)) {
        filteredData = filteredData.filter(row => filterVal.includes(row[col]));
      } else if (filterVal.min !== undefined || filterVal.max !== undefined) {
        filteredData = filteredData.filter(row => {
          const val = parseFloat(row[col]);
          if (isNaN(val)) return false;
          if (filterVal.min !== undefined && val < filterVal.min) return false;
          if (filterVal.max !== undefined && val > filterVal.max) return false;
          return true;
        });
      }
    }

    // Group and aggregate
    const grouped = {};
    const counts = {};
    const maxs = {};
    const mins = {};
    const sums = {};

    for (const row of filteredData) {
      const key = row[xAxis] || 'Unknown';
      const val = row[yAxis];
      
      if (data[0] && typeof data[0][yAxis] === 'string') {
        // Categorical Y
        grouped[key] = (grouped[key] || 0) + 1;
        counts[key] = (counts[key] || 0) + 1;
      } else {
        // Numeric Y
        const numVal = parseFloat(val);
        if (!isNaN(numVal)) {
          grouped[key] = (grouped[key] || 0) + numVal;
          sums[key] = (sums[key] || 0) + numVal;
          counts[key] = (counts[key] || 0) + 1;
          maxs[key] = Math.max(maxs[key] || -Infinity, numVal);
          mins[key] = Math.min(mins[key] === undefined ? Infinity : mins[key], numVal);
        }
      }
    }

    const entries = Object.entries(grouped);
    const result = [];

    for (const [name, value] of entries) {
      result.push({
        name: String(name).substring(0, 18),
        value: data[0] && typeof data[0][yAxis] === 'string' ? value : 
          (aggregation === 'sum' ? Math.round((sums[name] || 0) * 100) / 100 : 
           aggregation === 'count' ? counts[name] || 0 :
           aggregation === 'avg' ? Math.round(((sums[name] || 0) / (counts[name] || 1)) * 100) / 100 :
           aggregation === 'max' ? Math.round((maxs[name] || 0) * 100) / 100 :
           aggregation === 'min' ? Math.round((mins[name] || 0) * 100) / 100 : value),
        rawValue: value,
        count: counts[name] || 0,
        max: maxs[name],
        min: mins[name]
      });
    }

    result.sort((a, b) => b.value - a.value);

    return res.json({
      success: true,
      data: result.slice(0, limit)
    });

  } catch (err) {
    console.error("[CHART DATA CONTROLLER]", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
};
export const getCleanedDatasets = async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(401).json({ success: false, message: "Authentication required" });

    // Get user
    const userResult = await pool.query("SELECT user_id, role FROM users WHERE email = $1", [userEmail]);
    const user = userResult.rows[0];
    if (!user) return res.status(401).json({ success: false, message: "User not found" });

    // Get datasets assigned to user
    let datasetsQuery;
    if (user.role === 'admin') {
      datasetsQuery = "SELECT dataset_id, dataset_name FROM datasets WHERE upload_status = 'cleaned'";
    } else {
      datasetsQuery = `
        SELECT d.dataset_id, d.dataset_name
        FROM datasets d
        JOIN permissions p ON d.dataset_id = p.dataset_id
        WHERE p.user_id = $1 AND d.upload_status = 'cleaned' AND p.can_view = TRUE
      `;
    }

    const datasetsResult = await pool.query(datasetsQuery, user.role === 'admin' ? [] : [user.user_id]);
    const assignedDatasets = datasetsResult.rows;

    // Check which have cleaned files
    const uploadsRoot = path.resolve(process.cwd(), "..", "uploads");
    const cleanedDir = path.join(uploadsRoot, 'cleaned');

    const availableDatasets = [];
    for (const ds of assignedDatasets) {
      const cleanedFile = path.join(cleanedDir, `cleaned_${ds.dataset_id}.csv`);
      try {
        await fs.access(cleanedFile);
        availableDatasets.push({
          dataset_id: ds.dataset_id,
          name: ds.dataset_name
        });
      } catch {
        // File doesn't exist, skip this dataset
      }
    }

    return res.json({
      success: true,
      data: availableDatasets
    });

  } catch (err) {
    console.error("[CLEANED DATASETS CONTROLLER]", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
}
