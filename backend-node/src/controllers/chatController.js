import path from "path";
import { spawn } from "child_process";
import { pool } from "../config/db.js";
import { validateDatasetAccess, getDatasetPaths } from "../utils/accessUtils.js";
import fs from "fs/promises";
import { logActivity, logChatActivity, logPermissionActivity } from "./activityController.js";
import { callChatbotRunner } from "../services/chatbotService.js";


export const askQuestion = async (req, res) => {
  const { message, question, datasetId } = req.body;
  const queryText = (message || question || "").trim();
  const userEmail = req.user?.email;

  if (!queryText || !datasetId) {
    return res.status(400).json({ success: false, message: "Message and datasetId are required." });
  }

  try {
    const userRes = await pool.query("SELECT user_id, role, company_id FROM users WHERE email = $1", [userEmail]);
    const user = userRes.rows[0];
    if (!user) return res.status(401).json({ success: false, message: "User not found" });

    // 1. Permission Check
    // Get granular permissions from database
    let granularPerms = { can_view: false, can_edit: false, can_query: false };
    
    if (user.role === 'admin') {
      granularPerms = { can_view: true, can_edit: true, can_query: true };
    } else {
      const permRes = await pool.query(
        "SELECT can_view, can_edit, can_query FROM permissions WHERE user_id = $1 AND dataset_id = $2",
        [user.user_id, datasetId]
      );
      if (permRes.rows.length > 0) {
        granularPerms = permRes.rows[0];
      }
    }

    if (!granularPerms.can_view && !granularPerms.can_query) {
      return res.status(403).json({ success: false, message: "Forbidden: You do not have access to this dataset" });
    }

    const dsRes = await pool.query("SELECT file_name FROM datasets WHERE dataset_id = $1", [datasetId]);
    if (!dsRes.rows[0]) return res.status(404).json({ success: false, message: "Dataset not found" });

    const { file_name } = dsRes.rows[0];
    const paths = getDatasetPaths(datasetId, file_name);

    // 2. Resolve the best available CSV file (cleaned > working > raw)
    let csvFilePath = null;
    for (const candidate of [paths.cleaned, paths.working, paths.raw]) {
      try { await fs.access(candidate); csvFilePath = candidate; break; } catch {}
    }

    if (!csvFilePath) {
      return res.json({
        success: true,
        source: "fallback",
        answer: "⚠️ This dataset has not been finalized yet. Please complete the cleaning process first."
      });
    }

    // 3. Call integrated chatbot engine
    // The chatbot_runner.py handles: dataset loading, RAG, intent, SQL generation, execution, and response
    const startTimeMs = Date.now();

    try {
      const result = await callChatbotRunner({
        sessionId: `user_${user.user_id}_${datasetId}`,  // Unique session per user+dataset
        datasetId: datasetId,
        question: queryText,
      });

      const duration = Date.now() - startTimeMs;

      // Log query to database
      try {
        await pool.query(
          "INSERT INTO query_logs (company_id, user_id, dataset_id, query_text, query_type, execution_time_ms, status, generated_code, error_msg) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
          [
            user.company_id || null,
            user.user_id,
            datasetId,
            queryText,
            result.intent || 'unknown',
            duration,
            result.success === false ? "failed" : "success",
            null,  // SQL code not returned by chatbot_runner
            result.error || null
          ]
        );
      } catch (dbErr) {
        console.error("Could not log query to query_logs:", dbErr);
      }

      // Return response
      return res.json({
        success: result.success !== false,
        source: "ml-engine-chatbot",
        answer: result.answer || "I couldn't find an answer.",
        intent: result.intent,
        confidence: result.confidence || 0,
        suggested_questions: result.suggested_questions || []
      });

    } catch (err) {
      console.error("Chatbot runner error:", err);
      return res.json({
        success: false,
        source: "error",
        answer: "The chatbot encountered an error. Please try rephrasing your question.",
        error: err.message
      });
    }

  } catch (err) {
    console.error("askQuestion error:", err);
    return res.status(500).json({ success: false, message: "Internal server error" });
  }
};

export const startChatSession = async (req, res) => {
  try {
    const { datasetId } = req.body;
    const userId = req.user.user_id;
    const email = req.user.email;
    
    const dsRes = await pool.query("SELECT dataset_name FROM datasets WHERE dataset_id = $1", [datasetId]);
    const dsName = dsRes.rows[0]?.dataset_name || "Unknown Dataset";
    
    await logChatActivity(userId, null, email, datasetId, dsName, "CHAT_START");
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
};

export const endChatSession = async (req, res) => {
  try {
    const { datasetId, reason } = req.body; // reason: 'closed', 'cleared'
    const userId = req.user.user_id;
    const email = req.user.email;
    
    const dsRes = await pool.query("SELECT dataset_name FROM datasets WHERE dataset_id = $1", [datasetId]);
    const dsName = dsRes.rows[0]?.dataset_name || "Unknown Dataset";
    
    await logChatActivity(userId, null, email, datasetId, dsName, "CHAT_END", `Session ended: ${reason || 'User finished'}`);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false });
  }
};
