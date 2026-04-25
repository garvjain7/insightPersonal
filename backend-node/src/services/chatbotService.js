import { spawn } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

/**
 * Call the integrated Python chatbot engine.
 *
 * @param {Object} params
 * @param {string} params.sessionId - Unique session identifier
 * @param {string} params.datasetId - Dataset ID (maps to cleaned_*.csv)
 * @param {string} params.question - User's natural language question
 * @returns {Promise<Object>} Chatbot response JSON
 */
export async function callChatbotRunner(params) {
  return new Promise((resolve, reject) => {
    const { sessionId, datasetId, question } = params;

    // Path to the chatbot_runner.py orchestrator
    const chatbotRunnerPath = path.resolve(
      __dirname,
      "..",
      "..",
      "..",
      "ml_engine",
      "chatbot_engine",
      "chatbot_runner.py"
    );

    const pythonProcess = spawn("python", [
      chatbotRunnerPath,
      "--session_id", sessionId,
      "--dataset_id", datasetId,
      "--question", question,
    ]);

    let stdout = "";
    let stderr = "";

    pythonProcess.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    pythonProcess.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    pythonProcess.on("close", (code) => {
      if (stderr) {
        console.warn(`[chatbot_runner stderr]: ${stderr}`);
      }

      if (code !== 0) {
        console.error(`[chatbot_runner] exited with code ${code}`);
        return reject(new Error(`Chatbot runner failed with code ${code}`));
      }

      try {
        const result = JSON.parse(stdout.trim());
        resolve(result);
      } catch (parseErr) {
        console.error(`Failed to parse chatbot response: ${stdout}`);
        reject(new Error(`Invalid JSON response from chatbot: ${parseErr.message}`));
      }
    });

    pythonProcess.on("error", (err) => {
      reject(new Error(`Failed to spawn chatbot_runner: ${err.message}`));
    });

    // Timeout after 120 seconds
    setTimeout(() => {
      pythonProcess.kill();
      reject(new Error("Chatbot request timeout"));
    }, 120000);
  });
}

export default { callChatbotRunner };
