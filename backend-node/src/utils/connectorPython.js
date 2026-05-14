import { spawn } from "child_process";
import path from "path";

const REPO_ROOT = path.resolve(process.cwd(), "..");

/**
 * Runs `python -m ml_engine.connectors.runner` with JSON on stdin; parses JSON stdout.
 * cwd = repo root so imports `ml_engine.connectors` resolve.
 */
export function runConnectorPython(payload) {
  return new Promise((resolve) => {
    const proc = spawn("python", ["-m", "ml_engine.connectors.runner"], {
      cwd: REPO_ROOT,
      env: { ...process.env, PYTHONUTF8: "1" },
    });
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (d) => {
      stdout += d.toString();
    });
    proc.stderr.on("data", (d) => {
      stderr += d.toString();
    });
    proc.on("close", (code) => {
      if (stderr) {
        console.error("[connectors/runner.py stderr]\n", stderr.slice(0, 4000));
      }
      try {
        const out = JSON.parse(stdout.trim());
        resolve(out);
      } catch {
        resolve({
          success: false,
          message:
            stdout.trim() ||
            stderr.trim() ||
            (code !== 0 ? `Python exited with code ${code}` : "Failed to parse connector runner output"),
        });
      }
    });
    proc.on("error", (err) => {
      resolve({ success: false, message: `Failed to start Python: ${err.message}` });
    });
    proc.stdin.write(JSON.stringify(payload));
    proc.stdin.end();
  });
}
