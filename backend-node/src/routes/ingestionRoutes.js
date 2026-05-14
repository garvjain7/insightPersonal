import express from "express";
import multer from "multer";
import { protect } from "../middleware/protect.js";
import { getCatalog, validate, importData } from "../controllers/ingestionController.js";

const router = express.Router();

// Memory storage as we pipe the data to Python via base64 for unified handling
const storage = multer.memoryStorage();
const upload = multer({ storage });

const multiUpload = upload.fields([
  { name: "dataset", maxCount: 1 },         // Legacy key / direct file
  { name: "file", maxCount: 1 },            // Connector key
  { name: "service_account_file", maxCount: 1 }
]);

/**
 * Unified Ingestion Endpoints
 */

// GET /api/ingestion/catalog -> List all connectors with schemas
router.get("/catalog", protect, getCatalog);

// POST /api/ingestion/validate -> Test connection / check files
router.post("/validate", protect, multiUpload, validate);

// POST /api/ingestion/import -> Finalize import and register dataset
router.post("/import", protect, multiUpload, importData);

export default router;
