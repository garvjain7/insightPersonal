import { pool } from "../config/db.js";

/**
 * Inserts a datasets row after the raw file exists at uploads/raw/{finalFileName}.
 * Matches the semantics of uploadController (company fallback, not_cleaned).
 */
export async function insertRawDatasetRecord({
  userEmail,
  datasetId,
  datasetName,
  finalFileName,
  fileSize,
  uploadStatus = "not_cleaned",
}) {
  const userResult = await pool.query("SELECT user_id, company_id FROM users WHERE email = $1", [userEmail]);
  const uploadedBy = userResult.rows[0]?.user_id || null;
  let companyId = userResult.rows[0]?.company_id || null;

  if (!companyId) {
    const companyResult = await pool.query("SELECT company_id FROM companies ORDER BY created_at ASC LIMIT 1");
    companyId = companyResult.rows[0]?.company_id;
  }

  if (!companyId) {
    const err = new Error("No company in database environment");
    err.code = "NO_COMPANY";
    throw err;
  }

  await pool.query(
    `INSERT INTO datasets (dataset_id, company_id, uploaded_by, dataset_name, file_name, file_size, upload_status)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [datasetId, companyId, uploadedBy, datasetName, finalFileName, fileSize ?? null, uploadStatus]
  );

  return { uploadedBy, companyId };
}
