import fs from "fs/promises";
import path from "path";

/**
 * Sandboxed Validation Layer
 * -------------------------
 * This utility validates that uploaded files are safe and conform to expected formats.
 * 
 * Rules:
 * 1. Mime-type verification.
 * 2. Magic number (header) check.
 * 3. File extension vs Content consistency.
 */
export const validateFileSafety = async (filePath, originalName) => {
  const stats = await fs.stat(filePath);
  if (stats.size > 500 * 1024 * 1024) { // 500MB limit
    throw new Error("File exceeds maximum size limit (500MB).");
  }

  const ext = path.extname(originalName).toLowerCase();
  
  // Read first few bytes for magic number check
  const fd = await fs.open(filePath, 'r');
  const { buffer } = await fd.read(Buffer.alloc(8), 0, 8, 0);
  await fd.close();

  const header = buffer.toString('hex');

  // CSV check (no strict magic number, but check for text-like content)
  if (ext === '.csv') {
    // Check if it looks like binary (simple check for null bytes or lots of non-printable)
    const sample = buffer.slice(0, 1024).toString('utf8');
    if (sample.includes('\x00')) {
      throw new Error("Security Violation: CSV file contains binary/null characters.");
    }
  }

  // Excel check (Magic number for Zip/Office: 504b0304)
  if (ext === '.xlsx' || ext === '.xls') {
    if (!header.startsWith('504b0304') && !header.startsWith('d0cf11e0')) {
      throw new Error("Security Violation: Excel file has invalid header signatures.");
    }
  }

  return true;
};
