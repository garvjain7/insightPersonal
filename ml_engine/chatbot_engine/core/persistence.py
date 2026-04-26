"""
core/persistence.py
===================
Robust artifact persistence with .lock file protection and atomic renames.
Ensures that concurrent Python processes don't corrupt or redundantly 
rebuild FAISS indexes or dataset schemas.
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("chatbot.persistence")

def get_artifact_dir(uploads_root: str, dataset_id: str) -> Path:
    """Returns the artifact directory for a dataset, creating it if needed."""
    path = Path(uploads_root) / "artifacts" / dataset_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def is_artifact_valid(artifact_path: str, source_csv_path: str) -> bool:
    """
    Checks if an artifact exists and is newer than the source CSV.
    Invalidates if the CSV has been modified since the artifact was created.
    """
    if not os.path.exists(artifact_path):
        return False
    
    try:
        csv_mtime = os.path.getmtime(source_csv_path)
        art_mtime = os.path.getmtime(artifact_path)
        return art_mtime > csv_mtime
    except Exception:
        return False


class ArtifactLock:
    """
    A simple filesystem-based lock to prevent concurrent builds.
    """
    def __init__(self, lock_path: str, timeout: int = 15):
        self.lock_path = lock_path
        self.timeout = timeout
        self.acquired = False

    def __enter__(self):
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            try:
                # O_EXCL ensures the call fails if the file already exists (atomic)
                fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                self.acquired = True
                logger.debug(f"Lock acquired: {self.lock_path}")
                return self
            except FileExistsError:
                # Wait and retry
                time.sleep(0.5)
        
        raise TimeoutError(f"Could not acquire lock on {self.lock_path} after {self.timeout}s")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            try:
                os.remove(self.lock_path)
                logger.debug(f"Lock released: {self.lock_path}")
            except Exception as e:
                logger.warning(f"Failed to remove lock file {self.lock_path}: {e}")


def atomic_save_json(path: str, data: Any):
    """Saves data to a JSON file using a temporary file and atomic rename."""
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)


def load_json(path: str) -> Optional[Any]:
    """Loads a JSON file if it exists, otherwise returns None."""
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON from {path}: {e}")
        return None
