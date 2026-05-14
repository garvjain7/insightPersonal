"""Match backend-node/src/utils/fileUtils.js sanitizeFilename for raw CSV basenames."""

import re
from pathlib import Path


def sanitize_filename(filename: str) -> str:
    if not filename:
        return "unnamed_dataset"

    p = Path(filename)
    ext = p.suffix.lower()
    name = p.stem if ext else filename

    name = re.sub(r"[^a-zA-Z0-9._-]", "_", name)
    name = re.sub(r"\.+", ".", name)
    name = re.sub(r"_+", "_", name)

    if len(name) > 30:
        name = name[:30]

    name = re.sub(r"[._]+$", "", name)

    if not ext:
        ext = ".csv"

    if not name:
        name = "dataset"

    return f"{name}{ext}"
