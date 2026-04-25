# DataInsights.ai Chatbot Integration Complete

## Summary

The standalone `offline chatbot` FastAPI service has been fully migrated into DataInsights.ai as an integrated chatbot engine within the ML pipeline.

## Architecture

### Before Migration
```
┌─────────────────┐
│  React UI       │
└────────┬────────┘
         │ HTTP
         ↓
┌─────────────────┐        ┌─────────────────┐
│  Node.js Backend│        │  FastAPI Service│
│  (:3001)        │   +    │  (Port 8100)    │
└─────────────────┘        └─────────────────┘
         │                         │
         ↓                         ↓
    PostgreSQL         /offline chatbot/uploads/
```

### After Migration
```
┌─────────────────┐
│  React UI       │
└────────┬────────┘
         │ HTTP
         ↓
┌─────────────────────────────────┐
│  Node.js Backend (:3001)        │
│  chatController.js              │
└────────┬────────────────────────┘
         │ spawn
         ↓
┌─────────────────────────────────┐
│ chatbot_runner.py               │
│ (ml_engine/chatbot_engine/)     │
│ ├─ RAG + Intent Classification  │
│ ├─ SQL Generation & Validation  │
│ ├─ DuckDB Execution             │
│ └─ Ollama LLM Integration       │
└────────┬────────────────────────┘
         │
    ┌────┴──────┬────────────┐
    ↓           ↓            ↓
Database   /uploads/     Ollama
         cleaned/       (local)
```

## Key Improvements

### 1. **No Duplicate Services**
- Single backend instead of 2 servers
- No separate FastAPI instance to manage
- Simpler deployment

### 2. **Integrated Permissions**
- Respects DataInsights.ai role-based access control
- Uses `dataset_permissions` table
- Admin vs. regular user enforcement

### 3. **Single Dataset Pipeline**
- Uses cleaned datasets from `/uploads/cleaned/cleaned_*.csv`
- Unified dataset finalization process
- No duplicate CSV management

### 4. **Production-Ready Integration**
- Node.js spawns Python orchestrator (same pattern as existing ML pipeline)
- JSON-based communication
- Proper error handling and logging
- Timeout protection (120 seconds per query)

### 5. **Clean Code Organization**
```
ml_engine/chatbot_engine/
├── chatbot_runner.py      ← Called by Node.js
├── config.py              ← Configuration
├── requirements.txt       ← Dependencies
├── core/                  ← Processing modules
├── models/                ← Pydantic schemas
└── prompts/               ← Prompt engineering
```

## Files Modified

### Backend (Node.js)
- `backend-node/src/controllers/chatController.js` — Now uses integrated chatbot_runner
- `backend-node/src/services/chatbotService.js` — NEW: Wraps chatbot invocation

### Frontend (React)
- ✅ **No changes** — Existing chat UI works with new backend

### New Python Module
- `ml_engine/chatbot_engine/` — Complete migrated chatbot engine
- `ml_engine/chatbot_engine/MIGRATION.md` — Detailed migration docs

## Features

### ✅ Preserved Features

- **RAG (Retrieval-Augmented Generation)** — Finds relevant columns for questions
- **Intent Classification** — Distinguishes data queries from analytical questions
- **SQL Generation** — Creates safe SELECT queries (single or multi-part plans)
- **SQL Security** — Read-only enforcement via sqlglot AST validation
- **DuckDB Execution** — Fast in-memory query processing
- **Session Management** — Chat history with TTL-based cleanup
- **Ollama Integration** — Fully offline LLM (llama3.2:3b)
- **Error Handling** — Friendly error messages and fallbacks

### ✅ New Features

- **Database-Driven Access Control** — Respects user permissions
- **Unified Dataset Pipeline** — Single source of truth for datasets
- **Admin/User Roles** — Proper role-based filtering
- **Production Logging** — Activity tracking and query logging

## Configuration

### Required Environment

**Python Dependencies:**
```bash
pip install -r ml_engine/chatbot_engine/requirements.txt
```

**Ollama Models:**
```bash
ollama pull llama3.2:3b
ollama pull nomic-embed-text
ollama serve
```

**Node.js Backend:**
```bash
npm install
npm start
```

**React Frontend:**
```bash
npm install
npm run dev
```

### Settings

**File:** `ml_engine/chatbot_engine/config.py`

```python
OLLAMA_BASE_URL = "http://localhost:11434"
CHAT_MODEL = "llama3.2:3b"
EMBED_MODEL = "nomic-embed-text"
UPLOADS_DIR = Path("../../uploads")  # Points to /uploads/cleaned/
RAG_TOP_K = 8
MAX_RESULT_ROWS = 50
HISTORY_WINDOW = 6
SESSION_TTL_MINS = 120
```

## Testing the Integration

### Manual Test

1. **Start all services:**
   ```bash
   # Terminal 1: Ollama
   ollama serve

   # Terminal 2: Node backend
   cd backend-node && npm start

   # Terminal 3: React frontend
   cd frontend-react && npm run dev
   ```

2. **Use the Chat UI:**
   - Log in to React (`http://localhost:5173`)
   - Navigate to **Employee → Chat**
   - Select a cleaned dataset
   - Ask a question: *"How many rows in this dataset?"*
   - Verify response appears

3. **Check Logs:**
   ```bash
   # Node logs should show:
   # [chatbot_runner] called with dataset_id=...
   # Response: {"success": true, "answer": "..."}
   ```

## Cleanup

Once verified, remove the old standalone chatbot:

```bash
rm -rf offline\ chatbot/
```

**⚠️ Important:** Only do this after testing confirms the integrated chatbot works correctly.

## Migration Notes

- The standalone folder `/offline chatbot/` is no longer needed
- All chatbot functionality is now in `ml_engine/chatbot_engine/`
- No breaking changes to the React UI
- Node.js backend integration is transparent to the frontend
- Session management is per-user, per-dataset (format: `user_{user_id}_{dataset_id}`)

## Troubleshooting

### "Dataset not found"
- Verify dataset was cleaned and finalized
- Check `/uploads/cleaned/cleaned_{dataset_id}.csv` exists
- Verify user has `dataset_permissions` entry

### "Ollama connection refused"
- Ensure Ollama is running: `ollama serve`
- Check it's listening on `http://localhost:11434`
- Run: `curl http://localhost:11434/api/tags`

### "Invalid SQL validation"
- The chatbot only allows SELECT queries
- Check for accidental INSERT/UPDATE/DELETE requests

### Timeout errors
- Default timeout is 120 seconds per query
- Check Ollama model isn't stuck
- Increase timeout in chatbot_runner.py if needed

## Status

- ✅ **Migration Complete** — All code moved and integrated
- ✅ **Backend Integration** — chatController.js updated
- ✅ **Configuration** — config.py set for new paths
- ⏳ **Testing** — Verify in development environment
- ⏳ **Cleanup** — Remove /offline chatbot/ after testing
- ⏳ **Documentation** — Update main README (this file)

---

**For detailed migration information, see:** [MIGRATION.md](ml_engine/chatbot_engine/MIGRATION.md)