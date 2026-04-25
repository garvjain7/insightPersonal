# Chatbot Engine Migration Complete

## Overview

The standalone `/offline chatbot` FastAPI service has been fully migrated and integrated into the DataInsights.ai ecosystem as `ml_engine/chatbot_engine/`.

## Key Changes

### Architecture

**Before:** Separate FastAPI service
- Runs independently on port 8100
- Duplicates dataset management logic
- Separate session store
- No integration with DataInsights.ai permissions/access control

**After:** Integrated into Node.js backend
```
React Frontend
  ↓ (HTTP)
Node.js Backend (/chat, /query endpoints)
  ↓ (spawn)
Python chatbot_runner.py orchestrator
  ├─→ core/ (RAG, SQL generation, validation, execution)
  ├─→ models/ (Pydantic schemas)
  ├─→ prompts/ (LLM prompt building)
  └─→ config/ (Ollama, paths, limits)
```

### Files & Structure

**New Location:** `ml_engine/chatbot_engine/`

```
chatbot_engine/
├── chatbot_runner.py          ← Main orchestrator (called by Node.js)
├── config.py                  ← Configuration (Ollama URL, timeouts, RAG settings)
├── requirements.txt           ← Python dependencies
├── core/
│   ├── dataset.py            ← Load cleaned CSV, build schema
│   ├── rag.py                ← RAG (FAISS-style, with keyword fallback)
│   ├── intent.py             ← Intent classification (rule + LLM)
│   ├── sql_generator.py      ← Convert question → SQL (single or multi-query plan)
│   ├── sql_validator.py      ← Enforce read-only, check for unsafe operations
│   ├── sql_executor.py       ← Execute SQL, format results
│   ├── llm.py                ← Ollama HTTP wrapper
│   └── session.py            ← Session management (history, TTL)
├── models/
│   └── schemas.py            ← Pydantic models
├── prompts/
│   └── builder.py            ← Assemble final prompt for LLM
└── __init__.py
```

### Dataset Integration

**Before:** Looked in `/offline chatbot/uploads/`

**After:** Uses DataInsights.ai pipeline
- Cleans datasets: `/uploads/cleaned/cleaned_{dataset_id}.csv`
- Database-driven access control via `dataset_permissions` table
- Respects user roles (admin vs. regular)

### Node.js Backend Changes

**File:** `backend-node/src/controllers/chatController.js`

**Change:**
```javascript
// OLD: Spawned cognitive_engine.py
const pythonScript = path.resolve(process.cwd(), "..", "ml_engine", "pipeline", "cognitive_engine.py");
pyProcess = spawn("python", [pythonScript, ...args]);

// NEW: Calls integrated orchestrator
import { callChatbotRunner } from "../services/chatbotService.js";
const result = await callChatbotRunner({
  sessionId, datasetId, question
});
```

**New File:** `backend-node/src/services/chatbotService.js`
- Wraps `chatbot_runner.py` invocation
- Handles JSON parsing, error handling, timeouts

### React Frontend

**No changes needed** — already uses existing `/chat` and `/query` endpoints.

The EmployeeChatPage continues to work as-is:
- Calls `askQuery()` from `api.js`
- Backend now routes to integrated chatbot_runner
- Response format is compatible

### Key Features Preserved

✅ **RAG (Retrieval-Augmented Generation)**
- Semantic (embedding-based) column retrieval
- Keyword fallback if Ollama embedding model unavailable
- Respects RAG_TOP_K and RAG_MIN_SCORE config

✅ **Intent Classification**
- Rule-based (regex patterns) for speed
- LLM fallback for ambiguous cases
- Supports: data_query, analytical, general

✅ **SQL Generation**
- Single query for data_query intent
- Multi-query plan (up to 4 sub-questions) for analytical intent
- LLM-powered with DuckDB syntax

✅ **SQL Security**
- sqlglot-based AST validation
- Read-only enforcement (SELECT only)
- Blocks INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, etc.
- No system table access (information_schema, pg_catalog, etc.)

✅ **Ollama Integration**
- Fully offline, no cloud APIs
- Model: `llama3.2:3b` (chat) + `nomic-embed-text` (embeddings)
- Streaming responses when available

✅ **DuckDB Execution**
- In-memory SQL execution
- Result pagination (MAX_RESULT_ROWS = 50)
- Friendly error messages

✅ **Session Management**
- Per-user, per-dataset sessions
- Chat history with rolling window (HISTORY_WINDOW = 6 pairs)
- Auto-purge expired sessions (TTL = 120 minutes)

### What Was Removed

❌ FastAPI server in `/offline chatbot/`
- No longer needed; all functionality integrated

❌ Duplicate dataset management
- Now uses DataInsights.ai pipeline

❌ Separate uploads directory
- Uses shared `/uploads/cleaned/` with access control

❌ HTML/CSS/JS frontend in `/offline chatbot/frontend/`
- React frontend (`frontend-react`) is the canonical UI

## Configuration

**File:** `ml_engine/chatbot_engine/config.py`

```python
# Ollama
OLLAMA_BASE_URL = "http://localhost:11434"
CHAT_MODEL = "llama3.2:3b"
EMBED_MODEL = "nomic-embed-text"

# Paths
UPLOADS_DIR = Path("../../uploads")  # Relative to chatbot_engine/

# RAG
RAG_TOP_K = 8
RAG_MIN_SCORE = 0.25
SAMPLE_VALUES_N = 6

# SQL
MAX_RESULT_ROWS = 50
QUERY_TIMEOUT = 30

# Sessions
HISTORY_WINDOW = 6
SESSION_TTL_MINS = 120
```

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r ml_engine/chatbot_engine/requirements.txt
   ```

2. **Ensure Ollama is running:**
   ```bash
   ollama pull llama3.2:3b
   ollama pull nomic-embed-text
   ollama serve  # Listen on http://localhost:11434
   ```

3. **Node.js backend is unchanged:**
   ```bash
   cd backend-node
   npm install
   npm start
   ```

4. **React frontend is unchanged:**
   ```bash
   cd frontend-react
   npm install
   npm run dev
   ```

## How It Works (End-to-End)

1. **User asks a question** in React Chat UI
   ```
   "What are the top 5 products by sales?"
   ```

2. **Frontend calls** `POST /chat` or `POST /query`
   ```javascript
   askQuery(datasetId, "What are the top 5 products by sales?")
   ```

3. **Node.js backend** (chatController.js)
   - Validates user access (role + permissions)
   - Resolves dataset to cleaned CSV file
   - Calls `chatbotService.callChatbotRunner()`

4. **chatbot_runner.py** executes
   - Loads dataset (with in-memory cache)
   - Classifies intent → `data_query`
   - RAG retrieves relevant columns → `[product, sales, revenue, ...]`
   - SQL generator creates: `SELECT product, SUM(sales) as total FROM dataset GROUP BY product ORDER BY total DESC LIMIT 5`
   - SQL validator checks for safety → ✓ Valid
   - DuckDB executes → Returns 5 rows
   - LLM reasons over results + builds response → "The top 5 products..."
   - Stores in session history

5. **Response sent to frontend**
   ```json
   {
     "success": true,
     "answer": "The top 5 products by sales are...",
     "intent": "data_query",
     "confidence": 0.85,
     "suggested_questions": [...]
   }
   ```

6. **React displays** the answer and suggestions

## Cleanup

The original `/offline chatbot/` folder is no longer needed and can be removed once migration is verified:

```bash
rm -rf offline\ chatbot/
```

**Note:** Do NOT remove it until testing confirms everything works.

## Testing

### Quick Test

1. Start all services (Ollama, Node backend, React frontend)
2. Log in to React
3. Go to **Employee → Chat**
4. Select a cleaned dataset
5. Ask: *"How many rows are in this dataset?"*
6. Verify response

### Verify Integration

Check that chatbot is using new orchestrator:
```bash
# Node logs should show:
# [chatbot_runner stderr]: (no errors)
# [chatbot_runner] response: {"success": true, ...}

# NOT the old:
# [query_engine stderr]: ...
# [cognitive_engine] ...
```

### Verify Permissions

- Admin user can chat with all cleaned datasets
- Regular user can chat only with assigned datasets (via `dataset_permissions` table)

## Migration Summary

| Aspect | Before | After |
|--------|--------|-------|
| **Server** | FastAPI (separate) | Integrated in Node.js |
| **Language** | Python FastAPI + HTML | Python orchestrator + React |
| **Dataset Path** | `/offline chatbot/uploads/` | `/uploads/cleaned/` |
| **Access Control** | None | Database-driven + role-based |
| **Session Store** | In-memory (separate) | In-memory (per session_id) |
| **Deployment** | 2 separate services | 1 unified backend |
| **Maintenance** | Duplicate logic | Single source of truth |

## Next Steps

1. ✅ Migration complete
2. ⏳ Test end-to-end in development
3. ⏳ Remove `/offline chatbot/` folder
4. ⏳ Update documentation
5. ⏳ Deploy to production