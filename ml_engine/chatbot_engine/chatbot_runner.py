#!/usr/bin/env python3
"""
chatbot_runner.py
=================
Main orchestrator for the integrated chatbot engine.
Called by Node.js backend via spawn() with command-line arguments.

Responsibilities:
  1. Load dataset from /uploads/cleaned/
  2. Classify intent
  3. Retrieve relevant columns via RAG
  4. Generate SQL (if needed)
  5. Validate and execute SQL
  6. Format final response
  7. Return JSON to Node.js

Never call multiple Python files from Node — call only this orchestrator.
This keeps the Node↔Python boundary clean and testable.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import config
from core import dataset as dataset_module
from core import session as session_store
from core.intent import classify
from core.llm import health as llm_health, stream_chat
from core.rag import ColumnIndex, retrieve
from core.sql_executor import QueryResult, execute, results_to_context
from core.sql_generator import generate as generate_sql
from core.sql_validator import ValidationError, validate
from models.schemas import IntentType, MessageRole
from prompts.builder import build as build_prompt

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("chatbot.runner")


# ── Main entry point ─────────────────────────────────────────────────────────

async def main():
    """
    Parse CLI args, execute the chatbot pipeline, return JSON.
    """
    try:
        # Parse command-line arguments
        args = _parse_args()
        
        # Get or create session
        sess = session_store.get_or_create(args["session_id"])

        # Load dataset
        try:
            if sess.dataset is None or sess.dataset_id != args["dataset_id"]:
                loaded = dataset_module.load(args["dataset_id"])
                sess.dataset = loaded
                sess.dataset_id = args["dataset_id"]
                sess.column_index = None  # Force rebuild
        except FileNotFoundError as exc:
            return _json_response(
                success=False,
                answer=f"Dataset not found: {exc}",
                intent=None,
                error=str(exc),
            )

        loaded = sess.dataset
        
        # Build or reuse RAG column index
        if sess.column_index is None:
            # We need the physical path to validate cache
            cleaned_dir = config.UPLOADS_DIR / "cleaned"
            csv_path = cleaned_dir / f"cleaned_{args['dataset_id']}.csv"
            
            sess.column_index = await ColumnIndex.get_or_build(
                args["dataset_id"], 
                loaded.schema, 
                str(config.UPLOADS_DIR),
                str(csv_path)
            )

        # Classify intent
        intent = await classify(args["question"])
        logger.info(f"Intent: {intent.value} | Q: {args['question'][:80]!r}")

        # Retrieve relevant columns
        relevant_cols = await retrieve(sess.column_index, args["question"])
        logger.info(f"RAG retrieved {len(relevant_cols)} columns")

        # Generate SQL if needed
        query_results: list[QueryResult] = []
        sql_executed: list[str] = []

        if intent in (IntentType.data_query, IntentType.analytical):
            sql_plan = await generate_sql(args["question"], relevant_cols, intent)

            for item in sql_plan:
                raw_sql = item["sql"]
                sub_question = item["sub_question"]

                # Validate SQL
                try:
                    safe_sql = validate(raw_sql)
                except ValidationError as exc:
                    logger.warning(f"SQL validation failed: {exc}")
                    return _json_response(
                        success=False,
                        answer=f"Generated query was unsafe: {exc}",
                        intent=intent.value,
                        error=str(exc),
                    )

                # Execute SQL
                result = execute(loaded.conn, safe_sql, sub_question)
                query_results.append(result)
                sql_executed.append(safe_sql)

                if result.error:
                    logger.warning(f"Query error: {result.error}")

        # Build final prompt
        data_context = results_to_context(query_results) if query_results else None
        messages = build_prompt(
            question=args["question"],
            dataset_id=args["dataset_id"],
            total_rows=len(loaded.df),
            total_cols=len(loaded.schema),
            relevant_cols=relevant_cols,
            history=sess.get_history_window(),
            data_context=data_context,
            intent=intent,
        )

        # Stream response from Ollama
        full_answer = ""
        try:
            async for token in stream_chat(messages):
                full_answer += token
        except Exception as exc:
            logger.error(f"Stream error: {exc}")
            return _json_response(
                success=False,
                answer=f"Failed to generate response: {exc}",
                intent=intent.value,
                error=str(exc),
            )

        # Store in session history
        sess.add_message(MessageRole.user, args["question"])
        sess.add_message(MessageRole.assistant, full_answer)

        # Return success
        return _json_response(
            success=True,
            answer=full_answer,
            intent=intent.value,
            suggested_questions=_generate_suggestions(args["question"], full_answer),
            confidence=0.85,  # Placeholder
        )

    except Exception as exc:
        logger.exception(f"Unexpected error: {exc}")
        return _json_response(
            success=False,
            answer=f"An unexpected error occurred: {exc}",
            intent=None,
            error=str(exc),
        )


def _parse_args() -> dict:
    """
    Parse command-line arguments.
    Expected format:
      python chatbot_runner.py
        --session_id <id>
        --dataset_id <id>
        --question <text>
    """
    args = {"session_id": None, "dataset_id": None, "question": None}
    
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith("--"):
            key = sys.argv[i][2:]
            if i + 1 < len(sys.argv):
                args[key] = sys.argv[i + 1]
                i += 2
            else:
                i += 1
        else:
            i += 1

    # Validate required args
    for key in ["session_id", "dataset_id", "question"]:
        if not args.get(key):
            raise ValueError(f"Missing required argument: --{key}")

    return args


def _json_response(
    success: bool,
    answer: str,
    intent: str | None = None,
    confidence: float = 0.0,
    suggested_questions: list[str] | None = None,
    error: str | None = None,
) -> str:
    """
    Format the response as JSON that Node.js expects.
    """
    return json.dumps(
        {
            "success": success,
            "answer": answer,
            "intent": intent,
            "confidence": confidence,
            "suggested_questions": suggested_questions or [],
            "error": error,
        }
    )


def _generate_suggestions(question: str, answer: str) -> list[str]:
    """
    Generate simple follow-up suggestions based on the question.
    (Could be enhanced with LLM-based generation.)
    """
    # Placeholder: return empty for now
    # In production, this could call the LLM with a tiny prompt
    return []


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        print(result)
        sys.exit(0)
    except Exception as exc:
        logger.exception(f"Fatal error: {exc}")
        print(_json_response(
            success=False,
            answer=f"Fatal error: {exc}",
            intent=None,
            error=str(exc),
        ))
        sys.exit(1)