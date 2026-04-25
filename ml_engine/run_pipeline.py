"""
run_pipeline.py — ML Pipeline Manager
=======================================
Called by Node.js AFTER the user has clicked "Finalize Dataset" in the cleaning wizard.

This script reads the finalized cleaned CSV from:
    /uploads/cleaned/cleaned_{dataset_id}.csv

If that file does not yet exist, the pipeline exits with an error — the user must
complete the cleaning wizard and click Finalize first.

Pipeline stages (unchanged from previous version):
    1.  Validator         — validate + register dataset metadata
    2.  Schema Manager    — detect schema, types, date/target columns
    3.  Feature Engineer  — derived features (no scaler for tree models)
    4.  Trainer           — RF (lightweight) or RF+XGBoost (full mode)
    5.  Forecaster        — only if datetime + target column present
    6.  BI Engine         — KPIs, aggregations
    7.  Metric Engine     — metric definitions
    8.  Insight Engine    — auto insights
    9.  Dashboard         — dashboard config generation
    10. Artifact contract — verify all expected files were generated

Pipeline mode is chosen by row count:
    < 5 000  rows → bi_only      (skip ML training)
    < 100 000 rows → lightweight  (RF only)
    ≥ 100 000 rows → full         (RF + XGBoost)
"""

import argparse
import json
import logging
import os
import re
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("system_logger")


# ── PII masking (for logs) ────────────────────────────────────────────────────

def _mask_pii(text: str) -> str:
    if not text or not isinstance(text, str):
        return text
    text = re.sub(r"([\w\.-]{2})[\w\.-]+@([\w\.-]+)", r"\1***@\2", text)
    return text


# ── Pipeline stage imports ────────────────────────────────────────────────────
# Imported lazily so that import errors surface with a clear message.

def _import_stages():
    try:
        from pipeline.validator import validate_dataset
        from pipeline.schema_manager import process_schema
        from pipeline.feature_engineer import engineer_features
        from pipeline.trainer import train_evaluate_models
        from pipeline.forecaster import generate_forecast
        from pipeline.bi_engine import run_bi_engine
        from pipeline.metric_engine import generate_metric_definitions
        from pipeline.insight_engine import generate_insights
        from pipeline.dashboard import generate_dashboard_config
    except ImportError as exc:
        logger.error(f"Failed to import pipeline stage: {exc}")
        raise

    return (
        validate_dataset, process_schema, engineer_features,
        train_evaluate_models, generate_forecast, run_bi_engine,
        generate_metric_definitions, generate_insights, generate_dashboard_config,
    )


# ── Expected artifact contract ────────────────────────────────────────────────

EXPECTED_ARTIFACTS = [
    "dataset_metadata.json",
    "schema.json",
    "profile_report.json",
    "metrics.json",
    "feature_importance.json",
    "kpi_summary.json",
    "forecast.json",
    "dashboard_config.json",
    "metrics_definition.json",
    "insights.json",
    "model_metrics.json",
]

# ── Size thresholds ───────────────────────────────────────────────────────────

SMALL_THRESHOLD = 5_000
MEDIUM_THRESHOLD = 100_000


# ── Helpers ───────────────────────────────────────────────────────────────────

def _timed_stage(name: str, fn, *args, **kwargs):
    """Run fn(*args, **kwargs), emit timing logs, return (result, elapsed_s)."""
    print(f"[STAGE-START] {name}", flush=True)
    t0 = time.time()
    result = fn(*args, **kwargs)
    elapsed = time.time() - t0
    elapsed_ms = int(elapsed * 1000)
    logger.info(f"[STAGE-END] {name} duration={elapsed_ms}ms")
    print(f"[STAGE-END] {name} duration={elapsed_ms}ms", flush=True)
    return result, elapsed


def _write_skipped_forecast(dataset_dir: str, reason: str) -> None:
    import json
    from filelock import FileLock

    forecast_path = os.path.join(dataset_dir, "forecast.json")
    with FileLock(forecast_path + ".lock"):
        with open(forecast_path, "w") as fh:
            json.dump({"status": "skipped", "reason": reason}, fh, indent=4)


# ── Guard: cleaned file must exist ────────────────────────────────────────────

def _resolve_cleaned_path(uploads_root: str, dataset_id: str) -> str | None:
    """
    Return the path to the finalized cleaned CSV, or None if it doesn't exist.
    """
    cleaned_path = os.path.join(uploads_root, "cleaned", f"cleaned_{dataset_id}.csv")
    return cleaned_path if os.path.isfile(cleaned_path) else None


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(
    dataset_id: str,
    uploads_root: str,
    user_id: str = "default_user",
    dataset_dir_override: str | None = None,
) -> dict:
    """
    Run the full ML pipeline for a finalized dataset.

    Parameters
    ----------
    dataset_id        : ID of the dataset (used to locate cleaned CSV)
    uploads_root      : Absolute path to the uploads/ root directory
    user_id           : Identifier of the user/org (for logging)
    dataset_dir_override : If provided, use this directory for ML artifacts instead
                           of the default <uploads_root>/artifacts/<dataset_id>/
    """
    overall_start = time.time()
    logger.info(f"[PIPELINE-START] dataset_id={dataset_id}")
    print(f"\n[PIPELINE-START] dataset_id={dataset_id}", flush=True)

    result_payload = {
        "status": "failed",
        "dataset_id": dataset_id,
        "error": "",
        "data_quality_score": None,
        "artifacts_generated": [],
        "pipeline_mode": None,
        "execution_time": None,
    }

    # ── Guard: require finalized cleaned file ─────────────────────────────────
    cleaned_path = _resolve_cleaned_path(uploads_root, dataset_id)
    if cleaned_path is None:
        msg = (
            f"Cleaned dataset not found at uploads/cleaned/cleaned_{dataset_id}.csv. "
            "The dataset must be fully cleaned and finalized before running the ML pipeline."
        )
        logger.error(f"[{dataset_id}] {msg}")
        result_payload["error"] = msg
        return result_payload

    logger.info(f"[{dataset_id}] Using cleaned file: {cleaned_path}")

    # ── Resolve artifact directory ────────────────────────────────────────────
    if dataset_dir_override:
        dataset_dir = dataset_dir_override
    else:
        dataset_dir = os.path.join(uploads_root, "artifacts", str(dataset_id))

    os.makedirs(dataset_dir, exist_ok=True)

    training_time = 0.0
    forecast_time = 0.0
    artifact_status = "success"

    try:
        (
            validate_dataset, process_schema, engineer_features,
            train_evaluate_models, generate_forecast, run_bi_engine,
            generate_metric_definitions, generate_insights, generate_dashboard_config,
        ) = _import_stages()

        # ── 1. Validation & Setup ─────────────────────────────────────────────
        # Pass the already-cleaned file as the entry point.
        # The validator registers it and sets up dataset_dir artifacts.
        val_res, _ = _timed_stage(
            "validator",
            validate_dataset,
            cleaned_path,
            base_dir=os.path.dirname(os.path.abspath(__file__)),
            user_id=user_id,
            dataset_id=dataset_id,
            dataset_dir=dataset_dir,          # tell validator where to write artifacts
        )

        if val_res.get("status") == "error":
            result_payload["error"] = val_res.get("message", "Validator returned an error")
            return result_payload

        row_count = int(val_res.get("rows", 0))

        # ── Determine pipeline mode ───────────────────────────────────────────
        if row_count < SMALL_THRESHOLD:
            pipeline_mode = "bi_only"
        elif row_count < MEDIUM_THRESHOLD:
            pipeline_mode = "lightweight"
        else:
            pipeline_mode = "full"

        result_payload["pipeline_mode"] = pipeline_mode
        logger.info(f"[{dataset_id}] {row_count} rows → mode: {pipeline_mode}")

        # ── 2. Schema & Profiling ─────────────────────────────────────────────
        schema, _ = _timed_stage("schema_manager", process_schema, cleaned_path, dataset_dir)
        if not schema:
            raise RuntimeError("Schema detection failed")
        result_payload["artifacts_generated"].extend(["schema.json", "profile_report.json"])

        # ── 3. Feature Engineering ────────────────────────────────────────────
        # Tree-based models (RF, XGBoost) do not need scaling.
        feat_res, _ = _timed_stage(
            "feature_engineer",
            engineer_features,
            dataset_dir,
            use_scaler=False,
        )
        if not feat_res:
            raise RuntimeError("Feature engineering failed")

        # ── 4. Training (mode-aware) ──────────────────────────────────────────
        if pipeline_mode != "bi_only":
            train_res, training_time = _timed_stage(
                "trainer",
                train_evaluate_models,
                dataset_dir,
                pipeline_mode=pipeline_mode,
            )
            if train_res:
                result_payload["artifacts_generated"].extend(
                    ["feature_importance.json", "model_metrics.json"]
                )
        else:
            logger.info(f"[{dataset_id}] Training skipped (bi_only mode)")
            # Write empty placeholders so artifact contract is satisfied
            for artifact in ["feature_importance.json", "model_metrics.json"]:
                artifact_path = os.path.join(dataset_dir, artifact)
                if not os.path.exists(artifact_path):
                    with open(artifact_path, "w") as fh:
                        json.dump({"status": "skipped", "reason": "bi_only_mode"}, fh)
            result_payload["artifacts_generated"].extend(
                ["feature_importance.json", "model_metrics.json"]
            )

        # ── 5. Forecasting ────────────────────────────────────────────────────
        has_datetime = bool(schema.get("date_column"))
        has_target = bool(schema.get("sales_column") or schema.get("profit_column"))

        if has_datetime and has_target:
            fc_res, forecast_time = _timed_stage(
                "forecaster", generate_forecast, dataset_dir
            )
        else:
            reason = "missing_datetime_or_target"
            logger.info(f"[{dataset_id}] Forecasting skipped — {reason}")
            _write_skipped_forecast(dataset_dir, reason)
            fc_res = {"status": "skipped"}

        result_payload["artifacts_generated"].append("forecast.json")

        # ── 6. BI Engine ──────────────────────────────────────────────────────
        bi_res, _ = _timed_stage("bi_engine", run_bi_engine, dataset_dir)
        if bi_res:
            result_payload["artifacts_generated"].append("kpi_summary.json")

        # ── 7. Metric Engine ──────────────────────────────────────────────────
        metric_res, _ = _timed_stage(
            "metric_engine", generate_metric_definitions, dataset_dir
        )
        if metric_res:
            result_payload["artifacts_generated"].extend(
                ["metrics.json", "metrics_definition.json"]
            )

        # ── 8. Insight Engine ─────────────────────────────────────────────────
        insight_res, _ = _timed_stage("insight_engine", generate_insights, dataset_dir)
        if insight_res:
            result_payload["artifacts_generated"].append("insights.json")

        # ── 9. Dashboard ──────────────────────────────────────────────────────
        dash_res, _ = _timed_stage("dashboard", generate_dashboard_config, dataset_dir)
        if dash_res:
            result_payload["artifacts_generated"].append("dashboard_config.json")

        # ── 10. Artifact contract verification ────────────────────────────────
        missing_artifacts = [
            a for a in EXPECTED_ARTIFACTS
            if not os.path.exists(os.path.join(dataset_dir, a))
        ]
        if missing_artifacts:
            artifact_status = "partial_failure"
            result_payload["status"] = "failed"
            result_payload["error"] = (
                f"artifact_generation_failed: missing {', '.join(missing_artifacts)}"
            )
            logger.error(f"[{dataset_id}] Missing artifacts: {', '.join(missing_artifacts)}")
        else:
            result_payload["status"] = "completed"

    except Exception as exc:
        logger.exception(f"[{dataset_id}] Pipeline failed: {exc}")
        result_payload["error"] = f"Pipeline execution failed: {str(exc)}"
        artifact_status = "failure"

    total_time = time.time() - overall_start
    result_payload["execution_time"] = round(total_time, 3)

    logger.info(
        f"PIPELINE_RUN | User: {_mask_pii(user_id)} | Dataset: {dataset_id} | "
        f"Mode: {result_payload.get('pipeline_mode')} | TotalTime: {total_time:.2f}s | "
        f"TrainTime: {training_time:.2f}s | ForecastTime: {forecast_time:.2f}s | "
        f"Rows: {row_count if 'row_count' in dir() else '?'} | "  # noqa
        f"ArtifactStatus: {artifact_status}"
    )
    print(f"[PIPELINE-END] total_duration={int(total_time)}s\n", flush=True)

    return result_payload


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the DataInsights.ai ML Pipeline")
    parser.add_argument(
        "--dataset_id",
        type=str,
        required=True,
        help="Dataset ID (used to locate cleaned_{id}.csv)",
    )
    parser.add_argument(
        "--uploads_root",
        type=str,
        required=True,
        help="Absolute path to the uploads/ root directory",
    )
    parser.add_argument(
        "--user_id",
        type=str,
        default="default_user",
        help="User / organisation identifier (for logging)",
    )
    parser.add_argument(
        "--dataset_dir",
        type=str,
        default=None,
        help="Override directory for ML artifact output",
    )
    args = parser.parse_args()

    res = run_pipeline(
        dataset_id=args.dataset_id,
        uploads_root=args.uploads_root,
        user_id=args.user_id,
        dataset_dir_override=args.dataset_dir,
    )
    print(json.dumps(res, indent=4))
    sys.exit(0 if res.get("status") == "completed" else 1)