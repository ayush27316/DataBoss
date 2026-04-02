"""
Pipeline Orchestrator
---------------------
Drives the full agent lifecycle from a single Pub/Sub event:

  1. Download file from GCS -> buffer into staging_raw -> delete GCS object
  2. Check threshold -> if not met, return early
  3. Atomically claim pending rows (prevents duplicate cycles)
  4. Run Inspector Agent (schema design + PR):
     - Writes dev_schema.sql (DDL), executes it to create DEV_ tables
     - Writes dbt staging models (pure SELECTs)
     - Writes migration_plan.sql + injection_plan.sql (SQL scripts)
     - Writes schema_summary.md
     - Opens GitHub PR with all files
  5. Run Injection Agent (data movement):
     - Executes migration_plan.sql (PROD_ → DEV_ backfill, if exists)
     - Executes injection_plan.sql (staging_raw → DEV_ tables)
     - Deletes accepted rows from staging_raw / marks rejected rows
"""

import uuid
import json
import threading
import traceback
from sqlalchemy import text

from app.config import get_settings
from app.database import Session, ensure_staging_table
from app.logging_config import get_logger
from app.services.gcs import download_object, delete_object
from app.services.pubsub import GCSNotification

settings = get_settings()
log = get_logger("orchestrator")

_pipeline_lock = threading.Lock()


def handle_gcs_event(notification: GCSNotification) -> dict:
    """
    Called by the FastAPI webhook handler for every OBJECT_FINALIZE event.
    Returns a status dict describing what action was taken.
    """
    log.info("GCS event: %s/%s", notification.bucket, notification.object_name)
    ensure_staging_table()

    raw_bytes = download_object(notification.bucket, notification.object_name)

    try:
        payload = json.loads(raw_bytes)
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {"_raw": raw_bytes.decode("utf-8", errors="replace")}

    with Session() as session:
        session.execute(text("""
            INSERT INTO staging_raw (gcs_bucket, gcs_object, raw_payload)
            VALUES (:bucket, :object, CAST(:payload AS jsonb))
        """), {
            "bucket": notification.bucket,
            "object": notification.object_name,
            "payload": json.dumps(payload),
        })
        session.commit()

    # Data is safely in staging_raw — delete the GCS object immediately
    try:
        delete_object(notification.bucket, notification.object_name)
        log.info("Deleted GCS object: %s/%s", notification.bucket, notification.object_name)
    except Exception:
        log.warning("Failed to delete GCS object %s/%s — will retry on next cycle",
                     notification.bucket, notification.object_name)

    with Session() as session:
        count = session.execute(
            text("SELECT COUNT(*) FROM staging_raw WHERE status = 'pending'")
        ).scalar()

    if count < settings.staging_threshold:
        log.info("Buffered (%d/%d)", count, settings.staging_threshold)
        return {"status": "buffered", "pending_count": count, "threshold": settings.staging_threshold}

    acquired = _pipeline_lock.acquire(blocking=False)
    if not acquired:
        log.info("Pipeline already running — skipping (pending=%d)", count)
        return {"status": "skipped_pipeline_busy", "pending_count": count}

    try:
        return _run_pipeline()
    finally:
        _pipeline_lock.release()


def _run_pipeline() -> dict:
    """Claim pending rows atomically, then run inspector + injection."""
    cycle_id = str(uuid.uuid4())[:8]

    with Session() as session:
        claimed = session.execute(text("""
            UPDATE staging_raw SET status = 'processing'
            WHERE id IN (
                SELECT id FROM staging_raw WHERE status = 'pending'
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
        """)).fetchall()
        session.commit()

    claimed_ids = [r.id for r in claimed]
    claimed_count = len(claimed_ids)
    if claimed_count == 0:
        log.info("No pending rows to claim — aborting cycle")
        return {"status": "no_rows", "cycle_id": cycle_id}

    log.info("=== Cycle %s started — claimed %d rows ===", cycle_id, claimed_count)

    from app.agents.inspector import run_inspector
    from app.agents.injection import run_injection

    pr_url = ""
    inspector_failed = False
    try:
        pr_url = run_inspector(cycle_id=cycle_id)
    except Exception:
        log.error("Inspector failed [%s]:\n%s", cycle_id, traceback.format_exc())
        inspector_failed = True

    injection_status = ""
    if inspector_failed:
        log.warning("Skipping injection — inspector failed; marking rows as rejected")
        _mark_rows_rejected(claimed_ids)
        injection_status = "skipped_inspector_failed"
    else:
        try:
            injection_status = run_injection(cycle_id=cycle_id)
        except Exception:
            log.error("Injection failed [%s]:\n%s", cycle_id, traceback.format_exc())
            injection_status = "failed"
            _mark_remaining_processing_rejected()

    summary = (pr_url or "(no PR)")[:200]
    log.info("=== Cycle %s done — PR: %s ===", cycle_id, summary)

    return {
        "status": "pipeline_complete",
        "cycle_id": cycle_id,
        "pr_url": pr_url,
        "injection_status": injection_status,
        "claimed_count": claimed_count,
    }


def _mark_rows_rejected(row_ids: list[int]) -> None:
    """Mark specific staging rows as rejected."""
    if not row_ids:
        return
    with Session() as session:
        session.execute(text(
            "UPDATE staging_raw SET status = 'rejected' WHERE id = ANY(:ids)"
        ), {"ids": row_ids})
        session.commit()
    log.info("Marked %d rows as rejected", len(row_ids))


def _mark_remaining_processing_rejected() -> None:
    """Safety net: mark any rows still stuck in 'processing' as 'rejected'."""
    with Session() as session:
        result = session.execute(text(
            "UPDATE staging_raw SET status = 'rejected' WHERE status = 'processing' RETURNING id"
        ))
        count = len(result.fetchall())
        session.commit()
    if count > 0:
        log.warning("Safety net: marked %d orphaned processing rows as rejected", count)
