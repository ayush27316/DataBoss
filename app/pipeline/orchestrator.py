"""
Pipeline Orchestrator
---------------------
Drives the full agent lifecycle from a single Pub/Sub event:

  1. Download file from GCS -> buffer into staging_raw
  2. Check threshold -> if not met, return early
  3. Atomically claim pending rows (prevents duplicate cycles)
  4. Run Inspector Agent (schema analysis -> DEV_ tables via dbt -> PR)
  5. Run Injection Agent (push accepted staging data into DEV_ tables)
"""

import uuid
import json
import threading
import traceback
from sqlalchemy import text

from app.config import get_settings
from app.database import Session, ensure_staging_table
from app.logging_config import get_logger
from app.services.gcs import download_object
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

    claimed_count = len(claimed)
    if claimed_count == 0:
        log.info("No pending rows to claim — aborting cycle")
        return {"status": "no_rows", "cycle_id": cycle_id}

    log.info("=== Cycle %s started — claimed %d rows ===", cycle_id, claimed_count)

    from app.agents.inspector import run_inspector
    from app.agents.injection import run_injection

    pr_url = ""
    try:
        pr_url = run_inspector(cycle_id=cycle_id)
    except Exception:
        log.error("Inspector failed [%s]:\n%s", cycle_id, traceback.format_exc())

    injection_status = ""
    try:
        injection_status = run_injection(cycle_id=cycle_id)
    except Exception:
        log.error("Injection failed [%s]:\n%s", cycle_id, traceback.format_exc())

    summary = (pr_url or "(no PR)")[:200]
    log.info("=== Cycle %s done — PR: %s ===", cycle_id, summary)

    return {
        "status": "pipeline_complete",
        "cycle_id": cycle_id,
        "pr_url": pr_url,
        "injection_status": injection_status,
        "claimed_count": claimed_count,
    }
