from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request

from app.config import get_settings
from app.database import ensure_staging_table
from app.logging_config import setup_logging, get_logger
from app.services.pubsub import PubSubPayload, parse_gcs_notification
from app.pipeline.orchestrator import handle_gcs_event

setup_logging()

settings = get_settings()
log = get_logger("server")


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_staging_table()
    log.info("JDataBoss started — staging table ready")
    yield


app = FastAPI(title="JDataBoss", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/webhook/pubsub")
async def pubsub_webhook(payload: PubSubPayload, background_tasks: BackgroundTasks, request: Request):
    if settings.pubsub_verification_token:
        token = request.query_params.get("token", "")
        if token != settings.pubsub_verification_token:
            raise HTTPException(status_code=403, detail="Invalid verification token")

    notification = parse_gcs_notification(payload)
    if notification is None:
        return {"status": "ignored"}

    log.info("Accepted webhook for %s — queued for processing", notification.object_name)
    background_tasks.add_task(handle_gcs_event, notification)
    return {"status": "accepted", "object": notification.object_name}
