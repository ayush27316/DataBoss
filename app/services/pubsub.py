"""
Parse inbound Pub/Sub push messages from GCS bucket notifications.

GCS sends push notifications as HTTP POST to your webhook URL.
Body format:
  {
    "message": {
      "data": "<base64>",
      "attributes": {
        "bucketId": "...",
        "objectId": "...",
        "eventType": "OBJECT_FINALIZE" | "OBJECT_DELETE" | ...
      },
      "messageId": "...",
      "publishTime": "..."
    },
    "subscription": "projects/.../subscriptions/..."
  }
"""

import base64
from pydantic import BaseModel


class GCSNotification(BaseModel):
    bucket: str
    object_name: str
    event_type: str
    message_id: str


class PubSubMessage(BaseModel):
    data: str = ""
    attributes: dict = {}
    messageId: str = ""
    publishTime: str = ""


class PubSubPayload(BaseModel):
    message: PubSubMessage
    subscription: str = ""


def parse_gcs_notification(payload: PubSubPayload) -> GCSNotification | None:
    """
    Extract GCS metadata from a Pub/Sub push payload.
    Returns None if the event type is not OBJECT_FINALIZE (we only care about new uploads).
    """
    attrs = payload.message.attributes
    event_type = attrs.get("eventType", "")

    if event_type != "OBJECT_FINALIZE":
        return None

    return GCSNotification(
        bucket=attrs.get("bucketId", ""),
        object_name=attrs.get("objectId", ""),
        event_type=event_type,
        message_id=payload.message.messageId,
    )
