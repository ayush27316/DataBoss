"""
Push demo unstructured data to GCS bucket.
Simulates clients uploading messy, varied data that the Inspector Agent
will need to analyze and turn into a structured schema.

Usage:
    python scripts/push_demo_data.py
"""

import json
from google.cloud import storage


BUCKET_NAME = "data-connect-27316"


def get_client():
    return storage.Client.from_service_account_json("credentials.json")


# Each item represents one file landing in GCS — varied formats, missing fields,
# extra fields, inconsistent types. This is the kind of mess the agent has to handle.
DEMO_DATA = [
    {
        "filename": "user-signup-001.json",
        "payload": {
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "signed_up": "2026-03-15T10:30:00Z",
            "plan": "pro",
            "referral_code": "FRIEND50"
        },
    },
    {
        "filename": "user-signup-002.json",
        "payload": {
            "full_name": "Bob Smith",          # different key than above
            "email": "bob@example.com",
            "signup_date": "March 20, 2026",   # different date format
            "plan": "free"
            # no referral_code
        },
    },
    {
        "filename": "user-signup-003.json",
        "payload": {
            "name": "Charlie Lee",
            "email": "charlie@example.com",
            "signed_up": "2026-03-22",
            "plan": "enterprise",
            "company": "Acme Corp",            # extra field
            "referral_code": None
        },
    },
    {
        "filename": "user-signup-004.json",
        "payload": {
            "name": "Diana Patel",
            "email_address": "diana@example.com",  # yet another key name
            "signed_up": "2026-03-25T08:00:00Z",
            "plan": "pro",
            "age": 29                               # extra field, different type
        },
    },
    {
        "filename": "user-signup-005.json",
        "payload": {
            "name": "Eve Torres",
            "email": "eve@example.com",
            "signed_up": "2026-03-28",
            "plan": "free",
            "tags": ["beta-tester", "early-adopter"]  # nested array
        },
    },
    {
        "filename": "order-001.json",
        "payload": {
            "customer_email": "alice@example.com",
            "item": "Widget Pro",
            "quantity": 3,
            "price_cents": 4999,
            "ordered_at": "2026-03-16T14:20:00Z"
        },
    },
    {
        "filename": "order-002.json",
        "payload": {
            "customer_email": "bob@example.com",
            "items": [                                # array instead of single item
                {"name": "Gadget Basic", "qty": 1, "price": 19.99},
                {"name": "Cable Pack", "qty": 2, "price": 9.99}
            ],
            "order_date": "2026-03-21"
        },
    },
    {
        "filename": "order-003.json",
        "payload": {
            "customer": "Charlie Lee",               # name instead of email
            "item": "Enterprise License",
            "quantity": 1,
            "price_cents": 99900,
            "ordered_at": "2026-03-23T09:00:00Z",
            "notes": "Annual billing, PO #12345"     # free-text field
        },
    },
    {
        "filename": "feedback-001.json",
        "payload": {
            "user": "alice@example.com",
            "rating": 5,
            "comment": "Love the new dashboard!",
            "submitted_at": "2026-03-17"
        },
    },
    {
        "filename": "feedback-002.json",
        "payload": {
            "email": "bob@example.com",
            "score": "4 out of 5",                   # string instead of int
            "feedback": "Pretty good, needs dark mode",
            "timestamp": 1711200000                   # unix timestamp
        },
    },
]


def main():
    client = get_client()
    bucket = client.bucket(BUCKET_NAME)

    print(f"Pushing {len(DEMO_DATA)} files to gs://{BUCKET_NAME}/\n")

    for item in DEMO_DATA:
        blob = bucket.blob(item["filename"])
        data = json.dumps(item["payload"], indent=2)
        blob.upload_from_string(data, content_type="application/json")
        print(f"  uploaded  {item['filename']}  ({len(data)} bytes)")

    print(f"\nDone. {len(DEMO_DATA)} files pushed.")
    print("If Pub/Sub is configured, these should trigger webhook events on your Railway server.")


if __name__ == "__main__":
    main()
