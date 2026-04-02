"""
Push *additional* demo JSON to GCS so a new pipeline cycle sees schema drift.

Same bucket and auth as push_demo_data.py, but uses new object names and payloads
that keep matching existing staging filters (email/plan/order keys, etc.) while
introducing extra attributes — phone, locale, gift metadata, NPS — so the
Inspector Agent is nudged to extend DEV_ column lists / normalization.

Usage:
    python scripts/push_demo_data_evolution.py
"""

import json
from google.cloud import storage


BUCKET_NAME = "data-connect-27316"


def get_client():
    return storage.Client.from_service_account_json("credentials.json")


# New blobs only (do not overwrite original demo files).
EVOLUTION_DATA = [
    {
        "filename": "evolve-user-001.json",
        "payload": {
            "name": "Jordan Kim",
            "email": "jordan.kim@example.com",
            "signed_up": "2026-04-01T11:00:00Z",
            "plan": "pro",
            "phone": "+1-415-555-0199",
            "locale": "en-AU",
            "job_title": "Data Engineer",
            "marketing_opt_in": True,
        },
    },
    {
        "filename": "evolve-user-002.json",
        "payload": {
            "full_name": "Sam Rivera",
            "email_address": "sam.rivera@example.com",
            "signup_date": "2026-04-02",
            "plan": "enterprise",
            "company": "Northwind Labs",
            "timezone": "America/Chicago",
            "employee_count": 120,
        },
    },
    {
        "filename": "evolve-order-001.json",
        "payload": {
            "customer_email": "jordan.kim@example.com",
            "item": "Sensor Kit v2",
            "quantity": 2,
            "price_cents": 12999,
            "ordered_at": "2026-04-01T16:45:00Z",
            "currency": "USD",
            "discount_code": "LAUNCH10",
            "gift_message": "Happy belated Q1 rollout",
            "tax_included": False,
        },
    },
    {
        "filename": "evolve-order-002.json",
        "payload": {
            "customer_email": "sam.rivera@example.com",
            "items": [
                {"name": "API Credits Pack", "qty": 1, "price": 249.0},
                {"name": "Support Plus (monthly)", "qty": 3, "price": 79.5},
            ],
            "order_date": "2026-04-02",
            "po_number": "PO-88421",
            "requested_ship_date": "2026-04-10",
        },
    },
    {
        "filename": "evolve-feedback-001.json",
        "payload": {
            "user": "jordan.kim@example.com",
            "rating": 4,
            "comment": "Great ETL previews; would like column-level lineage in the UI.",
            "submitted_at": "2026-04-02",
            "category": "product",
            "nps": 8,
            "session_id": "sess_9f3c2a1b",
        },
    },
    {
        "filename": "evolve-feedback-002.json",
        "payload": {
            "email": "sam.rivera@example.com",
            "score": "5 out of 5",
            "feedback": "Onboarding call was excellent.",
            "timestamp": 1712102400,
            "sentiment": "positive",
            "helpdesk_ticket": "HD-44012",
        },
    },
    {
        "filename": "evolve-user-003.json",
        "payload": {
            "name": "Priya Shah",
            "email": "priya.shah@example.com",
            "signed_up": "2026-04-03T09:15:00Z",
            "plan": "free",
            "referral_code": "PARTNER-77",
            "linkedin_url": "https://linkedin.com/in/priyashah",
            "country_code": "IN",
        },
    },
    {
        "filename": "evolve-user-004.json",
        "payload": {
            "full_name": "Marcus O'Neil",
            "email_address": "marcus.oneil@example.com",
            "signup_date": "April 4, 2026",
            "plan": "pro",
            "department": "Finance",
            "cost_center": "CC-9001",
            "manager_email": "cfo@example.com",
        },
    },
    {
        "filename": "evolve-user-005.json",
        "payload": {
            "name": "Yuki Tanaka",
            "email": "yuki.tanaka@example.com",
            "signed_up": "2026-04-05",
            "plan": "enterprise",
            "phone": "+81-3-5555-0100",
            "preferred_language": "ja",
            "sso_provider": "okta",
        },
    },
    {
        "filename": "evolve-user-006.json",
        "payload": {
            "name": "Alex Morgan",
            "email": "alex.morgan@example.com",
            "signed_up": "2026-04-06T12:00:00Z",
            "plan": "free",
            "age": 34,
            "tags": ["self-serve", "trial-extended"],
            "trial_ends": "2026-05-01",
        },
    },
    {
        "filename": "evolve-order-003.json",
        "payload": {
            "customer_email": "priya.shah@example.com",
            "item": "Starter Template Pack",
            "quantity": 1,
            "price_cents": 0,
            "order_date": "2026-04-03",
            "fulfillment_region": "ap-south-1",
            "vat_id": "IN29ABCDE1234F1Z5",
        },
    },
    {
        "filename": "evolve-order-004.json",
        "payload": {
            "customer_email": "marcus.oneil@example.com",
            "customer": "Marcus O'Neil",
            "item": "Annual Support Bundle",
            "quantity": 1,
            "price_cents": 480000,
            "ordered_at": "2026-04-04T10:00:00Z",
            "notes": "Net-45; invoice to subsidiary",
            "contract_id": "CNT-2026-441",
        },
    },
    {
        "filename": "evolve-order-005.json",
        "payload": {
            "customer_email": "yuki.tanaka@example.com",
            "items": [
                {"name": "Seat pack (10)", "qty": 1, "price": 990.0},
                {"name": "Training hours", "qty": 8, "price": 175.0},
            ],
            "ordered_at": "2026-04-05T07:30:00Z",
            "incoterms": "DAP",
            "forwarder_ref": "FW-8821",
        },
    },
    {
        "filename": "evolve-order-006.json",
        "payload": {
            "customer_email": "alex.morgan@example.com",
            "item": "Data residency add-on",
            "quantity": 1,
            "price_cents": 150000,
            "order_date": "2026-04-06",
            "billing_period": "annual",
            "auto_renew": True,
        },
    },
    {
        "filename": "evolve-feedback-003.json",
        "payload": {
            "user": "priya.shah@example.com",
            "rating": 3,
            "comment": "Docs are dense; a quickstart video would help.",
            "submitted_at": "2026-04-03T18:00:00Z",
            "page_url": "/docs/pipelines",
            "browser": "Chrome 123",
        },
    },
    {
        "filename": "evolve-feedback-004.json",
        "payload": {
            "email": "yuki.tanaka@example.com",
            "score": "4 out of 5",
            "feedback": "SSO login smooth; role sync took ~10 min.",
            "timestamp": 1712275200,
            "region": "tokyo",
            "feature_flags": ["new_nav", "beta_sql"],
        },
    },
]


def main():
    client = get_client()
    bucket = client.bucket(BUCKET_NAME)

    print(f"Pushing {len(EVOLUTION_DATA)} schema-evolution files to gs://{BUCKET_NAME}/\n")

    for item in EVOLUTION_DATA:
        blob = bucket.blob(item["filename"])
        data = json.dumps(item["payload"], indent=2)
        blob.upload_from_string(data, content_type="application/json")
        print(f"  uploaded  {item['filename']}  ({len(data)} bytes)")

    print(f"\nDone. {len(EVOLUTION_DATA)} files pushed.")
    print(
        "These reuse core keys your staging models already match, plus new fields "
        "for a slightly wider DEV_ schema on the next cycle."
    )


if __name__ == "__main__":
    main()
