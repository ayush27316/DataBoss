"""
Delete every object in the demo GCS bucket (not the bucket itself).

Uses the same credentials and bucket as push_demo_data.py. Destructive — run only
when you intend to reset uploads before re-seeding.

Usage:
    python scripts/clear_gcs_bucket.py              # prompts for confirmation
    python scripts/clear_gcs_bucket.py --yes        # skip confirmation
"""

import argparse
import sys

from google.cloud import storage


BUCKET_NAME = "data-connect-27316"
CREDENTIALS_PATH = "credentials.json"


def get_client():
    return storage.Client.from_service_account_json(CREDENTIALS_PATH)


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Delete all objects in gs://{BUCKET_NAME}/")
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Delete without typing the bucket name",
    )
    args = parser.parse_args()

    client = get_client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs())
    if not blobs:
        print(f"gs://{BUCKET_NAME}/ is already empty.")
        return 0

    print(f"About to delete {len(blobs)} object(s) from gs://{BUCKET_NAME}/\n")

    if not args.yes:
        typed = input(f'Type the bucket name "{BUCKET_NAME}" to confirm: ').strip()
        if typed != BUCKET_NAME:
            print("Aborted (name did not match).")
            return 1

    deleted = 0
    for blob in blobs:
        blob.delete()
        deleted += 1
        print(f"  deleted  {blob.name}")

    print(f"\nDone. Removed {deleted} object(s) from gs://{BUCKET_NAME}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
