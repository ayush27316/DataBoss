"""
Delete demo unstructured data from the GCS bucket.

This removes the same object names that `scripts/push_demo_data.py` uploads.

Usage:
    python scripts/delete_demo_data.py
    python scripts/delete_demo_data.py --dry-run
"""

import argparse
from google.cloud import storage


BUCKET_NAME = "data-connect-27316"

# Keep this list in sync with scripts/push_demo_data.py
DEMO_FILENAMES = [
    "user-signup-001.json",
    "user-signup-002.json",
    "user-signup-003.json",
    "user-signup-004.json",
    "user-signup-005.json",
    "order-001.json",
    "order-002.json",
    "order-003.json",
    "feedback-001.json",
    "feedback-002.json",
]


def get_client():
    return storage.Client.from_service_account_json("credentials.json")


def delete_demo_files(dry_run: bool = False):
    client = get_client()
    bucket = client.bucket(BUCKET_NAME)

    print(f"Target bucket: gs://{BUCKET_NAME}/")
    print(f"Demo files configured: {len(DEMO_FILENAMES)}")
    if dry_run:
        print("Running in dry-run mode; no files will be deleted.\n")
    else:
        print("Deleting files...\n")

    deleted = 0
    missing = 0

    for filename in DEMO_FILENAMES:
        blob = bucket.blob(filename)
        if not blob.exists():
            print(f"  missing   {filename}")
            missing += 1
            continue

        if dry_run:
            print(f"  would delete  {filename}")
        else:
            blob.delete()
            print(f"  deleted   {filename}")
            deleted += 1

    print("\nDone.")
    if dry_run:
        print(f"Dry run complete. {len(DEMO_FILENAMES) - missing} file(s) would be deleted, {missing} missing.")
    else:
        print(f"Deleted {deleted} file(s), {missing} missing.")


def parse_args():
    parser = argparse.ArgumentParser(description="Delete demo data files from GCS.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    delete_demo_files(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
