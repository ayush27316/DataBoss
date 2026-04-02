# Schema Summary — Cycle a580000d

## Overview
Incoming staging data contains user signups, orders, and feedback entries with varying field names and structures. Created three normalized DEV_ tables: dev_users, dev_orders, and dev_feedback to accommodate all fields from the 10 sample rows.

## Table Definitions

### dev_orders
| Column         | Type    | Source                      | Description                          |
|----------------|---------|-----------------------------|--------------------------------------|
| source_id      | INTEGER | staging_raw.id              | Row ID from staging buffer           |
| source_object  | TEXT    | staging_raw.gcs_object      | GCS file that contained this record  |
| customer_email | TEXT    | raw_payload->>'customer_email' | Customer email address             |
| item           | TEXT    | raw_payload->>'item'        | Single item name (when not array)    |
| quantity       | INTEGER | raw_payload->>'quantity'    | Quantity ordered                     |
| price_cents    | INTEGER | raw_payload->>'price_cents' | Price in cents                       |
| ordered_at     | TEXT    | raw_payload->>'ordered_at'  | Order timestamp                      |
| notes          | TEXT    | raw_payload->>'notes'       | Additional notes                     |
| items          | JSONB   | raw_payload->'items'        | Array of items (when present)        |

### dev_users
| Column        | Type    | Source                                   | Description                          |
|---------------|---------|------------------------------------------|--------------------------------------|
| source_id     | INTEGER | staging_raw.id                           | Row ID from staging buffer           |
| source_object | TEXT    | staging_raw.gcs_object                   | GCS file that contained this record  |
| email         | TEXT    | COALESCE(raw_payload->>'email_address', raw_payload->>'email') | User email |
| name          | TEXT    | raw_payload->>'name'                     | User name                            |
| full_name     | TEXT    | raw_payload->>'full_name'                | Full user name                       |
| plan          | TEXT    | raw_payload->>'plan'                     | Subscription plan                    |
| signed_up     | TEXT    | raw_payload->>'signed_up'                | Signup timestamp                     |
| signup_date   | TEXT    | raw_payload->>'signup_date'              | Signup date                          |
| age           | INTEGER | raw_payload->>'age'                      | User age                             |
| company       | TEXT    | raw_payload->>'company'                  | Company name                         |
| referral_code | TEXT    | raw_payload->>'referral_code'            | Referral code                        |
| tags          | JSONB   | raw_payload->'tags'                      | User tags array                      |

### dev_feedback
| Column        | Type    | Source                                   | Description                          |
|---------------|---------|------------------------------------------|--------------------------------------|
| source_id     | INTEGER | staging_raw.id                           | Row ID from staging buffer           |
| source_object | TEXT    | staging_raw.gcs_object                   | GCS file that contained this record  |
| email         | TEXT    | COALESCE(raw_payload->>'user', raw_payload->>'email') | Feedback email |
| "user"        | TEXT    | raw_payload->>'user'                     | User identifier                      |
| rating        | INTEGER | raw_payload->>'rating'                   | Numeric rating                       |
| score         | TEXT    | raw_payload->>'score'                    | Text score                           |
| comment       | TEXT    | raw_payload->>'comment'                  | Comment text                         |
| feedback      | TEXT    | raw_payload->>'feedback'                 | Feedback text                        |
| submitted_at  | TEXT    | raw_payload->>'submitted_at'             | Submission timestamp                 |
| timestamp     | TEXT    | raw_payload->>'timestamp'                | Unix timestamp                       |

## Changes from PROD_
- First cycle — no existing PROD_ tables

## Data Classification
- Total staging rows processed: 10
- Expected accepted: 10 (all rows match one of the three entity types)
- Expected rejected: 0 (reason: N/A)
