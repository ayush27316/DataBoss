-- injection_plan.sql for cycle a580000d
-- Load orders
INSERT INTO dev_orders (
  source_id, source_object, customer_email, item, quantity, price_cents, ordered_at, notes, items
)
SELECT
  id,
  gcs_object,
  raw_payload->>'customer_email',
  raw_payload->>'item',
  NULLIF(raw_payload->>'quantity', '')::INTEGER,
  NULLIF(raw_payload->>'price_cents', '')::INTEGER,
  raw_payload->>'ordered_at',
  raw_payload->>'notes',
  raw_payload->'items'
FROM staging_raw
WHERE status IN ('pending', 'processing')
  AND raw_payload ?| ARRAY['item','items','customer_email','customer']
  AND raw_payload->>'customer_email' IS NOT NULL
ON CONFLICT DO NOTHING;

-- Load users
INSERT INTO dev_users (
  source_id, source_object, email, name, full_name, plan, signed_up, signup_date, age, company, referral_code, tags
)
SELECT
  id,
  gcs_object,
  COALESCE(NULLIF(raw_payload->>'email_address', ''), NULLIF(raw_payload->>'email', '')),
  raw_payload->>'name',
  raw_payload->>'full_name',
  raw_payload->>'plan',
  raw_payload->>'signed_up',
  raw_payload->>'signup_date',
  NULLIF(raw_payload->>'age', '')::INTEGER,
  raw_payload->>'company',
  raw_payload->>'referral_code',
  raw_payload->'tags'
FROM staging_raw
WHERE status IN ('pending', 'processing')
  AND raw_payload ?| ARRAY['name','full_name','email','email_address','plan']
  AND COALESCE(NULLIF(raw_payload->>'email_address', ''), NULLIF(raw_payload->>'email', '')) IS NOT NULL
ON CONFLICT DO NOTHING;

-- Load feedback
INSERT INTO dev_feedback (
  source_id, source_object, email, "user", rating, score, comment, feedback, submitted_at, timestamp
)
SELECT
  id,
  gcs_object,
  COALESCE(NULLIF(raw_payload->>'user', ''), NULLIF(raw_payload->>'email', '')),
  raw_payload->>'user',
  NULLIF(raw_payload->>'rating', '')::INTEGER,
  raw_payload->>'score',
  raw_payload->>'comment',
  raw_payload->>'feedback',
  raw_payload->>'submitted_at',
  raw_payload->>'timestamp'
FROM staging_raw
WHERE status IN ('pending', 'processing')
  AND raw_payload ?| ARRAY['user','email','rating','score','comment','feedback']
  AND COALESCE(NULLIF(raw_payload->>'user', ''), NULLIF(raw_payload->>'email', '')) IS NOT NULL
ON CONFLICT DO NOTHING;
