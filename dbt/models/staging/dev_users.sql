{{ config(materialized='table', alias='dev_users') }}

SELECT 
  id AS source_id,
  gcs_object AS source_object,
  COALESCE(NULLIF(raw_payload->>'email', ''), NULLIF(raw_payload->>'email_address', '')) AS email,
  COALESCE(NULLIF(raw_payload->>'name', ''), NULLIF(raw_payload->>'full_name', '')) AS full_name,
  NULLIF(raw_payload->>'plan', '') AS plan,
  NULLIF(raw_payload->>'company', '') AS company,
  NULLIF(raw_payload->>'signed_up', '') AS signup_date,
  NULLIF((raw_payload->>'age')::int::text, '')::int AS age,
  raw_payload->'tags',
  NULLIF(raw_payload->>'referral_code', '') AS referral_code
FROM public.staging_raw 
WHERE status IN ('pending', 'processing')
  AND (raw_payload ? 'email' OR raw_payload ? 'email_address')
