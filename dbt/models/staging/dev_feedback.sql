{{ config(materialized='table', alias='dev_feedback') }}

SELECT 
  id AS source_id,
  gcs_object AS source_object,
  NULLIF(COALESCE(raw_payload->>'user', raw_payload->>'email'), '') AS email,
  NULLIF(raw_payload->>'submitted_at', '') AS submitted_at,
  NULLIF(raw_payload->>'timestamp', '') AS timestamp,
  NULLIF((raw_payload->>'rating')::int::text, '')::int AS rating,
  NULLIF(raw_payload->>'score', '') AS score,
  NULLIF(raw_payload->>'comment', '') AS comment,
  NULLIF(raw_payload->>'feedback', '') AS feedback
FROM public.staging_raw 
WHERE status IN ('pending', 'processing')
  AND raw_payload ? ANY(ARRAY['rating', 'score', 'comment', 'feedback'])
