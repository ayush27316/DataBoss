{{ config(materialized='table', alias='dev_orders') }}

SELECT 
  id AS source_id,
  gcs_object AS source_object,
  NULLIF(raw_payload->>'customer_email', '') AS customer_email,
  NULLIF(raw_payload->>'ordered_at', '') AS ordered_at,
  NULLIF(raw_payload->>'order_date', '') AS order_date,
  NULLIF((raw_payload->>'price_cents')::int::text, '')::int AS price_cents,
  NULLIF((raw_payload->>'quantity')::int::text, '')::int AS quantity,
  NULLIF(raw_payload->>'item', '') AS item,
  raw_payload->'items'
FROM public.staging_raw 
WHERE status IN ('pending', 'processing')
  AND raw_payload ? ANY(ARRAY['customer_email', 'ordered_at', 'order_date'])
