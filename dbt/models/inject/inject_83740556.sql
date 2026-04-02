{{ config(materialized='table') }}

INSERT INTO dev_users (source_id, source_object, email, full_name, plan, company, signup_date, age, tags, referral_code)
SELECT source_id, source_object, email, full_name, plan, company, signup_date, age, tags, referral_code
FROM dev_users s
WHERE NOT EXISTS (
  SELECT 1 FROM dev_users t 
  WHERE t.source_id = s.source_id
);

INSERT INTO dev_orders (source_id, source_object, customer_email, ordered_at, order_date, price_cents, quantity, item, items)
SELECT source_id, source_object, customer_email, ordered_at, order_date, price_cents, quantity, item, items
FROM dev_orders s
WHERE NOT EXISTS (
  SELECT 1 FROM dev_orders t 
  WHERE t.source_id = s.source_id
);

INSERT INTO dev_feedback (source_id, source_object, email, submitted_at, timestamp, rating, score, comment, feedback)
SELECT source_id, source_object, email, submitted_at, timestamp, rating, score, comment, feedback
FROM dev_feedback s
WHERE NOT EXISTS (
  SELECT 1 FROM dev_feedback t 
  WHERE t.source_id = s.source_id
);