{{ config(materialized='table', alias='inject_d5e3d7ca') }}

-- Insert users
INSERT INTO dev_users (email, name, plan, signup_timestamp, company, referral_code, age, tags, gcs_object, received_at)
SELECT DISTINCT ON (email)
    email, name, plan, signup_timestamp, company, referral_code, age, tags, gcs_object, received_at
FROM dev_users
WHERE email IS NOT NULL;

-- Insert orders
INSERT INTO dev_orders (id, gcs_object, item_name, quantity, unit_price, order_timestamp, customer_email, notes, received_at, num_items)
SELECT * FROM dev_orders;

-- Insert feedback
INSERT INTO dev_feedback (id, gcs_object, user_email, rating, comment_text, feedback_timestamp, received_at)
SELECT * FROM dev_feedback;