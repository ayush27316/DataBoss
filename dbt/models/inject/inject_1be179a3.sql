{{ config(materialized='table') }}

TRUNCATE TABLE dev_users, dev_feedback, dev_orders RESTART IDENTITY CASCADE;

INSERT INTO dev_users SELECT * FROM {{ ref('dev_users') }};
INSERT INTO dev_feedback SELECT * FROM {{ ref('dev_feedback') }};
INSERT INTO dev_orders SELECT * FROM {{ ref('dev_orders') }};