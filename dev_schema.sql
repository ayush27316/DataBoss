-- DEV schema for cycle a580000d
CREATE TABLE IF NOT EXISTS dev_orders (
  source_id     INTEGER,
  source_object TEXT,
  customer_email TEXT,
  item          TEXT,
  quantity      INTEGER,
  price_cents   INTEGER,
  ordered_at    TEXT,
  notes         TEXT,
  items         JSONB
);

CREATE TABLE IF NOT EXISTS dev_users (
  source_id        INTEGER,
  source_object    TEXT,
  email            TEXT,
  name             TEXT,
  full_name        TEXT,
  plan             TEXT,
  signed_up        TEXT,
  signup_date      TEXT,
  age              INTEGER,
  company          TEXT,
  referral_code    TEXT,
  tags             JSONB
);

CREATE TABLE IF NOT EXISTS dev_feedback (
  source_id        INTEGER,
  source_object    TEXT,
  email            TEXT,
  "user"           TEXT,
  rating           INTEGER,
  score            TEXT,
  comment          TEXT,
  feedback         TEXT,
  submitted_at     TEXT,
  timestamp        TEXT
);
