from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from app.config import get_settings

# ---------------------------------------------------------------------------
# Single shared database
# Table naming convention:
#   PROD_<name>  — live production tables (what the app reads)
#   DEV_<name>   — agent's proposed new schema (built in parallel)
#   staging_raw  — inbound unstructured data buffer (no prefix, infrastructure)
# ---------------------------------------------------------------------------


@lru_cache
def get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


def Session():
    return sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)()


class Base(DeclarativeBase):
    pass


def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Staging table DDL
# ---------------------------------------------------------------------------

STAGING_DDL = """
CREATE TABLE IF NOT EXISTS staging_raw (
    id          SERIAL PRIMARY KEY,
    gcs_bucket  TEXT NOT NULL,
    gcs_object  TEXT NOT NULL,
    received_at TIMESTAMPTZ DEFAULT now(),
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | accepted | rejected
    raw_payload JSONB
);
"""


def ensure_staging_table():
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text(STAGING_DDL))
        conn.commit()


# ---------------------------------------------------------------------------
# Atomic table swap: DEV_ → PROD_
# ---------------------------------------------------------------------------

def swap_dev_to_prod():
    """
    Promote all DEV_ tables to PROD_ atomically.
    Returns a list of table names that were swapped (without prefix).
    """
    engine = get_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'dev\\_%' ESCAPE '\\'
        """)).fetchall()

        dev_tables = [r.tablename for r in rows]
        if not dev_tables:
            return []

        base_names = [t[len("dev_"):] for t in dev_tables]

        for base in base_names:
            prod = f"prod_{base}"
            dev  = f"dev_{base}"
            old  = f"_old_{base}"

            exists = conn.execute(text(
                "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=:t"
            ), {"t": prod}).fetchone()
            if exists:
                conn.execute(text(f'ALTER TABLE "{prod}" RENAME TO "{old}"'))

            conn.execute(text(f'ALTER TABLE "{dev}" RENAME TO "{prod}"'))

        for base in base_names:
            old = f"_old_{base}"
            exists = conn.execute(text(
                "SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=:t"
            ), {"t": old}).fetchone()
            if exists:
                conn.execute(text(f'DROP TABLE "{old}" CASCADE'))

    return base_names


def list_prod_tables() -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'prod\\_%' ESCAPE '\\'
        """)).fetchall()
    return [r.tablename[len("prod_"):] for r in rows]


def list_dev_tables() -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'dev\\_%' ESCAPE '\\'
        """)).fetchall()
    return [r.tablename[len("dev_"):] for r in rows]
