"""
Generate CREATE TABLE statements for existing public.dev_* tables from the live database.
Used for PR artifacts and local bootstrap documentation.
"""

from sqlalchemy import text

from app.database import get_engine


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _table_create_sql(conn, table_name: str) -> str:
    rows = conn.execute(
        text(
            """
            SELECT a.attname::text,
                   pg_catalog.format_type(a.atttypid, a.atttypmod),
                   NOT a.attnotnull AS is_nullable
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public'
              AND c.relkind = 'r'
              AND c.relname = :t
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """
        ),
        {"t": table_name},
    ).fetchall()
    if not rows:
        return ""
    col_lines = []
    for col_name, fmt_type, nullable in rows:
        null_sql = "" if nullable else " NOT NULL"
        col_lines.append(f"    {_quote_ident(col_name)} {fmt_type}{null_sql}")
    return (
        f"CREATE TABLE IF NOT EXISTS public.{_quote_ident(table_name)} (\n"
        + ",\n".join(col_lines)
        + "\n);"
    )


def render_dev_schema_sql(cycle_id: str) -> str:
    """
    Build a single .sql file containing CREATE TABLE for every public.dev_* table.
    Returns an empty string if no dev tables exist.
    """
    header = f"""-- dev_schema_{cycle_id}.sql — introspected DDL for public.dev_* (PostgreSQL).
--
-- Optional bootstrap (empty database): apply this file, then load staging_raw separately.
-- Typical pipeline: `dbt run --select path:models/staging` builds dev_* via CTAS.
-- After marking staging rows accepted: `dbt run --select inject_{cycle_id}_*`
--
-- This file is generated from the live database after the inspector run.

"""

    engine = get_engine()
    with engine.connect() as conn:
        tables = conn.execute(
            text(
                """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'dev\\_%' ESCAPE '\\'
            ORDER BY tablename
            """
            )
        ).fetchall()

    if not tables:
        return ""

    parts = [header.strip()]
    with engine.connect() as conn:
        for (tname,) in tables:
            stmt = _table_create_sql(conn, tname)
            if stmt:
                parts.append(stmt)
    return "\n\n".join(parts) + "\n"
