"""
Injection Agent
---------------
Runs immediately after the Inspector Agent finishes (before the PR is merged).

Executes the SQL scripts the Inspector wrote:
  1. migration_plan.sql — backfill PROD_ data into DEV_ tables (if file exists)
  2. injection_plan.sql — load staging_raw data into DEV_ tables

Then marks staging rows as accepted or rejected based on results.

Uses direct SQL execution — never dbt for INSERT statements.
"""

import textwrap
from pathlib import Path

from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from sqlalchemy import text

from app.config import get_settings
from app.database import Session
from app.logging_config import get_logger
from app.services.dbt_runner import DBT_PROJECT_DIR

settings = get_settings()
log = get_logger("injection")

MIGRATION_PLAN_PATH = DBT_PROJECT_DIR.parent / "migration_plan.sql"
INJECTION_PLAN_PATH = DBT_PROJECT_DIR.parent / "injection_plan.sql"
DEV_SCHEMA_PATH = DBT_PROJECT_DIR.parent / "dev_schema.sql"


@tool
def read_migration_plan() -> str:
    """Read the migration_plan.sql file written by the Inspector.
    Returns the SQL content, or a message if the file doesn't exist (meaning no migration needed).
    """
    if not MIGRATION_PLAN_PATH.exists():
        return "NO_MIGRATION_PLAN: File does not exist — no PROD_ data to migrate."
    content = MIGRATION_PLAN_PATH.read_text()
    if not content.strip():
        return "NO_MIGRATION_PLAN: File is empty — no PROD_ data to migrate."
    return content


@tool
def read_injection_plan() -> str:
    """Read the injection_plan.sql file written by the Inspector.
    This contains INSERT statements to load staging_raw data into DEV_ tables.
    """
    if not INJECTION_PLAN_PATH.exists():
        return "ERROR: injection_plan.sql does not exist — Inspector may have failed to write it."
    return INJECTION_PLAN_PATH.read_text()


@tool
def read_dev_schema() -> str:
    """Read the dev_schema.sql file to understand the DEV_ table definitions."""
    if not DEV_SCHEMA_PATH.exists():
        return "ERROR: dev_schema.sql does not exist."
    return DEV_SCHEMA_PATH.read_text()


@tool
def run_sql(sql: str) -> str:
    """Execute a SQL statement against the database.
    Use for running migration and injection INSERT statements.

    Args:
        sql: a single SQL statement to execute
    """
    log.info("Executing SQL (%d chars)", len(sql))
    try:
        with Session() as session:
            result = session.execute(text(sql))
            rowcount = result.rowcount if result.returns_rows is False else len(result.fetchall())
            session.commit()
        msg = f"SQL OK. Rows affected: {rowcount}"
        log.info(msg)
        return msg
    except Exception as e:
        msg = f"SQL FAILED: {e}"
        log.error(msg)
        return msg


@tool
def get_dev_table_columns(table_name: str) -> str:
    """Get the column names and types for a DEV_ table.

    Args:
        table_name: the full table name, e.g. 'dev_users'
    """
    with Session() as session:
        result = session.execute(text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :tbl
            ORDER BY ordinal_position
        """), {"tbl": table_name})
        rows = result.fetchall()
    if not rows:
        return f"ERROR: table {table_name} not found in database"
    return "\n".join(
        f"  {r.column_name}  {r.data_type}  ({'NULL' if r.is_nullable == 'YES' else 'NOT NULL'})"
        for r in rows
    )


@tool
def list_dev_tables() -> str:
    """List all DEV_ tables currently in the database with their row counts."""
    with Session() as session:
        tables = session.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'dev\\_%' ESCAPE '\\'
            ORDER BY tablename
        """)).fetchall()
        if not tables:
            return "No DEV_ tables found in the database."
        lines = []
        for t in tables:
            cnt = session.execute(text(f'SELECT COUNT(*) FROM "{t.tablename}"')).scalar()
            lines.append(f"  {t.tablename}: {cnt} rows")
    return "\n".join(lines)


@tool
def read_staging_sample(limit: int = 20) -> str:
    """Read a sample of pending/processing rows from staging_raw.

    Args:
        limit: max rows to return (default 20)
    """
    with Session() as session:
        result = session.execute(text(
            "SELECT id, gcs_object, raw_payload FROM staging_raw "
            "WHERE status IN ('pending', 'processing') LIMIT :n"
        ), {"n": limit})
        rows = result.fetchall()
    if not rows:
        return "No pending/processing rows in staging_raw."
    return str([{"id": r.id, "object": r.gcs_object, "payload": r.raw_payload} for r in rows])


@tool
def mark_staging_rows(accepted_ids: list[int], rejected_ids: list[int]) -> str:
    """Finalise staging rows after injection.
    Accepted rows are DELETED from staging_raw (data is now in DEV_ tables).
    Rejected rows are marked 'rejected' so they stay in the buffer for review.

    Args:
        accepted_ids: list of staging_raw IDs successfully injected — will be deleted
        rejected_ids: list of staging_raw IDs that failed injection — kept as rejected
    """
    with Session() as session:
        if accepted_ids:
            session.execute(text(
                "DELETE FROM staging_raw WHERE id = ANY(:ids)"
            ), {"ids": accepted_ids})
        if rejected_ids:
            session.execute(text(
                "UPDATE staging_raw SET status = 'rejected' WHERE id = ANY(:ids)"
            ), {"ids": rejected_ids})
        session.commit()
    msg = f"Deleted {len(accepted_ids)} accepted rows, marked {len(rejected_ids)} rejected."
    log.info(msg)
    return msg


@tool
def get_processing_row_ids() -> str:
    """Get all staging_raw row IDs currently in 'processing' status."""
    with Session() as session:
        result = session.execute(text(
            "SELECT id FROM staging_raw WHERE status = 'processing' ORDER BY id"
        ))
        ids = [r.id for r in result.fetchall()]
    return str(ids)


INJECTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """\
You are the Injection Agent for an autonomous data pipeline on PostgreSQL.

The Inspector Agent has already:
  - Created DEV_ tables (via dev_schema.sql DDL)
  - Written migration_plan.sql (if PROD_ data exists to backfill)
  - Written injection_plan.sql (to load staging_raw → DEV_ tables)

Your job: execute those scripts, then mark staging rows.

CRITICAL RULES:
- PostgreSQL only. JSON: raw_payload->>'key' (text), raw_payload->'key' (jsonb).
- Use run_sql for all SQL execution. NEVER use dbt for INSERTs.
- Execute each INSERT statement from the plan files ONE AT A TIME via run_sql.
  Do NOT pass the entire file at once — run_sql only accepts single statements.

STEPS:

1. Call read_migration_plan.
   - If it returns "NO_MIGRATION_PLAN" → skip migration.
   - Otherwise: split the content by semicolons and execute each INSERT via run_sql.
     If a migration INSERT fails, log it but continue with the rest.

2. Call read_injection_plan.
   - Split by semicolons and execute each INSERT via run_sql.
   - If an INSERT fails:
     a. Read the error carefully.
     b. Call get_dev_table_columns for the target table to check the actual schema.
     c. Fix the SQL (type casts, column names) and retry (up to 3 times per statement).
     d. If it still fails after retries, note which table failed.

3. After all INSERTs:
   - Call get_processing_row_ids.
   - If all injection INSERTs succeeded → mark ALL processing rows as accepted.
     Accepted rows are DELETED from staging_raw (their data now lives in DEV_ tables).
   - If some failed → still mark all as accepted (partial success is OK since
     the staging models will re-extract the data on rebuild).
   - Only mark as rejected if EVERY injection INSERT failed for EVERY table.
     Rejected rows stay in staging_raw for review/retry.

4. Report what happened: which tables succeeded, row counts, any errors.

IMPORTANT: Every processing row MUST be marked accepted or rejected before
you finish. NEVER leave rows in 'processing' status.

Cycle ID: {cycle_id}
"""),
    ("human", "Execute migration and injection for cycle {cycle_id}."),
    MessagesPlaceholder("agent_scratchpad"),
])


def run_injection(cycle_id: str) -> str:
    """Entrypoint: run the Injection Agent and return final status."""
    log.info("Starting injection for cycle %s", cycle_id)

    llm = ChatOpenAI(
        model=settings.xai_model,
        api_key=settings.xai_api_key,
        base_url=settings.xai_base_url,
        temperature=0,
    )

    tools = [
        read_migration_plan, read_injection_plan, read_dev_schema,
        run_sql,
        get_dev_table_columns, list_dev_tables,
        read_staging_sample,
        mark_staging_rows, get_processing_row_ids,
    ]

    agent = create_tool_calling_agent(llm, tools, INJECTION_PROMPT)
    executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=False,
        max_iterations=25,
        handle_parsing_errors=True,
    )

    result = executor.invoke({"cycle_id": cycle_id})
    output = result.get("output", "")
    log.info("Injection finished for cycle %s", cycle_id)
    return output
