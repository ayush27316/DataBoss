"""
Inspector Agent
---------------
Responsibilities (schema design + PR only — NO data movement):
  1. Read current schema from PROD_ tables.
  2. Read unstructured data from staging_raw.
  3. Design a normalised schema for DEV_ tables.
  4. Write dev_schema.sql (DDL) — canonical table definitions.
  5. Execute DDL to create DEV_ tables in the database.
  6. Write dbt staging models (pure SELECTs for CI rebuilds).
  7. Write migration_plan.sql — INSERT scripts to backfill PROD_ → DEV_ (if needed).
  8. Write injection_plan.sql — INSERT scripts to load staging → DEV_.
  9. Write schema_summary.md for human review.
  10. Commit everything to GitHub and open a PR.

The inspector does NOT execute migration or injection — that is the Injection
Agent's job.  It also does NOT mark staging rows.
"""

from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from sqlalchemy import text

from app.config import get_settings
from app.database import Session
from app.logging_config import get_logger
from app.services.dbt_runner import run_dbt, DBT_PROJECT_DIR
from app.services.github import get_github_toolkit

settings = get_settings()
log = get_logger("inspector")

STAGING_DIR = DBT_PROJECT_DIR / "models" / "staging"
SCHEMA_SUMMARY_PATH = DBT_PROJECT_DIR.parent / "schema_summary.md"
DEV_SCHEMA_PATH = DBT_PROJECT_DIR.parent / "dev_schema.sql"
MIGRATION_PLAN_PATH = DBT_PROJECT_DIR.parent / "migration_plan.sql"
INJECTION_PLAN_PATH = DBT_PROJECT_DIR.parent / "injection_plan.sql"


# ── Tools ────────────────────────────────────────────────────────────────────

@tool
def write_dbt_staging_model(filename: str, content: str) -> str:
    """Write a dbt staging model file (pure SELECT only) to dbt/models/staging/.

    Args:
        filename: e.g. "dev_users.sql"
        content: the full SQL content — must be a pure SELECT (no INSERT)
    """
    if not filename.endswith(".sql"):
        return "ERROR: filename must end with .sql"
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    filepath = STAGING_DIR / filename
    filepath.write_text(content)
    log.info("Wrote staging/%s", filename)
    return f"Written: dbt/models/staging/{filename}"


@tool
def read_dbt_staging_model(filename: str) -> str:
    """Read back a dbt staging model file.

    Args:
        filename: the .sql filename to read
    """
    filepath = STAGING_DIR / filename
    if not filepath.exists():
        return f"ERROR: {filepath} does not exist"
    return filepath.read_text()


@tool
def list_dbt_staging_models() -> str:
    """List all .sql files in dbt/models/staging/."""
    if not STAGING_DIR.exists():
        return "No staging model directory."
    files = sorted(STAGING_DIR.glob("*.sql"))
    return "\n".join(f"  staging/{f.name}" for f in files) if files else "No staging models."


@tool
def get_current_schema() -> str:
    """Return the schema of all PROD_ tables as a DDL-style summary."""
    with Session() as session:
        result = session.execute(text("""
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name LIKE 'prod\\_%'
            ORDER BY table_name, ordinal_position
        """))
        rows = result.fetchall()
    if not rows:
        return "No PROD_ tables found — this is the first migration cycle."
    lines = [
        f"{r.table_name}.{r.column_name}  {r.data_type}  ({'NULL' if r.is_nullable == 'YES' else 'NOT NULL'})"
        for r in rows
    ]
    return "\n".join(lines)


@tool
def read_staging_data(limit: int = 50) -> str:
    """Read pending/processing rows from staging_raw.

    Args:
        limit: max rows to return (default 50)
    """
    with Session() as session:
        result = session.execute(text(
            "SELECT id, gcs_object, raw_payload FROM staging_raw "
            "WHERE status IN ('pending', 'processing') LIMIT :n"
        ), {"n": limit})
        rows = result.fetchall()
    return str([{"id": r.id, "object": r.gcs_object, "payload": r.raw_payload} for r in rows])


@tool
def run_dbt_command(command: str) -> str:
    """Run a dbt command. Use ONLY for staging models (pure SELECTs).

    Args:
        command: e.g. 'run --select staging' or 'compile'
    """
    log.info("dbt %s", command)
    returncode, output = run_dbt(command.split())
    status = "SUCCESS" if returncode == 0 else "FAILED"
    log.info("dbt %s -> %s", command, status)
    return f"[{status}]\n{output}"


@tool
def run_sql(sql: str) -> str:
    """Execute raw SQL against the database.
    Use this ONLY for executing the dev_schema.sql DDL to create tables.
    Do NOT use this for INSERT/migration/injection — those are the Injection Agent's job.

    Args:
        sql: the SQL to execute
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
def check_prod_has_data() -> str:
    """Check whether any PROD_ tables exist AND contain data.
    Returns a summary so you know whether to write a migration_plan.sql.
    """
    with Session() as session:
        tables = session.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'prod\\_%' ESCAPE '\\'
        """)).fetchall()

        if not tables:
            return "NO_PROD_TABLES: No PROD_ tables exist. Do NOT write migration_plan.sql."

        counts = []
        total = 0
        for t in tables:
            cnt = session.execute(
                text(f'SELECT COUNT(*) FROM "{t.tablename}"')
            ).scalar()
            counts.append(f"  {t.tablename}: {cnt} rows")
            total += cnt

    if total == 0:
        return "PROD_TABLES_EMPTY: PROD_ tables exist but are empty. Do NOT write migration_plan.sql.\n" + "\n".join(counts)

    return "PROD_HAS_DATA: You MUST write migration_plan.sql.\n" + "\n".join(counts)


@tool
def write_dev_schema(content: str) -> str:
    """Write dev_schema.sql — CREATE TABLE DDL for all DEV_ tables.
    This is the canonical schema and gets committed to the PR.

    IMPORTANT DDL RULES for PostgreSQL:
    - Quote reserved words: "user", "order", "table", "group", etc.
    - Every column line needs a comma EXCEPT the last one.
    - Use TEXT for string columns (not VARCHAR).
    - Use TIMESTAMPTZ for timestamps.
    - All columns should be TEXT unless you have a strong reason for a specific type.
      This avoids type-mismatch errors during injection.

    Args:
        content: full SQL DDL (CREATE TABLE IF NOT EXISTS dev_* statements)
    """
    DEV_SCHEMA_PATH.write_text(content)
    log.info("Wrote dev_schema.sql")
    return f"Written: dev_schema.sql ({len(content)} chars)"


@tool
def read_dev_schema() -> str:
    """Read the current dev_schema.sql file."""
    if not DEV_SCHEMA_PATH.exists():
        return "No dev_schema.sql file exists yet."
    return DEV_SCHEMA_PATH.read_text()


@tool
def write_migration_plan(content: str) -> str:
    """Write migration_plan.sql — SQL INSERT statements to backfill PROD_ data into DEV_ tables.
    Only write this if check_prod_has_data returned PROD_HAS_DATA.

    The file should contain one INSERT statement per PROD_ table:
      INSERT INTO dev_<name> (col1, col2, ...)
      SELECT col1, col2, ...
      FROM prod_<name>
      ON CONFLICT DO NOTHING;

    This file is committed to the PR AND executed by the Injection Agent.

    Args:
        content: full SQL content with INSERT statements
    """
    MIGRATION_PLAN_PATH.write_text(content)
    log.info("Wrote migration_plan.sql")
    return f"Written: migration_plan.sql ({len(content)} chars)"


@tool
def read_migration_plan() -> str:
    """Read the current migration_plan.sql file."""
    if not MIGRATION_PLAN_PATH.exists():
        return "No migration_plan.sql exists."
    return MIGRATION_PLAN_PATH.read_text()


@tool
def write_injection_plan(content: str) -> str:
    """Write injection_plan.sql — SQL INSERT statements to load staging_raw data into DEV_ tables.

    The file should contain one INSERT statement per DEV_ table:
      INSERT INTO dev_<name> (col1, col2, ...)
      SELECT
        id AS source_id,
        gcs_object AS source_object,
        raw_payload->>'field1' AS col1,
        (raw_payload->>'field2')::INTEGER AS col2,
        ...
      FROM public.staging_raw
      WHERE status IN ('pending', 'processing')
        AND <entity filter>
      ON CONFLICT DO NOTHING;

    IMPORTANT:
    - Cast values to match the column types in dev_schema.sql exactly.
    - If a column is TEXT in dev_schema.sql, use raw_payload->>'key' (returns text).
    - If a column is INTEGER, cast: (raw_payload->>'key')::INTEGER
    - If a column is TIMESTAMPTZ, cast: (raw_payload->>'key')::TIMESTAMPTZ
    - If a column is JSONB, use raw_payload->'key' (returns jsonb).

    This file is committed to the PR AND executed by the Injection Agent.

    Args:
        content: full SQL content with INSERT statements
    """
    INJECTION_PLAN_PATH.write_text(content)
    log.info("Wrote injection_plan.sql")
    return f"Written: injection_plan.sql ({len(content)} chars)"


@tool
def read_injection_plan() -> str:
    """Read the current injection_plan.sql file."""
    if not INJECTION_PLAN_PATH.exists():
        return "No injection_plan.sql exists."
    return INJECTION_PLAN_PATH.read_text()


@tool
def write_schema_summary(content: str) -> str:
    """Write a human-readable schema summary (Markdown) to schema_summary.md.

    Args:
        content: full Markdown content describing the proposed schema
    """
    SCHEMA_SUMMARY_PATH.write_text(content)
    log.info("Wrote schema_summary.md")
    return f"Written: schema_summary.md ({len(content)} chars)"


@tool
def read_schema_summary() -> str:
    """Read the current schema_summary.md file."""
    if not SCHEMA_SUMMARY_PATH.exists():
        return "No schema_summary.md file exists yet."
    return SCHEMA_SUMMARY_PATH.read_text()


# ── Prompt ───────────────────────────────────────────────────────────────────

INSPECTOR_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """\
You are the Inspector Agent for an autonomous data pipeline running on PostgreSQL.

YOUR ROLE: Design the schema, create tables, write SQL scripts, open a PR.
You do NOT execute migration or injection. You do NOT mark staging rows.
Those are the Injection Agent's responsibilities.

CRITICAL RULES:
- PostgreSQL ONLY. JSON: raw_payload->>'key' (text), raw_payload->'key' (jsonb).
- staging_raw is a REAL TABLE — never use dbt ref() or source() for it.
- dbt staging models must be pure SELECTs only.
- NEVER use reserved PostgreSQL keywords as unquoted identifiers.
  Reserved words include: user, order, table, group, select, where, from, etc.
  Either AVOID these as column names or double-quote them: "user", "order".
  PREFERRED: use descriptive names instead (e.g. user_name, order_ref).
- PREFER TEXT for all string/date columns in the DDL. This avoids type-cast
  errors when the Injection Agent inserts data from raw_payload (which is text).
  Only use specific types (INTEGER, JSONB) when the data is clearly that type.
- Complete ALL 6 phases. Phase 6 (GitHub PR) is MANDATORY.

TABLE CONVENTION:
- PROD_<name> = live production tables (read-only)
- DEV_<name>  = your proposed new schema
- staging_raw = inbound buffer (id, gcs_bucket, gcs_object, received_at, status, raw_payload JSONB)

─────────────────────────────────────────────
PHASE 1 — Analyse
─────────────────────────────────────────────
  1. Call get_current_schema to see existing PROD_ tables.
  2. Call read_staging_data to inspect incoming data.
  3. Call check_prod_has_data to determine if migration is needed.
  4. Design a normalised schema for DEV_ tables that accommodates BOTH
     existing PROD_ columns AND new fields from the staging data.

─────────────────────────────────────────────
PHASE 2 — Write & execute dev_schema.sql (DDL)
─────────────────────────────────────────────
  5. Call write_dev_schema with CREATE TABLE IF NOT EXISTS statements for every DEV_ table.

     DDL CHECKLIST (verify before writing):
     ☐ No reserved words used as unquoted column names
     ☐ Every column line has a trailing comma EXCEPT the last
     ☐ All string/date columns use TEXT (not TIMESTAMPTZ, not VARCHAR)
     ☐ Only INTEGER for clearly numeric fields, JSONB for JSON arrays/objects
     ☐ Each table has: source_id INTEGER, source_object TEXT as first two columns

     Example:
       -- DEV schema for cycle {{cycle_id}}
       CREATE TABLE IF NOT EXISTS dev_users (
         source_id     INTEGER,
         source_object TEXT,
         email         TEXT,
         full_name     TEXT,
         plan          TEXT,
         signup_date   TEXT,
         age           INTEGER,
         tags          JSONB,
         referral_code TEXT
       );

  6. Execute the DDL by calling run_sql with the content.
     If it fails, read the error, fix the DDL in write_dev_schema, and retry.

─────────────────────────────────────────────
PHASE 3 — Write dbt staging models
─────────────────────────────────────────────
  7. Write one dbt staging model per DEV_ table using write_dbt_staging_model.
     These are pure SELECTs for CI rebuilds and documentation.
     Filename: dev_<name>.sql
     Template:
       {{{{ config(materialized='table', alias='dev_<name>') }}}}
       SELECT
         id AS source_id,
         gcs_object AS source_object,
         raw_payload->>'col1' AS col1,
         ...
       FROM public.staging_raw
       WHERE status IN ('pending', 'processing')
         AND <filter>

  8. Run run_dbt_command("run --select staging") to validate they compile.
     On failure: fix and retry (max 3 attempts).

─────────────────────────────────────────────
PHASE 4 — Write SQL scripts (do NOT execute them)
─────────────────────────────────────────────
  9. If check_prod_has_data returned "PROD_HAS_DATA":
     Call write_migration_plan with INSERT statements to backfill PROD_ → DEV_.
     Map only columns that exist in both tables.

  10. Call write_injection_plan with INSERT statements to load staging_raw → DEV_.
      One INSERT per DEV_ table. Cast types to match the DDL exactly.
      IMPORTANT: since all string columns are TEXT in the DDL, use raw_payload->>'key'
      directly (no cast needed for TEXT). Only cast for INTEGER/JSONB.

─────────────────────────────────────────────
PHASE 5 — Write schema summary
─────────────────────────────────────────────
  11. Call write_schema_summary using this EXACT template (fill in placeholders):

  ```
  # Schema Summary — Cycle {{cycle_id}}

  ## Overview
  <1-2 sentences: what data arrived, what tables were created/modified>

  ## Table Definitions

  ### dev_<name1>
  | Column | Type | Source | Description |
  |--------|------|--------|-------------|
  | source_id | INTEGER | staging_raw.id | Row ID from staging buffer |
  | source_object | TEXT | staging_raw.gcs_object | GCS file that contained this record |
  | <col> | <type> | raw_payload->>'<key>' | <description> |

  ### dev_<name2>
  ...repeat for each table...

  ## Changes from PROD_
  - <"First cycle — no existing PROD_ tables" OR list specific changes>
  - <e.g. "Added column `referral_code` (TEXT) to dev_users — not in prod_users">

  ## Data Classification
  - Total staging rows processed: <N>
  - Expected accepted: <N>
  - Expected rejected: <N> (reason: <why>)
  ```

─────────────────────────────────────────────
PHASE 6 — Commit to GitHub & open PR (MANDATORY)
─────────────────────────────────────────────
  12. Use the GitHub tools to:
      a. Create branch "agent/migration-{{cycle_id}}" from main.
      b. Commit each file by reading it first:
         - dbt staging models (read with read_dbt_staging_model) → dbt/models/staging/<file>
         - dev_schema.sql (read with read_dev_schema) → dev_schema.sql
         - injection_plan.sql (read with read_injection_plan) → injection_plan.sql
         - migration_plan.sql (read with read_migration_plan, if it exists) → migration_plan.sql
         - schema_summary.md (read with read_schema_summary) → schema_summary.md
      c. Open a PR titled "agent/migration-{{cycle_id}}" into main.

  PR BODY — use this EXACT template (fill in the placeholders):

  ```
  ## Proposed Schema Changes — Cycle {{cycle_id}}

  ### New / Modified DEV_ Tables
  | Table | Columns | Status |
  |-------|---------|--------|
  | dev_<name> | col1, col2, ... | NEW or MODIFIED |
  | ... | ... | ... |

  ### Changes from PROD_
  - <Bullet describing what changed: new table, new column, type change, etc.>
  - <If first cycle: "First cycle — no existing PROD_ tables.">

  ### Migration Plan
  - <"No migration needed — no PROD_ data." OR "Backfill from prod_X (N rows) → dev_X">

  ### Injection Plan
  - Staging rows classified: <N> accepted, <M> rejected
  - Injects into: dev_<name1>, dev_<name2>, ...

  ### Files in this PR
  - `dev_schema.sql` — DDL to create all DEV_ tables
  - `injection_plan.sql` — SQL to load staging data → DEV_ tables
  - `migration_plan.sql` — SQL to backfill PROD_ → DEV_ *(only if migration needed)*
  - `schema_summary.md` — Human-readable schema documentation
  - `dbt/models/staging/dev_*.sql` — dbt models for CI rebuilds

  ### What happens on merge
  1. `dev_schema.sql` creates DEV_ tables
  2. `migration_plan.sql` backfills PROD_ data (if present)
  3. `injection_plan.sql` loads staging data
  4. Atomic swap: DROP PROD_ → RENAME DEV_ to PROD_
  5. Accepted staging rows cleaned up
  ```

  THIS STEP IS REQUIRED. Do not skip it.

Cycle ID: {cycle_id}
"""),
    ("human", "Begin the migration pipeline for cycle {cycle_id}. Complete all 6 phases including the GitHub PR."),
    MessagesPlaceholder("agent_scratchpad"),
])


def run_inspector(cycle_id: str) -> str:
    """Run the Inspector Agent. Returns the agent output (typically PR info)."""
    log.info("Starting inspector [%s]", cycle_id)

    llm = ChatOpenAI(
        model=settings.xai_model,
        api_key=settings.xai_api_key,
        base_url=settings.xai_base_url,
        temperature=0,
    )

    branch_name = f"agent/migration-{cycle_id}"
    github_tools = get_github_toolkit(branch=branch_name).get_tools()
    custom_tools = [
        write_dbt_staging_model, read_dbt_staging_model, list_dbt_staging_models,
        get_current_schema, read_staging_data,
        run_dbt_command, run_sql,
        check_prod_has_data,
        write_dev_schema, read_dev_schema,
        write_migration_plan, read_migration_plan,
        write_injection_plan, read_injection_plan,
        write_schema_summary, read_schema_summary,
    ]
    all_tools = github_tools + custom_tools

    agent = create_tool_calling_agent(llm, all_tools, INSPECTOR_PROMPT)
    executor = AgentExecutor(
        agent=agent,
        tools=all_tools,
        verbose=False,
        max_iterations=80,
        handle_parsing_errors=True,
    )

    result = executor.invoke({"cycle_id": cycle_id})
    output = result.get("output", "")
    log.info("Inspector done [%s]", cycle_id)
    return output
