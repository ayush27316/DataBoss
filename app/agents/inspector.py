"""
Inspector Agent
---------------
Responsibilities:
  1. Read current schema from PROD_ tables.
  2. Read unstructured data from staging_raw.
  3. Generate dbt models for DEV_ tables (new schema) + migration + injection scripts.
  4. Write models locally -> run dbt -> iterate until stable.
  5. Mark staging rows as accepted/rejected.
  6. Commit final working files to GitHub and open a PR.
"""

from langchain_openai import ChatOpenAI
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.tools import tool
from sqlalchemy import text

from app.config import get_settings
from app.database import Session
from app.logging_config import get_logger
from app.services.dev_schema_ddl import render_dev_schema_sql
from app.services.dbt_runner import run_dbt, DBT_PROJECT_DIR
from app.services.github import get_github_toolkit, push_dev_schema_artifact

settings = get_settings()
log = get_logger("inspector")

ALLOWED_DIRS = {
    "staging":    DBT_PROJECT_DIR / "models" / "staging",
    "migrations": DBT_PROJECT_DIR / "models" / "migrations",
    "inject":     DBT_PROJECT_DIR / "models" / "inject",
}


@tool
def write_dbt_model(directory: str, filename: str, content: str) -> str:
    """Write a dbt SQL model file to the local filesystem so dbt can run it.

    Args:
        directory: one of "staging", "migrations", or "inject"
        filename: e.g. "dev_users.sql" or "inject_a1b2c3d4.sql"
        content: the full SQL content of the dbt model
    """
    target_dir = ALLOWED_DIRS.get(directory)
    if target_dir is None:
        return f"ERROR: directory must be one of {list(ALLOWED_DIRS.keys())}"
    if not filename.endswith(".sql"):
        return "ERROR: filename must end with .sql"

    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / filename
    filepath.write_text(content)
    log.info("Wrote %s/%s", directory, filename)
    return f"Written: {filepath.relative_to(DBT_PROJECT_DIR.parent)}"


@tool
def read_dbt_model(directory: str, filename: str) -> str:
    """Read back a dbt model file you previously wrote.

    Args:
        directory: one of "staging", "migrations", or "inject"
        filename: the .sql filename to read
    """
    target_dir = ALLOWED_DIRS.get(directory)
    if target_dir is None:
        return f"ERROR: directory must be one of {list(ALLOWED_DIRS.keys())}"
    filepath = target_dir / filename
    if not filepath.exists():
        return f"ERROR: {filepath} does not exist"
    return filepath.read_text()


@tool
def list_dbt_models() -> str:
    """List all .sql files currently in the dbt models directories."""
    lines = []
    for name, dir_path in ALLOWED_DIRS.items():
        if dir_path.exists():
            files = sorted(dir_path.glob("*.sql"))
            for f in files:
                lines.append(f"  {name}/{f.name}")
    return "\n".join(lines) if lines else "No .sql model files found."


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
        return "No PROD_ tables found — this may be the first migration cycle."
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
    """Run a dbt command against the shared DB.

    Args:
        command: e.g. 'run' or 'run --select dev_users' or 'test'
    """
    log.info("dbt %s", command)
    returncode, output = run_dbt(command.split())
    status = "SUCCESS" if returncode == 0 else "FAILED"
    log.info("dbt %s -> %s", command, status)
    return f"[{status}]\n{output}"


@tool
def prod_needs_migration() -> str:
    """Return whether migration dbt models are needed (PROD_ → DEV_ backfill).

    Call this before writing anything under directory=\"migrations\".
    If this returns a string starting with 'no', you must NOT create migration models.
    """
    with Session() as session:
        rows = session.execute(text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'prod\\_%' ESCAPE '\\'
            ORDER BY tablename
        """)).fetchall()
        if not rows:
            return "no — no PROD_ tables exist; skip migrations."

        for (tname,) in rows:
            cnt = session.execute(
                text(f"SELECT COUNT(*) FROM public.{tname}")
            ).scalar()
            if cnt:
                return f"yes — {tname} has {cnt} row(s); migration models may be needed."
        return "no — all PROD_ tables are empty; skip migrations."


@tool
def mark_staging_rows(accepted_ids: list[int], rejected_ids: list[int]) -> str:
    """Mark staging rows as accepted or rejected.

    Args:
        accepted_ids: list of staging_raw IDs that fit the new model
        rejected_ids: list of staging_raw IDs that do not fit
    """
    with Session() as session:
        if accepted_ids:
            session.execute(text(
                "UPDATE staging_raw SET status = 'accepted' WHERE id = ANY(:ids)"
            ), {"ids": accepted_ids})
        if rejected_ids:
            session.execute(text(
                "UPDATE staging_raw SET status = 'rejected' WHERE id = ANY(:ids)"
            ), {"ids": rejected_ids})
        session.commit()
    msg = f"Marked {len(accepted_ids)} accepted, {len(rejected_ids)} rejected."
    log.info(msg)
    return msg


INSPECTOR_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """\
You are the Inspector Agent for an autonomous data pipeline running on PostgreSQL.

CRITICAL RULES:
- The database is PostgreSQL. Use ONLY Postgres syntax.
- JSON access: use  raw_payload->>'key'  (text) or  raw_payload->'key'  (json).
  DO NOT use Snowflake (payload:key), BigQuery (json_extract_scalar), or other dialects.
- staging_raw is a REAL TABLE already in the database. Query it directly in SQL.
  DO NOT use dbt ref() or source() for staging_raw — it is not a dbt model.
- Every dbt model file MUST be a single SELECT statement (no INSERT/DELETE/TRUNCATE).
- Keep models simple. If dbt fails twice with the same error, simplify the SQL.
- You MUST complete all 4 phases. Phase 4 (GitHub PR) is mandatory — do not stop before it.

TABLE CONVENTION:
- PROD_<name> = live production tables (read-only for you)
- DEV_<name>  = your proposed new schema (dbt builds public.dev_<name> tables)
- staging_raw = inbound buffer (columns: id, gcs_bucket, gcs_object, received_at, status, raw_payload JSONB)

PHASE 1 — Analyse
  1. Call get_current_schema to see existing PROD_ tables.
  2. Call read_staging_data to inspect incoming data.
  3. Design a normalized schema for DEV_ tables.

PHASE 2 — Staging, optional migrations, inject (max 5 dbt retries per wave)
  4. Call prod_needs_migration BEFORE touching directory=\"migrations\".
     - If the result starts with \"no\": never write migration models; skip dbt for migrations.
     - If it starts with \"yes\": you may write directory=\"migrations\" models that copy/transform
       PROD_ → DEV_ as needed, then run_dbt_command(\"run --select migrations\") (or \"run\" if combined).
  5. write_dbt_model directory=\"staging\", filename=\"dev_<name>.sql\" for each DEV table:
       {{{{ config(materialized='table', alias='dev_<name>') }}}}
       SELECT from public.staging_raw with JSONB operators.
       Use: WHERE status IN ('pending', 'processing')  so the batch being inspected is included.
  6. run_dbt_command(\"run --select staging\") (or \"run\") until staging succeeds.
  7. Inject models — ONE FILE PER dev_<name> table:
       directory=\"inject\", filename=\"inject_{cycle_id}_<name>.sql\"
       Example: dev_users.sql → inject_{cycle_id}_users.sql
       Each file must be ONLY:
         {{{{ config(materialized='table', alias='dev_<name>') }}}}
         The same SELECT as the matching staging model EXCEPT
         WHERE status = 'accepted' (not pending/processing).
       Never put INSERT or multiple statements in inject models.
  8. run_dbt_command(\"run --select staging\") again if you changed staging; then continue.

PHASE 3 — Classify staging, then materialize accepted rows
  9. Call mark_staging_rows with accepted/rejected IDs for the rows in this cycle.
  10. run_dbt_command with all inject models for this cycle, e.g.:
      run_dbt_command(\"run --select inject_{cycle_id}_users inject_{cycle_id}_orders ...\")
      (list every inject_{cycle_id}_*.sql model you created). This rebuilds DEV tables from accepted rows only.

PHASE 4 — Commit to GitHub & open PR (MANDATORY)
  11. Use the GitHub tools to:
      a. Create branch \"agent/migration-{cycle_id}\" from main.
      b. read_dbt_model each file you wrote (staging, inject, and migrations only if any) and commit at the same paths under dbt/models/...
      c. Open a PR titled \"agent/migration-{cycle_id}\" into main. In the body, list:
          - Staging models created
          - Inject models (inject_{cycle_id}_*) — SELECT-only, accepted rows
          - Whether migrations were skipped (prod_needs_migration said no) or which migration files were added
          - Note: dev_schema_{cycle_id}.sql is committed to dbt/artifacts/ on the same branch after the inspector finishes.
  Do not skip Phase 4 or claim a PR without calling the tools.

Cycle ID: {cycle_id}
"""),
    ("human", "Begin the migration pipeline for cycle {cycle_id}. Complete all 4 phases including the GitHub PR."),
    MessagesPlaceholder("agent_scratchpad"),
])


def run_inspector(cycle_id: str) -> str:
    """Run the Inspector Agent. Returns the opened PR URL."""
    log.info("Starting inspector [%s]", cycle_id)

    llm = ChatOpenAI(
        model=settings.xai_model,
        api_key=settings.xai_api_key,
        base_url=settings.xai_base_url,
        temperature=0,
    )

    github_tools = get_github_toolkit().get_tools()
    custom_tools = [
        write_dbt_model, read_dbt_model, list_dbt_models,
        get_current_schema, read_staging_data,
        prod_needs_migration,
        run_dbt_command, mark_staging_rows,
    ]
    all_tools = github_tools + custom_tools

    agent = create_tool_calling_agent(llm, all_tools, INSPECTOR_PROMPT)
    executor = AgentExecutor(
        agent=agent,
        tools=all_tools,
        verbose=False,
        max_iterations=40,
        handle_parsing_errors=True,
    )

    result = executor.invoke({"cycle_id": cycle_id})
    output = result.get("output", "")
    ddl = render_dev_schema_sql(cycle_id)
    if not ddl.strip():
        ddl = f"-- dev_schema_{cycle_id}.sql — no public.dev_* tables at end of cycle.\n"
    artifacts_dir = DBT_PROJECT_DIR / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifacts_dir / f"dev_schema_{cycle_id}.sql"
    artifact_path.write_text(ddl)
    push_msg = push_dev_schema_artifact(cycle_id, ddl)
    log.info("%s", push_msg)
    log.info("Inspector done [%s]", cycle_id)
    return output
