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
from app.services.dbt_runner import run_dbt, DBT_PROJECT_DIR
from app.services.github import get_github_toolkit

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
- staging_raw is a REAL TABLE already in the database. Query it directly in SQL:
    SELECT id, gcs_object, raw_payload FROM public.staging_raw WHERE status IN ('pending','processing')
  DO NOT use dbt ref() or source() for staging_raw — it is not a dbt model.
- Your dbt models must be plain SQL SELECTs that dbt materializes as tables.
- Keep models simple. If dbt fails twice with the same error, simplify the SQL.
- You MUST complete all 4 phases. Phase 4 (GitHub PR) is mandatory — do not stop before it.

TABLE CONVENTION:
- PROD_<name> = live production tables (read-only for you)
- DEV_<name>  = your proposed new schema (you create these via dbt)
- staging_raw = inbound buffer (columns: id, gcs_bucket, gcs_object, received_at, status, raw_payload JSONB)

PHASE 1 — Analyse
  1. Call get_current_schema to see existing PROD_ tables.
  2. Call read_staging_data to inspect incoming data.
  3. Design a normalized schema for DEV_ tables.

PHASE 2 — Write dbt models & iterate (max 5 dbt retries)
  4. Use write_dbt_model:
     - directory="staging", filename="dev_<name>.sql"
       Start each file with: {{{{ config(materialized='table', alias='dev_<name>') }}}}
       Write a SELECT from public.staging_raw using Postgres JSONB operators.
     - directory="inject", filename="inject_{cycle_id}.sql"
       Injection model that loads staging_raw rows into DEV_ tables.
  5. Run run_dbt_command("run").
  6. On failure: read the error carefully, fix with write_dbt_model, re-run.
     After 5 failures, move on with whatever succeeded.

PHASE 3 — Classify staging data
  7. Call mark_staging_rows with accepted/rejected IDs.

PHASE 4 — Commit to GitHub & open PR (MANDATORY)
  8. Use the GitHub tools to:
     a. Create branch "agent/migration-{cycle_id}" from main.
     b. Read each model with read_dbt_model and commit to the repo at the same path.
     c. Open a PR titled "agent/migration-{cycle_id}" into main with a summary body.
  THIS STEP IS REQUIRED. Do not skip it or claim you opened a PR without actually calling the tools.

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
    log.info("Inspector done [%s]", cycle_id)
    return output
