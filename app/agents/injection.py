"""
Injection step
--------------
Runs immediately after the Inspector Agent finishes (before the PR is merged).
Rebuilds DEV_ tables from staging_raw rows marked accepted using dbt models
inject_{cycle}_{suffix}.sql (SELECT-only, one model per DEV table).
"""

from app.logging_config import get_logger
from app.services.dbt_runner import run_dbt, DBT_PROJECT_DIR

log = get_logger("injection")

_INJECT_DIR = DBT_PROJECT_DIR / "models" / "inject"


def resolve_inject_model_names(cycle_id: str) -> list[str]:
    """
    Prefer inject_{cycle}_*.sql (one dbt model per DEV table).
    Fall back to inject_{cycle}.sql for older single-file layouts.
    """
    if not _INJECT_DIR.is_dir():
        return []
    multi = sorted(_INJECT_DIR.glob(f"inject_{cycle_id}_*.sql"))
    if multi:
        return [p.stem for p in multi]
    single = _INJECT_DIR / f"inject_{cycle_id}.sql"
    if single.is_file():
        return [single.stem]
    return []


def run_injection(cycle_id: str) -> str:
    """Run all inject_* dbt models for this cycle; return dbt log tail."""
    log.info("Starting injection for cycle %s", cycle_id)
    names = resolve_inject_model_names(cycle_id)
    if not names:
        msg = f"No inject models found for cycle {cycle_id}"
        log.warning(msg)
        return msg

    cmd = ["run", "--select"] + names
    log.info("dbt %s", " ".join(cmd))
    returncode, output = run_dbt(cmd)
    status = "SUCCESS" if returncode == 0 else "FAILED"
    log.info("Injection %s -> %s", names, status)
    log.info("Injection finished for cycle %s", cycle_id)
    return f"[{status}]\n{output}"
