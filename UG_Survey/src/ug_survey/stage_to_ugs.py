import argparse
import yaml
from pathlib import Path
from sqlalchemy import text
from .config import load_settings
from .logging_setup import setup_logging
from .db import mssql_engine, exec_query
from .qa import table_count

UGS_TABLES = [
    "tbl_ug_survey_stud_attr",
    "tbl_ug_survey_response",
    "tbl_ug_survey_intern",
    "tbl_ug_survey_intern_lrned",
    "tbl_ug_survey_intl_exp",
    "tbl_ug_survey_conn_to_psu",
]

def discover_terms(conn, stage_schema: str) -> list[str]:
    sql = f"SELECT DISTINCT term FROM {stage_schema}.tbl_stage_UG_Survey WHERE term IS NOT NULL"
    return [r[0] for r in exec_query(conn, sql)]

def delete_ugs_rows(conn, ugs_schema: str, terms: list[str]):
    for t in terms:
        for tbl in UGS_TABLES:
            exec_query(conn, f"DELETE FROM {ugs_schema}.{tbl} WHERE term = :t", {"t": t})

def run_inserts_from_files(conn, ugs_schema: str, sql_dir: Path):
    for name in sql_dir.glob("insert_*.sql"):
        sql_text = name.read_text(encoding="utf-8")
        exec_query(conn, sql_text)
        print(f"[OK] ran {name.name}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--settings", default="config/settings.yaml")
    args = ap.parse_args()

    cfg = load_settings(args.settings)
    logger = setup_logging("UG_Survey", cfg["app"]["log_dir"], cfg["app"].get("log_level") or "INFO")

    # Use default settings + MSSQL_URL env var.
    # mssql_engine() will read config/settings.yaml and let MSSQL_URL override database.url.
    eng = mssql_engine()

    stage_schema = "dbo"          # stage table lives in dbo
    ugs_schema   = "ugs"          # split tables live in ugs
    sql_dir      = Path(__file__).parent / "sql" / "ugs"

    with eng.begin() as conn:
        terms = discover_terms(conn, stage_schema)
        if not terms:
            logger.info("No terms in stage; nothing to do.")
            return

        # mirror proc behavior: clear target rows for those terms
        delete_ugs_rows(conn, ugs_schema, terms)

        # execute each INSERT … SELECT from /sql/ugs/*.sql
        run_inserts_from_files(conn, ugs_schema, sql_dir)

        # quick QA
        for t in UGS_TABLES:
            full = f"{ugs_schema}.{t}"
            logger.info(f"{full}: rows={table_count(conn, full)}")

if __name__ == "__main__":
    main()
