"""
ETL for ugs.tbl_ug_survey_empl_job_lrned

- Reads EMP_how_obtain_* columns from whsestage.dbo.tbl_stage_UG_Survey
- Unpivots each selected option into a row
- Loads into ugs.tbl_ug_survey_empl_job_lrned

This is a simple fact table:
    stud_id (char(9))
    term (char(4))
    empl_lmed_abt_job_cd (int)
    empl_lmed_abt_job (varchar(300))
    load_date (datetime, set in ETL or defaulted in SQL)

The IDENTITY column [id] is managed by SQL Server; we do NOT insert into it.
"""

import os
import sys
import logging
from datetime import datetime, timezone
from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER_NAME = "UG_Survey"

# -------------------------------------------------------------------
# Mapping: how-obtained columns -> code + description
# Descriptions are placeholders and can be refined later if needed.
# -------------------------------------------------------------------
EMP_HOW_OBTAIN_MAP: Dict[str, Dict[str, object]] = {
    "EMP_how_obtain_01": {"code": 1, "description": "EMP_how_obtain_01"},
    "EMP_how_obtain_02": {"code": 2, "description": "EMP_how_obtain_02"},
    "EMP_how_obtain_03": {"code": 3, "description": "EMP_how_obtain_03"},
    "EMP_how_obtain_04": {"code": 4, "description": "EMP_how_obtain_04"},
    "EMP_how_obtain_05": {"code": 5, "description": "EMP_how_obtain_05"},
    "EMP_how_obtain_06": {"code": 6, "description": "EMP_how_obtain_06"},
    "EMP_how_obtain_07": {"code": 7, "description": "EMP_how_obtain_07"},
    "EMP_how_obtain_08": {"code": 8, "description": "EMP_how_obtain_08"},
    "EMP_how_obtain_09": {"code": 9, "description": "EMP_how_obtain_09"},
    "EMP_how_obtain_10": {"code": 10, "description": "EMP_how_obtain_10"},
    "EMP_how_obtain_11": {"code": 11, "description": "EMP_how_obtain_11"},
    "EMP_how_obtain_12": {"code": 12, "description": "EMP_how_obtain_12"},
    "EMP_how_obtain_13": {"code": 13, "description": "EMP_how_obtain_13"},
    "EMP_how_obtain_14": {"code": 14, "description": "EMP_how_obtain_14"},
    "EMP_how_obtain_other": {"code": 99, "description": "Other"},
}

# ----------------------- logging / engine helpers -----------------------


def setup_logging() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        )
        logger.addHandler(h)
    return logger


logger = setup_logging()


def get_engine() -> Engine:
    url = os.getenv("MSSQL_URL")
    if not url:
        raise RuntimeError("MSSQL_URL environment variable is not set")
    logger.info("Using MSSQL_URL=%s", url)
    return create_engine(url, fast_executemany=True)


# ----------------------------- EXTRACT ---------------------------------


def extract_stage(engine: Engine) -> pd.DataFrame:
    """
    Pull how-obtained columns from stage table, plus student_id & term.
    """
    sql = """
        SELECT
            student_id,
            term,
            EMP_how_obtain_01, EMP_how_obtain_02, EMP_how_obtain_03,
            EMP_how_obtain_04, EMP_how_obtain_05, EMP_how_obtain_06,
            EMP_how_obtain_07, EMP_how_obtain_08, EMP_how_obtain_09,
            EMP_how_obtain_10, EMP_how_obtain_11, EMP_how_obtain_12,
            EMP_how_obtain_13, EMP_how_obtain_14,
            EMP_how_obtain_other,
            EMP_how_obtain_other_fill
        FROM whsestage.dbo.tbl_stage_UG_Survey
        WHERE Have_PostGrad_Info IS NOT NULL
    """
    logger.info("Extracting EMP_how_obtain_* from tbl_stage_UG_Survey ...")
    df = pd.read_sql(sql, engine)
    logger.info("Stage extract rows: %s", len(df))
    return df


# ---------------------------- TRANSFORM --------------------------------


def _is_selected(val) -> bool:
    """
    Generic flag detection: treat non-empty / non-zero / non-NO as selected.
    """
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    if isinstance(val, str):
        v = val.strip().upper()
        if v in ("", "0", "N", "NO", "NONE"):
            return False
        return True
    return bool(val)


def transform_empl_job_lrned(stage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn EMP_how_obtain_* flags into multiple rows per student.

    Output columns:
        stud_id               char(9)
        term                  char(4)
        empl_lmed_abt_job_cd  int
        empl_lmed_abt_job     varchar(300)
        load_date             datetime
    """
    if stage_df.empty:
        logger.warning("Stage dataframe is empty; nothing to transform.")
        return pd.DataFrame()

    df = stage_df.copy()

    # Normalize IDs and terms
    df["stud_id"] = (
        df["student_id"].astype(str).str.strip().str.zfill(9)
    )
    df["term"] = df["term"].astype(str).str.strip().str.zfill(4)

    records = []

    for _, row in df.iterrows():
        stud_id = row["stud_id"]
        term = row["term"]

        for col, meta in EMP_HOW_OBTAIN_MAP.items():
            if col not in row:
                continue

            flag = row[col]
            if not _is_selected(flag):
                continue

            code = meta["code"]
            description = meta["description"]

            # If "Other" and we have free text, append it
            if col == "EMP_how_obtain_other":
                other_txt = row.get("EMP_how_obtain_other_fill")
                if isinstance(other_txt, str) and other_txt.strip():
                    description = f"{description}: {other_txt.strip()}"

            records.append(
                {
                    "stud_id": stud_id,
                    "term": term,
                    "empl_lmed_abt_job_cd": code,
                    "empl_lmed_abt_job": description,
                    "load_date": datetime.now(timezone.utc),
                }
            )

    out_df = pd.DataFrame.from_records(records)
    logger.info("Transformed to %s empl_job_lrned rows", len(out_df))
    return out_df


# ------------------------------ LOAD -----------------------------------


def load_empl_job_lrned(engine: Engine, df: pd.DataFrame) -> None:
    """
    Truncate and reload ugs.tbl_ug_survey_empl_job_lrned using fast executemany.

    We only insert:
        stud_id,
        term,
        empl_lmed_abt_job_cd,
        empl_lmed_abt_job,
        load_date

    The [id] column is IDENTITY and is not included in the INSERT.
    """
    if df.empty:
        logger.info("No rows to insert into ugs.tbl_ug_survey_empl_job_lrned.")
        return

    # Column order must match INSERT
    cols = [
        "stud_id",
        "term",
        "empl_lmed_abt_job_cd",
        "empl_lmed_abt_job",
        "load_date",
    ]
    df = df[cols].where(pd.notnull(df), None)

    insert_sql = """
        INSERT INTO ugs.tbl_ug_survey_empl_job_lrned (
            stud_id,
            term,
            empl_lmed_abt_job_cd,
            empl_lmed_abt_job,
            load_date
        ) VALUES (?, ?, ?, ?, ?)
    """

    # Truncate first (full refresh pattern, like other UGS facts)
    with engine.begin() as conn:
        logger.info("Truncating ugs.tbl_ug_survey_empl_job_lrned ...")
        conn.execute(text("TRUNCATE TABLE ugs.tbl_ug_survey_empl_job_lrned;"))

    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass  # not critical

        params = [tuple(r) for r in df.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, params)
        conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    logger.info(
        "Inserted %s rows into ugs.tbl_ug_survey_empl_job_lrned.", len(df)
    )


# ----------------------------- DRIVER ----------------------------------


def build_tbl_ug_survey_empl_job_lrned(dry_run: bool = False) -> None:
    logger.info(
        "[%s] Starting ETL for ugs.tbl_ug_survey_empl_job_lrned",
        datetime.now(timezone.utc),
    )
    engine = get_engine()

    stage_df = extract_stage(engine)
    if stage_df.empty:
        logger.warning("Stage returned 0 rows; nothing to do.")
        return

    out_df = transform_empl_job_lrned(stage_df)
    if out_df.empty:
        logger.warning("Transform produced 0 rows; nothing to insert.")
        return

    logger.info("Prepared %s rows for load", len(out_df))

    if dry_run:
        logger.info("Dry run enabled; NOT loading data.")
        return

    load_empl_job_lrned(engine, out_df)

    logger.info(
        "[%s] ETL for ugs.tbl_ug_survey_empl_job_lrned completed",
        datetime.now(timezone.utc),
    )


if __name__ == "__main__":
    # simple CLI: python -m ug_survey.etl_empl_job_lrned
    build_tbl_ug_survey_empl_job_lrned(dry_run=False)
