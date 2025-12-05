"""
ETL for ugs.tbl_ug_survey_intl_exp

- Reads international experience flags from whsestage.dbo.tbl_stage_UG_Survey
- Unpivots each selected option into rows:
    stud_id, term, intl_exp_type, load_date
"""

import os
import sys
import logging
from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER_NAME = "UG_Survey"

logger = logging.getLogger(LOGGER_NAME)


def setup_logging() -> logging.Logger:
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        )
        logger.addHandler(h)
    return logger


setup_logging()


def get_engine() -> Engine:
    url = os.getenv("MSSQL_URL")
    if not url:
        raise RuntimeError("MSSQL_URL environment variable is not set")
    logger.info("Using MSSQL_URL=%s", url)
    return create_engine(url, fast_executemany=True)


# ----------------------------------------------------------------------
# International experience mapping
# ----------------------------------------------------------------------

# Each entry maps a stage-table boolean-ish flag column to:
#  - a human-readable label
#  - an optional "definition" column whose text we prefer if present
#  - an optional "other_text" column to append free text
INTL_EXP_MAP: Dict[str, Dict[str, Any]] = {
    # legacy flags
    "int_exp_semAbroad": {
        "label": "Semester abroad",
        "definition_col": "int_exp_semAbroad_definition",
    },
    "int_exp_AYEA": {
        "label": "Academic year education abroad (AYEA)",
        "definition_col": "int_exp_AYEA_definition",
    },
    "int_exp_embedded": {
        "label": "Embedded education abroad course",
        "definition_col": "int_exp_embedded_definition",
    },
    "int_exp_studorg": {
        "label": "Student organization abroad experience",
        "definition_col": "int_exp_studorg_definition",
    },
    "int_exp_other": {
        "label": "Other international experience",
        "other_text_col": "int_exp_other_fill",
    },
    "int_exp_none": {
        "label": "No international experience",
        # no definition column
    },

    # newer PS-design block
    "int_exp_long_via_psu": {
        "label": "Long-term experience abroad via PSU",
        "definition_col": "int_exp_long_via_psu_defin",
    },
    "int_exp_short_psu_course": {
        "label": "Short-term PSU course abroad",
        "definition_col": "int_exp_short_psu_course_defin",
    },
    "int_exp_short_psu_club_org": {
        "label": "Short-term PSU club/organization abroad",
        "definition_col": "int_exp_short_psu_club_org_def",
    },
    "int_exp_educ_prog_not_psu": {
        "label": "Educational program abroad not via PSU",
        "definition_col": "int_exp_educ_prog_not_psu_defin",
    },
    "int_exp_internship": {
        "label": "International internship",
        "definition_col": "int_exp_internship_definition",
    },
    "int_exp_did_not_have": {
        "label": "Did not have an international experience",
        "definition_col": "int_exp_did_not_have_defin",
    },
}


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _is_selected(val) -> bool:
    """Return True if a flag-like value means 'selected'."""
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    if isinstance(val, str):
        v = val.strip()
        if v == "":
            return False
        vu = v.upper()
        if vu in ("0", "N", "NO", "NONE"):
            return False
        # anything else non-empty counts as selected
        return True
    try:
        # numeric / boolean
        return bool(val)
    except Exception:
        return False


# ----------------------------------------------------------------------
# EXTRACT
# ----------------------------------------------------------------------

def extract_stage(engine: Engine) -> pd.DataFrame:
    """
    Extract international-experience related columns from tbl_stage_UG_Survey.
    """
    sql = """
        SELECT
            student_id AS stud_id,
            term,

            -- legacy block
            int_exp_semAbroad,
            int_exp_AYEA,
            int_exp_embedded,
            int_exp_studorg,
            int_exp_other,
            int_exp_other_fill,
            int_exp_none,
            int_exp_semAbroad_definition,
            int_exp_AYEA_definition,
            int_exp_embedded_definition,
            int_exp_studorg_definition,

            -- newer block
            int_exp_long_via_psu,
            int_exp_short_psu_course,
            int_exp_short_psu_club_org,
            int_exp_educ_prog_not_psu,
            int_exp_internship,
            int_exp_did_not_have,
            int_exp_long_via_psu_defin,
            int_exp_short_psu_course_defin,
            int_exp_short_psu_club_org_def,
            int_exp_educ_prog_not_psu_defin,
            int_exp_internship_definition,
            int_exp_did_not_have_defin
        FROM whsestage.dbo.tbl_stage_UG_Survey;
    """

    logger.info("Extracting international experience flags from tbl_stage_UG_Survey ...")
    df = pd.read_sql(sql, engine)
    logger.info("Stage extract rows: %s", len(df))
    return df


# ----------------------------------------------------------------------
# TRANSFORM
# ----------------------------------------------------------------------

def transform_intl_exp(stage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn international-experience flags into multiple rows per student:

        stud_id (char(9))
        term (char(4))
        intl_exp_type (varchar(255))
        load_date (datetime, UTC)
    """
    if stage_df.empty:
        logger.warning("Stage DF is empty in transform_intl_exp.")
        return stage_df

    df = stage_df.copy()

    # Normalize stud_id and term
    df["stud_id"] = (
        df["stud_id"].astype(str).str.strip().str.zfill(9)
    )
    df["term"] = (
        df["term"].astype(str).str.strip().str.zfill(4)
    )

    records = []
    now_utc = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        stud_id = row["stud_id"]
        term = row["term"]

        # iterate over each mapped flag
        for col, meta in INTL_EXP_MAP.items():
            if col not in row.index:
                # defensive: if column missing in extract for some reason, skip
                continue

            flag_val = row[col]
            if not _is_selected(flag_val):
                continue

            # Base label
            label = meta.get("label", col)

            # Prefer definition text if present for this row
            def_col = meta.get("definition_col")
            if def_col and def_col in row.index:
                def_val = row[def_col]
                if isinstance(def_val, str) and def_val.strip():
                    label = def_val.strip()

            # Handle "other" case with free-text
            other_col = meta.get("other_text_col")
            if other_col and other_col in row.index:
                other_txt = row[other_col]
                if isinstance(other_txt, str) and other_txt.strip():
                    label = f"{label}: {other_txt.strip()}"

            records.append(
                {
                    "stud_id": stud_id,
                    "term": term,
                    "intl_exp_type": label,
                    "load_date": now_utc,
                }
            )

    out_df = pd.DataFrame.from_records(records)
    logger.info("Transformed to %s intl_exp rows", len(out_df))
    return out_df


# ----------------------------------------------------------------------
# LOAD
# ----------------------------------------------------------------------

def load_intl_exp(engine: Engine, df: pd.DataFrame, truncate_before: bool) -> None:
    """
    Load rows into ugs.tbl_ug_survey_intl_exp.
    Does NOT insert 'id' (identity); only stud_id, term, intl_exp_type, load_date.
    """
    if df.empty:
        logger.info("No rows to insert into ugs.tbl_ug_survey_intl_exp.")
        return

    cols = ["stud_id", "term", "intl_exp_type", "load_date"]
    df = df[cols].where(pd.notnull(df), None)

    insert_sql = """
        INSERT INTO ugs.tbl_ug_survey_intl_exp (
            stud_id,
            term,
            intl_exp_type,
            load_date
        )
        VALUES (?, ?, ?, ?)
    """

    # Optional truncate first
    with engine.begin() as conn:
        if truncate_before:
            logger.info("Truncating ugs.tbl_ug_survey_intl_exp ...")
            conn.execute(text("TRUNCATE TABLE ugs.tbl_ug_survey_intl_exp;"))

    # Raw connection for fast executemany
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass

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
        "Inserted %s rows into ugs.tbl_ug_survey_intl_exp.", len(df)
    )


# ----------------------------------------------------------------------
# DRIVER
# ----------------------------------------------------------------------

def build_tbl_ug_survey_intl_exp(
    truncate_before_load: bool = True,
    dry_run: bool = False,
) -> None:
    logger.info(
        "[%s] Starting ETL for ugs.tbl_ug_survey_intl_exp",
        datetime.now(timezone.utc),
    )
    engine = get_engine()
    stage_df = extract_stage(engine)
    if stage_df.empty:
        logger.warning("Stage returned 0 rows; nothing to do.")
        return

    out_df = transform_intl_exp(stage_df)
    if out_df.empty:
        logger.warning("Transform produced 0 rows; nothing to insert.")
        return

    logger.info("Prepared %s rows for load", len(out_df))

    if dry_run:
        logger.info("Dry run enabled; NOT loading data into ugs.tbl_ug_survey_intl_exp")
        return

    load_intl_exp(engine, out_df, truncate_before_load)

    logger.info(
        "[%s] ETL for ugs.tbl_ug_survey_intl_exp completed",
        datetime.now(timezone.utc),
    )


if __name__ == "__main__":
    # Example CLI:
    #   $env:MSSQL_URL="mssql+pyodbc://@s8-whse-sql-d01/ugsurvey?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes"
    #   python -m ug_survey.etl_intl_exp
    build_tbl_ug_survey_intl_exp(truncate_before_load=True, dry_run=False)
