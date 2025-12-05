"""
ETL for ugs.tbl_ug_survey_intern

- Extracts internship-related columns from whsestage.dbo.tbl_stage_UG_Survey
- Unpivots up to 3 internships per student into separate rows
- Loads into ugs.tbl_ug_survey_intern
"""

import os
import sys
import logging
from datetime import datetime, timezone
import math
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER_NAME = "UG_Survey"


def setup_logging() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


logger = setup_logging()


def get_engine():
    url = os.getenv("MSSQL_URL")
    if not url:
        raise RuntimeError("MSSQL_URL environment variable is not set")
    logger.info(f"Using MSSQL_URL={url}")
    engine = create_engine(url, fast_executemany=True)
    return engine


def extract_stage(engine: Engine) -> pd.DataFrame:
    """
    Pull internship data from whsestage.dbo.tbl_stage_UG_Survey.

    Uses the *actual* column names from the stage table and aliases
    student_id -> stud_id so the rest of the ETL can keep using stud_id.
    """
    sql = """
        SELECT
            student_id              AS stud_id,
            term,
            CAST(NULLIF(intern_count, '') AS int) AS intern_count,

            -- Intern 1
            intern_organization1,
            intern_country1,
            intern_state1,
            intern_province1,
            intern_unit1,
            intern_title1,
            intern_exp1,
            intern_paid1,
            intern_paid_amt1,
            intern_college_credit1,
            intern_semesters1,

            -- Intern 2
            intern_organization2,
            intern_country2,
            intern_state2,
            intern_province2,
            intern_unit2,
            intern_title2,
            intern_exp2,
            intern_paid2,
            intern_paid_amt2,
            intern_college_credit2,
            intern_semesters2,

            -- Intern 3
            intern_organization3,
            intern_country3,
            intern_state3,
            intern_province3,
            intern_unit3,
            intern_title3,
            intern_exp3,
            intern_paid3,
            intern_paid_amt3,
            intern_college_credit3,
            intern_semesters3,

            -- Global "student org experience" flags
            int_exp_studorg,
            int_exp_studorg_definition
        FROM whsestage.dbo.tbl_stage_UG_Survey
        WHERE intern_count IS NOT NULL
          AND NULLIF(intern_count, '') <> '';
    """

    logger.info("Extracting internship data from whsestage.dbo.tbl_stage_UG_Survey")
    df = pd.read_sql(sql, engine)

    logger.info("Stage extract rows: %s", len(df))
    return df


def _normalize_yn(value):
    """
    Normalize various yes/no representations to 'Y'/'N' or None.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if isinstance(value, str):
        v = value.strip().upper()
        if v in ("Y", "YES", "TRUE", "T", "1"):
            return "Y"
        if v in ("N", "NO", "FALSE", "F", "0"):
            return "N"
        # fall back to first character if something like "Yes " etc
        if v:
            return v[0]
        return None

    # numeric / boolean
    try:
        if bool(value):
            return "Y"
        else:
            return "N"
    except Exception:
        return None


def transform_intern(stage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot the wide internship columns into multiple rows.

    Output columns:
        stud_id (char(9))
        term (char(4))
        intern_numb (1, 2, 3)
        intern_co_nm
        intern_dept
        intern_job_title
        intern_len_wks (NULL for now; no source column)
        indc_intern_paid (Y/N)
        intern_amt_paid (numeric)
        intern_country
        intern_state
        intern_province
        intern_college_credit (Y/N or similar, 1 char)
        intern_semesters (smallint)
    """
    records = []

    # ensure we treat stud_id / term as strings
    stage_df = stage_df.copy()
    stage_df["stud_id"] = (
        stage_df["stud_id"].astype(str).str.strip().str.zfill(9)
    )
    stage_df["term"] = (
        stage_df["term"].astype(str).str.strip().str.zfill(4)
    )

    for _, row in stage_df.iterrows():
        stud_id = row["stud_id"]
        term = row["term"]

        count = row.get("intern_count")
        if pd.isna(count):
            continue
        try:
            count = int(count)
        except Exception:
            # if non-numeric, skip
            continue
        if count <= 0:
            continue

        for i in (1, 2, 3):
            if count < i:
                continue

            org = row.get(f"intern{i}_organization")
            unit = row.get(f"intern{i}_unit")
            title = row.get(f"intern{i}_title")
            paid_raw = row.get(f"intern{i}_paid")
            amt = row.get(f"intern{i}_paid_amt")

            country = row.get(f"intern_country{i}")
            state = row.get(f"intern_state{i}")
            province = row.get(f"intern_province{i}")
            college_credit = row.get(f"intern_college_credit{i}")
            semesters = row.get(f"intern_semesters{i}")

            # skip completely empty internship slots
            key_fields = [
                org,
                title,
                unit,
                paid_raw,
                amt,
                country,
                state,
                province,
                college_credit,
                semesters,
            ]
            if all((pd.isna(x) if not isinstance(x, str) else x.strip() == "") for x in key_fields):
                continue

            indc_intern_paid = _normalize_yn(paid_raw)

            # normalize semesters to int
            sem_val = None
            if semesters is not None and not (isinstance(semesters, float) and pd.isna(semesters)):
                try:
                    sem_val = int(semesters)
                except Exception:
                    sem_val = None

            # normalize college_credit to 1-char (e.g. Y/N)
            cc_val = None
            if college_credit is not None and not (isinstance(college_credit, float) and pd.isna(college_credit)):
                if isinstance(college_credit, str):
                    cc_val = _normalize_yn(college_credit)
                else:
                    cc_val = _normalize_yn(college_credit)

            records.append(
                {
                    "stud_id": stud_id,
                    "term": term,
                    "intern_numb": i,
                    "intern_co_nm": org,
                    "intern_dept": unit,
                    "intern_job_title": title,
                    "intern_len_wks": None,  # no direct source column
                    "indc_intern_paid": indc_intern_paid,
                    "intern_amt_paid": amt,
                    "intern_country": country,
                    "intern_state": state,
                    "intern_province": province,
                    "intern_college_credit": cc_val,
                    "intern_semesters": sem_val,
                }
            )

    out_df = pd.DataFrame.from_records(records)

    if out_df.empty:
        logger.warning("No internship rows produced during transform_intern.")
        return out_df

    # coerce amount to numeric
    out_df["intern_amt_paid"] = pd.to_numeric(
        out_df["intern_amt_paid"], errors="coerce"
    )

    logger.info("Transformed to %s internship rows", len(out_df))
    return out_df
def load_internships(engine: Engine, out_df: pd.DataFrame, batch_size: int = 1000) -> None:
    """
    Insert internship rows into ugs.tbl_ug_survey_intern using batched
    executemany on a raw pyodbc cursor. This avoids pandas.to_sql and the
    huge parameter lists that were causing hangs.
    """
    row_count = len(out_df)
    if row_count == 0:
        logger.info("No internship rows to insert.")
        return

    logger.info(
        "Inserting %d rows into ugs.tbl_ug_survey_intern using batched executemany ...",
        row_count,
    )

    # Columns we insert (id is IDENTITY; we do NOT insert it)
    cols = [
        "stud_id",
        "term",
        "intern_numb",
        "intern_co_nm",
        "intern_dept",
        "intern_job_title",
        "intern_len_wks",
        "indc_intern_paid",
        "intern_amt_paid",
        "load_date",
        "intern_country",
        "intern_state",
        "intern_province",
        "intern_college_credit",
        "intern_semesters",
    ]

    # Ensure load_date exists
    if "load_date" not in out_df.columns:
        out_df = out_df.copy()
        # use naive datetime for SQL Server datetime column
        out_df["load_date"] = datetime.now()

    # Prepare data: replace NaN with None
    cleaned_rows = []
    for row in out_df[cols].itertuples(index=False, name=None):
        cleaned = []
        for v in row:
            if v is None:
                cleaned.append(None)
            elif isinstance(v, float) and math.isnan(v):
                cleaned.append(None)
            elif pd.isna(v):
                cleaned.append(None)
            else:
                cleaned.append(v)
        cleaned_rows.append(tuple(cleaned))

    insert_sql = """
        INSERT INTO ugs.tbl_ug_survey_intern (
            stud_id,
            term,
            intern_numb,
            intern_co_nm,
            intern_dept,
            intern_job_title,
            intern_len_wks,
            indc_intern_paid,
            intern_amt_paid,
            load_date,
            intern_country,
            intern_state,
            intern_province,
            intern_college_credit,
            intern_semesters
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        # enable fast_executemany where supported
        try:
            cursor.fast_executemany = True
        except Exception:
            pass

        for start in range(0, len(cleaned_rows), batch_size):
            chunk = cleaned_rows[start : start + batch_size]
            cursor.executemany(insert_sql, chunk)
            logger.info("Inserted %d rows into ugs.tbl_ug_survey_intern ...", len(chunk))

        conn.commit()
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()

    logger.info("Finished loading ugs.tbl_ug_survey_intern.")


def build_tbl_ug_survey_intern(
    truncate_before_load: bool = True, dry_run: bool = False
) -> None:
    logger.info(
        "[%s] Starting ETL for ugs.tbl_ug_survey_intern",
        datetime.now(timezone.utc),
    )
    engine = get_engine()

    # 1. Extract
    stage_df = extract_stage(engine)
    if stage_df.empty:
        logger.warning("Stage extract returned 0 rows; nothing to do.")
        return

    # 2. Transform
    out_df = transform_intern(stage_df)
    if out_df.empty:
        logger.warning("Transform produced 0 rows; nothing to insert.")
        return

    logger.info("Prepared %s rows for load", len(out_df))

    # 3. Dry run? -> stop before touching the table
    if dry_run:
        logger.info("Dry run enabled; NOT loading data into ugs.tbl_ug_survey_intern")
        return

    # 4. Truncate (optional)
    if truncate_before_load:
        logger.info("Truncating ugs.tbl_ug_survey_intern ...")
        with engine.begin() as conn:
            conn.exec_driver_sql("TRUNCATE TABLE ugs.tbl_ug_survey_intern;")

    # 5. Load
    load_internships(engine, out_df)

    logger.info(
        "[%s] ETL for ugs.tbl_ug_survey_intern completed",
        datetime.now(timezone.utc),
    )


if __name__ == "__main__":
    # simple CLI usage: python -m ug_survey.etl_intern
    build_tbl_ug_survey_intern(truncate_before_load=True, dry_run=False)
