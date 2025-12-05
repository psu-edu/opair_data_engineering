"""
ETL for ugs.tbl_ug_survey_acadyear_labels

This module treats ugs.tbl_ug_survey_acadyear_labels as a small
dimension table and acts as a "dimension maintainer":

- Reads distinct term values from whsestage.dbo.tbl_stage_UG_Survey
- Compares them to existing terms in ugs.tbl_ug_survey_acadyear_labels
- For any *new* term, derives:
    * acad_year
    * report_label
  and inserts a single row per term into the labels table.

Important:
- It NEVER truncates ugs.tbl_ug_survey_acadyear_labels.
- It ONLY inserts rows for terms that are not already present.
- Existing rows (like your canonical 2135–2241 mapping) remain unchanged.

Term -> acad_year logic:
- Term codes look like: yyyT, e.g. 2235, 2238, 2241
    * yyy is a 3-digit year code (e.g. 223)
    * T is term digit: 1 = spring, 5 = summer, 8 = fall
- For summer/fall (T != 1):
    acad_year = 2000 + (yyy - 200)
- For spring (T == 1):
    acad_year = 2000 + (yyy - 200 - 1)

This reproduces your mapping:
    2135, 2138, 2141 -> 2013
    2145, 2148, 2151 -> 2014
    ...
    2235, 2238, 2241 -> 2023
"""

import os
import sys
import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER_NAME = "UG_Survey"


# ----------------------- logging / engine helpers -----------------------


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


def get_engine() -> Engine:
    url = os.getenv("MSSQL_URL")
    if not url:
        raise RuntimeError("MSSQL_URL environment variable is not set")
    logger.info("Using MSSQL_URL=%s", url)
    return create_engine(url, fast_executemany=True)


# ----------------------------- EXTRACT ---------------------------------


def extract_terms(engine: Engine) -> pd.DataFrame:
    """
    Get distinct term values from the stage table.
    """
    sql = """
        SELECT DISTINCT term
        FROM whsestage.dbo.tbl_stage_UG_Survey
        WHERE term IS NOT NULL
          AND LTRIM(RTRIM(term)) <> '';
    """
    logger.info(
        "Extracting distinct terms from whsestage.dbo.tbl_stage_UG_Survey ..."
    )
    df = pd.read_sql(sql, engine)
    logger.info("Found %s distinct terms in stage", len(df))
    return df


# ---------------------------- TRANSFORM --------------------------------


def _derive_acad_year(term_int: int) -> int:
    """
    Given a numeric term like 2235, derive the acad_year according
    to the pattern described in the module docstring.
    """
    year_code = term_int // 10  # e.g. 223 from 2235
    term_digit = term_int % 10  # e.g. 5

    # Base offset from 2000
    base = year_code - 200

    # Spring term belongs to prior acad_year
    if term_digit == 1:
        base -= 1

    acad_year = 2000 + base
    return acad_year


def transform_term_labels(term_df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn a list of term values into acad_year/report_label rows.

    Input:
        term_df: DataFrame with a column 'term' (string or numeric).

    Output columns:
        term         (int)
        acad_year    (int)
        report_label (str)
    """
    records = []

    for raw in term_df["term"]:
        if pd.isna(raw):
            continue

        s = str(raw).strip()
        if not s:
            continue

        try:
            term_int = int(s)
        except ValueError:
            logger.warning("Skipping non-numeric term value '%s'", s)
            continue

        acad_year = _derive_acad_year(term_int)
        report_label = str(acad_year)

        records.append(
            {
                "term": term_int,
                "acad_year": acad_year,
                "report_label": report_label,
            }
        )

    out_df = pd.DataFrame.from_records(records)
    # just in case: one row per term
    if not out_df.empty:
        out_df = out_df.drop_duplicates(subset=["term"])

    logger.info(
        "Prepared %s term -> acad_year_label rows", len(out_df)
    )
    return out_df


# ------------------------------ LOAD -----------------------------------


def load_acadyear_labels(engine: Engine, df: pd.DataFrame) -> None:
    """
    Insert *only new* term rows into ugs.tbl_ug_survey_acadyear_labels.

    Assumes df already contains only terms not present in the table.
    """
    if df.empty:
        logger.info(
            "No new terms to insert into ugs.tbl_ug_survey_acadyear_labels."
        )
        return

    cols = ["term", "acad_year", "report_label"]
    df = df[cols].where(pd.notnull(df), None)

    insert_sql = """
        INSERT INTO ugs.tbl_ug_survey_acadyear_labels (
            term,
            acad_year,
            report_label
        )
        VALUES (?, ?, ?)
    """

    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            # not fatal
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
        "Inserted %s new rows into ugs.tbl_ug_survey_acadyear_labels.", len(df)
    )


# ----------------------------- DRIVER ----------------------------------


def build_tbl_ug_survey_acadyear_labels(dry_run: bool = False) -> None:
    """
    Main entry point.

    - Reads distinct terms from stage
    - Reads existing terms from labels table
    - Identifies new terms
    - Derives acad_year/report_label for those terms
    - Inserts only the new rows

    It does NOT truncate or modify existing rows.
    """
    logger.info(
        "[%s] Starting ETL for ugs.tbl_ug_survey_acadyear_labels",
        datetime.now(timezone.utc),
    )
    engine = get_engine()

    # 1) Distinct terms from stage
    term_df = extract_terms(engine)
    if term_df.empty:
        logger.warning("No terms found in stage; nothing to do.")
        return

    # Normalize stage terms to trimmed string form
    term_df = term_df.copy()
    term_df["term_str"] = term_df["term"].astype(str).str.strip()

    stage_terms = {t for t in term_df["term_str"] if t}

    # 2) Existing terms in labels table
    with engine.begin() as conn:
        try:
            existing_rows = conn.execute(
                text("SELECT DISTINCT term FROM ugs.tbl_ug_survey_acadyear_labels;")
            ).fetchall()
        except Exception as ex:
            # If table is missing, treat as empty and let this script populate it
            logger.warning(
                "Error reading existing terms from ugs.tbl_ug_survey_acadyear_labels: %s",
                ex,
            )
            existing_rows = []

    existing_terms = {str(r[0]).strip() for r in existing_rows if r[0] is not None}

    # 3) Determine which terms are new
    new_terms = sorted(stage_terms - existing_terms)

    if not new_terms:
        logger.info(
            "All stage terms already exist in ugs.tbl_ug_survey_acadyear_labels; nothing to insert."
        )
        return

    logger.info(
        "Found %s new terms to add: %s",
        len(new_terms),
        ", ".join(new_terms),
    )

    # 4) Filter the term_df down to only those new terms
    new_term_df = term_df[term_df["term_str"].isin(new_terms)].copy()
    # Use the normalized 'term_str' as the source for numeric conversion
    new_term_df["term"] = new_term_df["term_str"]

    # 5) Transform these new terms to acad_year/report_label
    out_df = transform_term_labels(new_term_df)
    logger.info("Prepared %s rows for load", len(out_df))

    if out_df.empty:
        logger.warning(
            "Transform produced 0 rows for new terms; nothing to insert."
        )
        return

    if dry_run:
        logger.info(
            "Dry run enabled; NOT inserting into ugs.tbl_ug_survey_acadyear_labels."
        )
        return

    # 6) Insert only the new terms
    load_acadyear_labels(engine, out_df)

    logger.info(
        "[%s] ETL for ugs.tbl_ug_survey_acadyear_labels completed",
        datetime.now(timezone.utc),
    )


if __name__ == "__main__":
    # simple CLI:
    #   $env:MSSQL_URL = "..."
    #   python -m ug_survey.etl_acadyear_labels
    build_tbl_ug_survey_acadyear_labels(dry_run=False)
