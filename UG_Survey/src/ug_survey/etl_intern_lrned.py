"""
ETL for ugs.tbl_ug_survey_intern_lrned

- Reads intern*_how_obtain_* columns from whsestage.dbo.tbl_stage_UG_Survey
- Unpivots each selected option into rows
- Loads into ugs.tbl_ug_survey_intern_lrned

Target table:
    ugs.tbl_ug_survey_intern_lrned (
        id BIGINT IDENTITY(1,1),
        stud_id CHAR(9),
        term CHAR(4),
        intern_nbr INT,
        intern_lrned_abt_cd INT,
        intern_lrned_abt VARCHAR(300),
        load_date DATETIME
    )
"""
import os
import sys
import logging
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

LOGGER_NAME = "UG_Survey"

# ---------------------------------------------------------------------
# Mapping: code -> generic label
# (replace with survey text later if you want)
# ---------------------------------------------------------------------
INTERNSHIP_HOW_OBTAIN_LABELS: Dict[int, str] = {
    1: "Internship obtained via option 01",
    2: "Internship obtained via option 02",
    3: "Internship obtained via option 03",
    4: "Internship obtained via option 04",
    5: "Internship obtained via option 05",
    6: "Internship obtained via option 06",
    7: "Internship obtained via option 07",
    8: "Internship obtained via option 08",
    9: "Internship obtained via option 09",
    10: "Internship obtained via option 10",
    11: "Internship obtained via option 11",
    12: "Internship obtained via option 12",
    13: "Internship obtained via option 13",
    14: "Internship obtained via option 14",
    99: "Internship obtained via Other",
}

# ---------------------------------------------------------------------
# Logging / engine helpers
# ---------------------------------------------------------------------


def setup_logging() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        )
        logger.addHandler(handler)
    return logger


logger = setup_logging()


def get_engine() -> Engine:
    url = os.getenv("MSSQL_URL")
    if not url:
        raise RuntimeError("MSSQL_URL environment variable is not set")
    logger.info("Using MSSQL_URL=%s", url)
    return create_engine(url, fast_executemany=True)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _is_selected(val) -> bool:
    """
    Decide if a checkbox / flag column is "selected".

    Treat as NOT selected if:
      - None / NaN
      - empty string
      - '0', 'N', 'NO', 'None', etc.
    Everything else is treated as selected.
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
    # numeric / boolean
    try:
        return bool(val)
    except Exception:
        return False


# ---------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------


def extract_stage(engine: Engine) -> pd.DataFrame:
    """
    Pull internship-how-obtained columns from stage table.

    Uses the actual column names from whsestage.dbo.tbl_stage_UG_Survey.
    """
    sql = """
        SELECT
            student_id                      AS stud_id,
            term,
            intern_count,

            -- How obtained for intern #1
            intern1_how_obtain_01,
            intern1_how_obtain_02,
            intern1_how_obtain_03,
            intern1_how_obtain_04,
            intern1_how_obtain_05,
            intern1_how_obtain_06,
            intern1_how_obtain_07,
            intern1_how_obtain_08,
            intern1_how_obtain_09,
            intern1_how_obtain_10,
            intern1_how_obtain_11,
            intern1_how_obtain_12,
            intern1_how_obtain_13,
            intern1_how_obtain_14,
            intern1_how_obtain_other,
            intern1_how_obtain_other_fill,

            -- How obtained for intern #2
            intern2_how_obtain_01,
            intern2_how_obtain_02,
            intern2_how_obtain_03,
            intern2_how_obtain_04,
            intern2_how_obtain_05,
            intern2_how_obtain_06,
            intern2_how_obtain_07,
            intern2_how_obtain_08,
            intern2_how_obtain_09,
            intern2_how_obtain_10,
            intern2_how_obtain_11,
            intern2_how_obtain_12,
            intern2_how_obtain_13,
            intern2_how_obtain_14,
            intern2_how_obtain_other,
            intern2_how_obtain_other_fill,

            -- How obtained for intern #3
            intern3_how_obtain_01,
            intern3_how_obtain_02,
            intern3_how_obtain_03,
            intern3_how_obtain_04,
            intern3_how_obtain_05,
            intern3_how_obtain_06,
            intern3_how_obtain_07,
            intern3_how_obtain_08,
            intern3_how_obtain_09,
            intern3_how_obtain_10,
            intern3_how_obtain_11,
            intern3_how_obtain_12,
            intern3_how_obtain_13,
            intern3_how_obtain_14,
            intern3_how_obtain_other,
            intern3_how_obtain_other_fill
        FROM whsestage.dbo.tbl_stage_UG_Survey
        WHERE intern_count IS NOT NULL
          AND NULLIF(intern_count, '') <> ''
    """

    logger.info("Extracting intern*_how_obtain_* from tbl_stage_UG_Survey ...")
    df = pd.read_sql(sql, engine)
    logger.info("Stage extract rows: %s", len(df))
    return df


# ---------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------


def _build_column_list_for_intern(n: int) -> Dict[str, List[str]]:
    """
    For intern #n (1,2,3), return the list of flag columns and the
    "other" + "other_fill" columns.
    """
    flag_cols = [f"intern{n}_how_obtain_{i:02d}" for i in range(1, 15)]
    other_col = f"intern{n}_how_obtain_other"
    other_fill_col = f"intern{n}_how_obtain_other_fill"
    return {"flags": flag_cols, "other": other_col, "other_fill": other_fill_col}


def transform_intern_lrned(stage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Turn intern*_how_obtain_* flags into multiple rows per student + internship.

    Output columns:
        stud_id             (char(9))
        term                (char(4))
        intern_nbr          (1,2,3)
        intern_lrned_abt_cd (int)
        intern_lrned_abt    (varchar(300))
        load_date           (datetime)
    """
    if stage_df.empty:
        logger.warning("Stage DataFrame is empty in transform_intern_lrned.")
        return stage_df

    df = stage_df.copy()

    # Normalize IDs
    df["stud_id"] = df["stud_id"].astype(str).str.strip().str.zfill(9)
    df["term"] = df["term"].astype(str).str.strip().str.zfill(4)

    # Normalize intern_count to int where possible
    def _to_int(x):
        if x is None:
            return None
        if isinstance(x, float) and pd.isna(x):
            return None
        s = str(x).strip()
        if s == "":
            return None
        try:
            return int(float(s))
        except Exception:
            return None

    df["intern_count_int"] = df["intern_count"].apply(_to_int)

    records = []
    now_utc = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        stud_id = row["stud_id"]
        term = row["term"]
        cnt = row["intern_count_int"]

        if cnt is None or cnt <= 0:
            continue

        # up to 3 internships
        for intern_nbr in (1, 2, 3):
            if cnt < intern_nbr:
                continue

            colinfo = _build_column_list_for_intern(intern_nbr)

            # handle regular flag options 1..14
            for idx, flag_col in enumerate(colinfo["flags"], start=1):
                if flag_col not in df.columns:
                    # should not happen given DDL, but be defensive
                    continue

                flag_val = row.get(flag_col)
                if not _is_selected(flag_val):
                    continue

                code = idx  # 1..14
                description = INTERNSHIP_HOW_OBTAIN_LABELS.get(
                    code, f"Internship obtained via option {idx:02d}"
                )

                records.append(
                    {
                        "stud_id": stud_id,
                        "term": term,
                        "intern_nbr": intern_nbr,
                        "intern_lrned_abt_cd": code,
                        "intern_lrned_abt": description,
                        "load_date": now_utc,
                    }
                )

            # handle "Other" flag + free-text
            other_flag_col = colinfo["other"]
            other_fill_col = colinfo["other_fill"]

            if other_flag_col in df.columns and _is_selected(row.get(other_flag_col)):
                code = 99
                description = INTERNSHIP_HOW_OBTAIN_LABELS.get(
                    code, "Internship obtained via Other"
                )
                other_text = row.get(other_fill_col)
                if isinstance(other_text, str) and other_text.strip():
                    description = f"{description}: {other_text.strip()}"

                records.append(
                    {
                        "stud_id": stud_id,
                        "term": term,
                        "intern_nbr": intern_nbr,
                        "intern_lrned_abt_cd": code,
                        "intern_lrned_abt": description,
                        "load_date": now_utc,
                    }
                )

    out_df = pd.DataFrame.from_records(records)
    # Safety net: ensure we don't send the identity column
    out_df = out_df.drop(columns=["id"], errors="ignore")
    logger.info("Transformed to %s intern_lrned rows", len(out_df))
    return out_df


# ---------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------


def load_intern_lrned(
    engine: Engine, df: pd.DataFrame, truncate_before: bool = True
) -> None:
    """
    Insert internship learned-about rows into ugs.tbl_ug_survey_intern_lrned.

    IMPORTANT:
    - Target table has IDENTITY column [id], so we do NOT insert into it.
    - SQL Server will auto-generate [id] values.
    """
    if df.empty:
        logger.info("No rows to insert into ugs.tbl_ug_survey_intern_lrned.")
        return

    # First, truncate if requested
    with engine.begin() as conn:
        if truncate_before:
            logger.info("Truncating ugs.tbl_ug_survey_intern_lrned ...")
            conn.execute(text("TRUNCATE TABLE ugs.tbl_ug_survey_intern_lrned;"))

    # We do NOT generate or insert id; let identity handle it
    df = df.copy()

    # Column order must match INSERT (no id)
    cols = [
        "stud_id",
        "term",
        "intern_nbr",
        "intern_lrned_abt_cd",
        "intern_lrned_abt",
        "load_date",
    ]

    df = df[cols].where(pd.notnull(df), None)

    insert_sql = """
        INSERT INTO ugs.tbl_ug_survey_intern_lrned (
            stud_id,
            term,
            intern_nbr,
            intern_lrned_abt_cd,
            intern_lrned_abt,
            load_date
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """

    # Use raw connection + executemany for speed
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
        "Inserted %s rows into ugs.tbl_ug_survey_intern_lrned.", len(df)
    )


# ---------------------------------------------------------------------
# DRIVER
# ---------------------------------------------------------------------


def build_tbl_ug_survey_intern_lrned(
    truncate_before_load: bool = True, dry_run: bool = False
) -> None:
    logger.info(
        "[%s] Starting ETL for ugs.tbl_ug_survey_intern_lrned",
        datetime.now(timezone.utc),
    )
    engine = get_engine()

    stage_df = extract_stage(engine)
    if stage_df.empty:
        logger.warning("Stage returned 0 rows; nothing to do.")
        return

    out_df = transform_intern_lrned(stage_df)
    if out_df.empty:
        logger.warning("Transform produced 0 rows; nothing to insert.")
        return

    logger.info("Prepared %s rows for load", len(out_df))

    if dry_run:
        logger.info("Dry run enabled; NOT loading data.")
        return

    load_intern_lrned(engine, out_df, truncate_before_load)

    logger.info(
        "[%s] ETL for ugs.tbl_ug_survey_intern_lrned completed",
        datetime.now(timezone.utc),
    )


if __name__ == "__main__":
    build_tbl_ug_survey_intern_lrned(truncate_before_load=True, dry_run=False)
