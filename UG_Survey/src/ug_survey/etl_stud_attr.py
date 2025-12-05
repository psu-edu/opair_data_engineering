"""
ETL: whsestage.dbo.tbl_stage_UG_Survey -> ugsurvey.ugs.tbl_ug_survey_stud_attr

- Reads from stage table in whsestage
- Transforms to match tbl_ug_survey_stud_attr
- Loads into existing table (optionally truncating first)
"""

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# --------------------------------------------------------------------
#  Connection
# --------------------------------------------------------------------
# IMPORTANT: MSSQL_URL should point to the *ugsurvey* database.
# Example:
#   mssql+pyodbc://@s8-whse-sql-d01/ugsurvey?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes
MSSQL_URL = os.environ["MSSQL_URL"]
engine = create_engine(MSSQL_URL, fast_executemany=True)


# --------------------------------------------------------------------
#  Helper functions
# --------------------------------------------------------------------
def normalize_stud_id(value) -> str | None:
    """
    Normalize student_id to CHAR(9):
    - strip whitespace
    - if > 9 chars, keep last 9
    - left-pad with zeros to length 9
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None
    if len(s) > 9:
        s = s[-9:]
    return s.zfill(9)


def normalize_term(value) -> str | None:
    """
    Return 4-digit term as string if valid, else None.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    t = str(value).strip()
    if len(t) == 4 and t.isdigit():
        return t
    return None


def derive_invalid_term_indc(raw_term) -> str:
    """
    'N' if term is valid 4-digit integer, else 'Y'.
    """
    return "N" if normalize_term(raw_term) is not None else "Y"


# --------------------------------------------------------------------
#  Core ETL
# --------------------------------------------------------------------
def build_tbl_ug_survey_stud_attr(
    truncate_before_load: bool = True,
    dry_run: bool = False,
) -> None:
    """
    Main ETL function.

    Parameters
    ----------
    truncate_before_load : bool
        If True, TRUNCATE ugs.tbl_ug_survey_stud_attr before inserting.
        If False, append to existing rows.
    dry_run : bool
        If True, do not write to the database; only show row counts / sample.
    """

    with engine.begin() as conn:
        print(f"[{datetime.now(timezone.utc)}] Starting ETL for ugs.tbl_ug_survey_stud_attr")

        # ----------------------------------------------------------------
        # 1) Extract minimal needed columns from stage
        # ----------------------------------------------------------------
        query = text(
            """
            SELECT
                  s.id
                , s.student_id
                , s.term
                , s.intern_count
                , s.collection_method
                , s.Have_PostGrad_Info

                , s.indicator_fulltime_employment
                , s.indicator_parttime_employment
                , s.indicator_further_education
                , s.indicator_fellowship
                , s.indicator_military
                , s.indicator_still_seeking
                , s.indicator_other_plans

                , s.indicator_any_employment
                , s.indicator_internship
                , s.indicator_international_experience
                , s.indicator_undergrad_research
                , s.indicator_post_intern_residency
                , s.indicator_entrepreneurship
                , s.indicator_stay_connected_PSU
                , s.connect_assist

                , s.int_exp_semAbroad
                , s.int_exp_AYEA
                , s.int_exp_embedded
                , s.int_exp_studorg
                , s.int_exp_other
                , s.int_exp_other_fill
                , s.int_exp_none

                , s.plans_cleaned_definition
                , s.plans_other

                , s.LOAD_UTC_DT
            FROM [whsestage].[dbo].[tbl_stage_UG_Survey] AS s
            """
        )

        df = pd.read_sql(query, conn)
        print(f"  - Extracted {len(df):,} rows from whsestage.dbo.tbl_stage_UG_Survey")

        if df.empty:
            print("  - No rows found. Exiting.")
            return

        # ----------------------------------------------------------------
        # 2) Transform to match ugs.tbl_ug_survey_stud_attr
        # ----------------------------------------------------------------

        # id -> bigint
        df["id_out"] = df["id"].astype("int64")

        # student id
        df["stud_id"] = df["student_id"].apply(normalize_stud_id)

        # term / invalid_term_indc
        df["term_norm"] = df["term"].apply(normalize_term)
        df["invalid_term_indc"] = df["term"].apply(derive_invalid_term_indc)

        # intern_numb
        df["intern_numb"] = pd.to_numeric(df["intern_count"], errors="coerce").astype("Int64")

        # collection method
        df["coltn_mthd_cd"] = pd.NA  # placeholder, can be mapped from a ref table later
        df["coltn_mthd"] = df["collection_method"].fillna("").str.slice(0, 50)

        # post-graduation status code: vectorized precedence logic
        conditions = [
            df["indicator_fulltime_employment"] == "Y",
            df["indicator_parttime_employment"] == "Y",
            df["indicator_further_education"] == "Y",
            df["indicator_fellowship"] == "Y",
            df["indicator_military"] == "Y",
            df["indicator_still_seeking"] == "Y",
            df["indicator_other_plans"] == "Y",
        ]
        choices = [1, 2, 3, 4, 5, 6, 9]

        df["post_graduation_stat_cd"] = np.select(
            conditions, choices, default=np.nan
        )
        df["post_graduation_stat_cd"] = df["post_graduation_stat_cd"].astype("Int64")

        df["post_graduation_stat"] = df["plans_cleaned_definition"]
        df["post_graduation_other"] = df["plans_other"]

        # load_date from LOAD_UTC_DT
        df["load_date"] = df["LOAD_UTC_DT"]

        # drop rows with no student_id (target NOT NULL)
        before = len(df)
        df = df[df["stud_id"].notna()]
        dropped = before - len(df)
        if dropped:
            print(f"  - Dropped {dropped:,} rows with NULL stud_id")

        # ----------------------------------------------------------------
        # 3) Build final DataFrame in target column order
        # ----------------------------------------------------------------
        df_out = pd.DataFrame(
            {
                "id": df["id_out"],
                "stud_id": df["stud_id"],
                "term": df["term_norm"].astype("string").where(df["term_norm"].notna(), None),
                "camp_cd": pd.NA,              # TODO: join from academic data
                "majr_cd": pd.NA,              # TODO
                "majr_degr_cd": pd.NA,         # TODO
                "reporting_coll_cd": pd.NA,    # TODO
                "dept_cd": pd.NA,              # TODO
                "intern_numb": df["intern_numb"],
                "coltn_mthd_cd": df["coltn_mthd_cd"],
                "coltn_mthd": df["coltn_mthd"],
                "indc_have_information": df["Have_PostGrad_Info"],
                "post_graduation_stat_cd": df["post_graduation_stat_cd"],
                "post_graduation_stat": df["post_graduation_stat"],
                "post_graduation_other": df["post_graduation_other"],
                "indc_intern": df["indicator_internship"],
                "indc_intl_exp": df["indicator_international_experience"],
                "indc_ug_rsrch": df["indicator_undergrad_research"],
                "indc_job_asst": df["connect_assist"],
                "indc_conn_to_psu": df["indicator_stay_connected_PSU"],
                "indc_ft_empl": df["indicator_fulltime_employment"],
                "indc_pt_empl": df["indicator_parttime_employment"],
                "indc_any_empl": df["indicator_any_employment"],
                "indc_fut_edu": df["indicator_further_education"],
                "indc_publ_srvc": pd.NA,  # no clear source yet
                "indc_fell": df["indicator_fellowship"],
                "indc_post_intern_resid": df["indicator_post_intern_residency"],
                "indc_still_seeking": df["indicator_still_seeking"],
                "indc_mil": df["indicator_military"],
                "indc_entre": df["indicator_entrepreneurship"],
                "indc_other_plans": df["indicator_other_plans"],
                "indc_exp_semAbroad": df["int_exp_semAbroad"],
                "indc_exp_AYEA": df["int_exp_AYEA"],
                "indc_exp_embedded": df["int_exp_embedded"],
                "indc_exp_studorg": df["int_exp_studorg"],
                "indc_exp_other": df["int_exp_other"],
                "exp_other_fill": df["int_exp_other_fill"],
                "indc_exp_none": df["int_exp_none"],
                "invalid_term_indc": df["invalid_term_indc"],
                "load_date": df["load_date"],
            }
        )

        print(f"  - Transformed to {len(df_out):,} output rows")

        if dry_run:
            print("  - Dry run: showing sample and not writing to DB")
            print(df_out.head())
            return

        # ----------------------------------------------------------------
        # 4) Load into target table
        # ---------------------------------------------------------------->
        if truncate_before_load:
            print("  - Truncating ugs.tbl_ug_survey_stud_attr ...")
            conn.execute(text("TRUNCATE TABLE [ugs].[tbl_ug_survey_stud_attr];"))

        print(f"  - Inserting {len(df_out):,} rows into ugs.tbl_ug_survey_stud_attr ...")
        if "id" in df_out.columns:
            df_out = df_out.drop(columns=["id"])
        df_out.to_sql(
            name="tbl_ug_survey_stud_attr",
            schema="ugs",
            con=conn,
            if_exists="append",
            index=False,
        )

        print(f"[{datetime.now(timezone.utc)}] ETL for ugs.tbl_ug_survey_stud_attr completed")


# --------------------------------------------------------------------
#  CLI entry
# --------------------------------------------------------------------
if __name__ == "__main__":
    # First run: truncate and load fresh
    build_tbl_ug_survey_stud_attr(truncate_before_load=True, dry_run=False)
