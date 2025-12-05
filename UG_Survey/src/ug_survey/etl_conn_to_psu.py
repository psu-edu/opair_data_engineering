"""
ETL for: ugs.tbl_ug_survey_conn_to_psu
Source: whsestage.dbo.tbl_stage_UG_Survey
Target schema matches exact DDL provided by user.
"""

import os
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# ---------------------------------------------------------
# Connection
# ---------------------------------------------------------
MSSQL_URL = os.environ["MSSQL_URL"]
engine = create_engine(MSSQL_URL, fast_executemany=True)


# ---------------------------------------------------------
# Helpers (same as stud_attr)
# ---------------------------------------------------------
def normalize_stud_id(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None
    if len(s) > 9:
        s = s[-9:]
    return s.zfill(9)


def normalize_term(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    t = str(value).strip()
    if len(t) == 4 and t.isdigit():
        return t
    return None


# ---------------------------------------------------------
# Main ETL
# ---------------------------------------------------------
def build_tbl_ug_survey_conn_to_psu(dry_run=False):

    with engine.begin() as conn:
        print(f"[{datetime.now(timezone.utc)}] Starting ETL for tbl_ug_survey_conn_to_psu")

        # 1. Extract
        query = text(
            """
            SELECT
                  id
                , student_id
                , term

                , connect_mentor
                , connect_emp_panel
                , connect_network
                , connect_assist
                , connect_ldship
                , connect_no
                , connect_email
                , connect_cell_phone
                , indicator_stay_connected_PSU

                , LOAD_UTC_DT
            FROM whsestage.dbo.tbl_stage_UG_Survey
            """
        )

        df = pd.read_sql(query, conn)
        print(f"  - Extracted {len(df):,} rows")

        # 2. Transform
        df["stud_id"] = df["student_id"].apply(normalize_stud_id)
        df["term_norm"] = df["term"].apply(normalize_term)
        df["load_date"] = df["LOAD_UTC_DT"]

        # Build "conn_to_psu" combined column
        connection_labels = {
            "connect_mentor": "Mentor",
            "connect_emp_panel": "Employer Panel",
            "connect_network": "Networking",
            "connect_assist": "Job Assistance",
            "connect_ldship": "Leadership",
            "connect_no": "No",
            "connect_email": "Email",
            "connect_cell_phone": "Cell Phone",
            "indicator_stay_connected_PSU": "Stay Connected to PSU"
        }

        def combine_connections(row):
            picked = []
            for col, label in connection_labels.items():
                val = row.get(col)
                # accept Y, Yes, 1, True, or non-empty text
                if (
                    isinstance(val, str) and val.strip() not in ("", "N", "No")
                ) or (
                    val in ("Y", 1, True)
                ):
                    picked.append(label)
            return ", ".join(picked) if picked else None

        df["conn_to_psu"] = df.apply(combine_connections, axis=1)

        # Drop null student IDs
        df = df[df["stud_id"].notna()]

        # 3. Final DataFrame matching target table columns (NO id)
        df_out = pd.DataFrame({
            "stud_id": df["stud_id"],
            "term": df["term_norm"],
            "conn_to_psu": df["conn_to_psu"].astype("string"),
            "load_date": df["load_date"],
        })

        # Drop rows with completely empty connection info if you want
        # (optional – you can comment this out)
        # df_out = df_out[df_out["conn_to_psu"].notna()]

        print(f"  - Final output rows: {len(df_out):,}")
        print("  - Output columns:", list(df_out.columns))

        if dry_run:
            print("  - Dry run sample:")
            print(df_out.head())
            return

        # Extra safety: ensure absolutely no 'id' column survives
        if "id" in df_out.columns:
            df_out = df_out.drop(columns=["id"])

        # 4. Load
        print("  - Truncating ugs.tbl_ug_survey_conn_to_psu ...")
        conn.execute(text("TRUNCATE TABLE ugs.tbl_ug_survey_conn_to_psu;"))

        print("  - Inserting rows ...")
        # force the exact column order we want
        df_out = df_out[["stud_id", "term", "conn_to_psu", "load_date"]]

        df_out.to_sql(
            "tbl_ug_survey_conn_to_psu",
            schema="ugs",
            con=conn,
            if_exists="append",
            index=False,
        )

        print(f"[{datetime.now(timezone.utc)}] Completed ETL for tbl_ug_survey_conn_to_psu")


if __name__ == "__main__":
    build_tbl_ug_survey_conn_to_psu(dry_run=False)
