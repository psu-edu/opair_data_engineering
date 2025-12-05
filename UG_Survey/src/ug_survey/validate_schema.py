import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

from ug_survey.config import load_settings
from ug_survey.db import get_engine
from ug_survey.logging_setup import setup_logging

LOGGER = logging.getLogger("UG_Survey_Schema")

# These are the "must-have" columns used in your SQL inserts and ETLs.
# Start with the ones that have already caused errors + clearly critical fields,
# and you can expand this list over time.
CRITICAL_STAGE_COLUMNS = [
    "student_id",
    "term",
    "PS_committment",
    "ps_committment_definition",
    "PS_committment_other",
    "FS_program",
    "FS_city",
    "FS_state",
    "FS_country",
    "SFT_email",
    "FE_type",
    "FE_type_definition",
    "FE_college",
    "FE_stage",
    "FE_stage_definition",
    "FE_offers",
    "FE_program",
    "FE_college_code",
    "EMP_Month",
    "EMP_Year",
    "offers_count",
    "EMP_company",
    "EMP_department",
    "EMP_title",
    "EMP_relate",
    "EMP_country",
    "EMP_state",
    "EMP_city",
    "EMP_salary",
    "EMP_bonus",
    "EMP_bonus_amount",
    "EMP_relocate",
    "EMP_relocate_amount",
    "EMP_functional",
    "EMP_functional_definition",
    "EMP_emp_edu",
    "EMP_emp_edu_manner",
    "EMP_emp_edu_manner_definition",
    "connect_email",
    "connect_cell",

    # intl_exp / stud_attr flags that caused issues:
    "int_exp_semAbroad",
    "int_exp_semAbroad_definition",
    "int_exp_AYEA",
    "int_exp_AYEA_definition",
    "int_exp_embedded",
    "int_exp_embedded_definition",
    "int_exp_studorg",
    "int_exp_studorg_definition",
    "int_exp_other",
    "int_exp_other_fill",
    "int_exp_none",
    "int_exp_long_via_psu",
    "int_exp_long_via_psu_defin",
    "int_exp_short_psu_course",
    "int_exp_short_psu_course_defin",
    "int_exp_short_psu_club_org",
    "int_exp_short_psu_club_org_def",
    "int_exp_educ_prog_not_psu",
    "int_exp_educ_prog_not_psu_defin",
    "int_exp_internship",
    "int_exp_internship_definition",
    "int_exp_did_not_have",
    "int_exp_did_not_have_defin",

    # general stud_attr fields:
    "intern_count",
    "collection_method",
    "Have_PostGrad_Info",
    "Plans_Cleaned",
    "plans_cleaned_definition",
    "plans_other",
    "indicator_internship",
    "indicator_international_experience",
    "indicator_undergrad_research",
    "SFT_assist",
    "indicator_stay_connected_PSU",
    "indicator_fulltime_employment",
    "indicator_parttime_employment",
    "indicator_any_employment",
    "indicator_further_education",
    "indicator_ps_committment",
    "indicator_fellowship",
    "indicator_post_intern_residency",
    "indicator_still_seeking",
    "indicator_military",
    "indicator_entrepreneurship",
    "indicator_other_plans",
]

def get_table_columns(engine, table_name: str, schema: str = "dbo") -> set[str]:
    sql = text("""
        SELECT c.name
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = :table_name
          AND s.name = :schema_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"table_name": table_name, "schema_name": schema})
        return {r[0] for r in rows}


def validate_stage_ug_survey_schema(engine, logger: logging.Logger) -> bool:
    logger.info("Validating schema for dbo.tbl_stage_UG_Survey ...")
    cols = get_table_columns(engine, "tbl_stage_UG_Survey", schema="dbo")
    missing = [c for c in CRITICAL_STAGE_COLUMNS if c.lower() not in {x.lower() for x in cols}]

    if not missing:
        logger.info("Schema OK: all %d critical columns found.", len(CRITICAL_STAGE_COLUMNS))
        return True

    logger.error("Schema INVALID: %d missing critical columns:", len(missing))
    for c in missing:
        logger.error("  - %s", c)

    return False


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate UG Survey stage table schema.")
    p.add_argument(
        "--settings",
        default="config/settings.yaml",
        help="Path to settings YAML (default: config/settings.yaml)",
    )
    p.add_argument(
        "--log-dir",
        default="logs",
        help="Directory for log files (default: logs)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    settings = load_settings(args.settings)
    log_dir = settings["app"].get("log_dir", args.log_dir) or args.log_dir

    logger = setup_logging("UG_Survey_Schema", log_dir, "INFO")
    logger.info("Starting schema validation using %s", args.settings)

    engine = get_engine(settings)

    ok = validate_stage_ug_survey_schema(engine, logger)
    if ok:
        logger.info("Schema validation PASSED.")
        return 0
    else:
        logger.error("Schema validation FAILED. See log for details.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
