import argparse
import logging

from ug_survey.config import load_settings
from ug_survey.db import get_engine
from ug_survey.logging_setup import setup_logging

LOGGER = logging.getLogger("UG_Survey_Reference")


def load_reference_tables(engine, logger: logging.Logger, settings: dict) -> None:
    """
    Placeholder for reference table loading.

    Currently, reference.source = "db", meaning we rely on existing
    lp_reference tables and do not load from CSV.
    """
    ref_cfg = settings.get("reference", {})
    source = ref_cfg.get("source", "db")

    if source == "db":
        logger.info(
            "Reference source is 'db'; using existing lp_reference tables "
            "and ugs.tbl_ug_survey_nsc_ipeds_codes. No CSV load performed."
        )
        # Here you could add optional health checks (row counts, etc.) if desired.
        return

    logger.warning(
        "Reference source '%s' is not implemented yet. No reference tables loaded.",
        source,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load reference tables for UG Survey ETL.")
    p.add_argument(
        "--settings",
        default="config/settings.yaml",
        help="Path to settings YAML (default: config/settings.yaml)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Load settings first
    settings = load_settings(args.settings)

    app_cfg = settings.get("app", {})
    log_dir = app_cfg.get("log_dir", "logs")
    log_level = app_cfg.get("log_level", "INFO")

    logger = setup_logging("UG_Survey_Reference", log_dir, log_level)
    logger.info("Starting reference load using settings %s", args.settings)

    try:
        engine = get_engine(settings)
    except Exception as ex:
        logger.error("Failed to create database engine", exc_info=ex)
        return 1

    try:
        load_reference_tables(engine, logger, settings)
        logger.info("Reference load complete.")
        return 0
    except Exception as ex:
        logger.error("Reference load failed", exc_info=ex)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
