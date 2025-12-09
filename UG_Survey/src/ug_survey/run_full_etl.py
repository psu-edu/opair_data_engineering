#!/usr/bin/env python
"""
Run a complete UG Survey ETL and send an email summary.

Examples:
    # Single file
    python -m ug_survey.run_full_etl --env DEV --mode test --raw-file "F:/Dat/UGSurvey/UGSurveyData_202324SP_11_5_25.csv"

    # Multiple files using glob pattern
    python -m ug_survey.run_full_etl --env DEV --mode test --raw-file "F:/Dat/UGSurvey/UGSurveyData_*.csv"
"""

import argparse
import logging
import os
import glob
from pathlib import Path
import smtplib
import subprocess
import sys
import traceback
from datetime import datetime
from email.message import EmailMessage
from typing import List, Dict, Any, Optional

from ug_survey.config import load_settings
from ug_survey.logging_setup import setup_logging as setup_app_logging
from ug_survey.file_utils import detect_survey_type, infer_term_from_filename

LOGGER = logging.getLogger("UG_Survey_FullETL")


# ----------------------------------------------------------------------
# Build ETL step list (so we can inject raw_file)
# ----------------------------------------------------------------------
def build_steps(raw_file: str) -> List[tuple[str, List[str]]]:
    """
    Build the ordered list of ETL steps.

    Each step is a tuple: (step_name, command_list)
    """
    steps: List[tuple[str, List[str]]] = [
        (
            "Validate stage schema",
            [sys.executable, "-m", "ug_survey.validate_schema"],
        ),
        (
            "Load reference tables",
            [sys.executable, "-m", "ug_survey.load_reference"],
        ),
        (
            "Load raw UG survey file -> tbl_stage_raw_survey",
            [
                sys.executable,
                "-m",
                "ug_survey.load_raw",
                "--file",
                raw_file,
            ],
        ),
        (
            "Transform stage_raw -> tbl_stage_UG_Survey",
            [sys.executable, "-m", "ug_survey.stage_to_ugs"],
        ),
        # --- Individual UGS ETLs ---
        (
            "ETL: ugs.tbl_ug_survey_response",
            [sys.executable, "-m", "ug_survey.etl_response"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_conn_to_psu",
            [sys.executable, "-m", "ug_survey.etl_conn_to_psu"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_acadyear_labels",
            [sys.executable, "-m", "ug_survey.etl_acadyear_labels"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_empl_job_lrned",
            [sys.executable, "-m", "ug_survey.etl_empl_job_lrned"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_intern",
            [sys.executable, "-m", "ug_survey.etl_intern"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_intern_lrned",
            [sys.executable, "-m", "ug_survey.etl_intern_lrned"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_intl_exp",
            [sys.executable, "-m", "ug_survey.etl_intl_exp"],
        ),
        (
            "ETL: ugs.tbl_ug_survey_stud_attr",
            [sys.executable, "-m", "ug_survey.etl_stud_attr"],
        ),
    ]
    return steps


# ----------------------------------------------------------------------
# Run a single step
# ----------------------------------------------------------------------
def run_step(name: str, cmd: List[str]) -> Dict[str, Any]:
    LOGGER.info("=== START STEP: %s ===", name)
    LOGGER.info("Command: %s", " ".join(cmd))

    start = datetime.now()
    result: Dict[str, Any] = {
        "name": name,
        "cmd": cmd,
        "start": start,
        "end": None,
        "returncode": None,
        "stdout": "",
        "stderr": "",
        "ok": False,
        "exception": None,
    }

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        result["end"] = datetime.now()
        result["returncode"] = proc.returncode
        result["stdout"] = proc.stdout
        result["stderr"] = proc.stderr
        result["ok"] = proc.returncode == 0

        if proc.stdout:
            LOGGER.info(proc.stdout)
        if proc.stderr:
            LOGGER.warning(proc.stderr)

        if result["ok"]:
            LOGGER.info("=== SUCCESS: %s ===", name)
        else:
            LOGGER.error("=== FAILED: %s (rc=%s) ===", name, proc.returncode)

    except Exception:
        result["end"] = datetime.now()
        result["exception"] = traceback.format_exc()
        LOGGER.exception("EXCEPTION while running step %s", name)

    return result


# ----------------------------------------------------------------------
# Email summary
# ----------------------------------------------------------------------
def send_email_summary(
    env: str,
    mode: str,
    steps: List[Dict[str, Any]],
    logfile: Path,
    start_ts: datetime,
    end_ts: datetime,
    settings: Optional[Dict[str, Any]] = None,
    raw_file: Optional[Path] = None,
) -> None:
    any_failed = any(not s["ok"] for s in steps)
    status = "FAILURE" if any_failed else "SUCCESS"

    email_cfg = (settings or {}).get("email", {})

    enabled = email_cfg.get("enabled", True)
    if not enabled:
        LOGGER.info("Email sending disabled by settings.")
        return

    # Settings + env override
    smtp_host = os.getenv("UGS_SMTP_SERVER", email_cfg.get("smtp_host", "smtp.psu.edu"))
    smtp_port = int(os.getenv("UGS_SMTP_PORT", str(email_cfg.get("smtp_port", 25))))

    from_addr = os.getenv("UGS_EMAIL_FROM", email_cfg.get("from_addr", "L-DWEMAIL@LISTS.PSU.EDU"))
    to_addrs_setting = os.getenv("UGS_EMAIL_TO", email_cfg.get("to_addrs", "L-DWEMAIL@LISTS.PSU.EDU"))

    subject_prefix = email_cfg.get("subject_prefix", "[UGS ETL]")

    to_list = [addr.strip() for addr in to_addrs_setting.split(",") if addr.strip()]

    file_info = f" file={raw_file.name}" if raw_file is not None else ""
    subject = f"{subject_prefix} {status} (env={env}, mode={mode}{file_info})"
    LOGGER.info("Preparing email to %s with subject: %s", to_list, subject)

    lines: List[str] = []
    lines.append(f"UG Survey ETL completed with status: {status}")
    lines.append("")
    lines.append(f"Environment : {env}")
    lines.append(f"Mode        : {mode}")
    if raw_file is not None:
        lines.append(f"Raw file    : {raw_file}")
    lines.append(f"Start Time  : {start_ts}")
    lines.append(f"End Time    : {end_ts}")
    lines.append(f"Duration    : {end_ts - start_ts}")
    lines.append("")
    lines.append(f"Log file: {logfile}")
    lines.append("")
    lines.append("Step Summary:")
    for s in steps:
        step_status = "OK" if s["ok"] else "FAILED"
        rc = s["returncode"]
        lines.append(f" - {s['name']}: {step_status} (rc={rc})")

    if any_failed:
        lines.append("")
        lines.append("Errors (stderr tail for failed steps):")
        for s in steps:
            if s["ok"]:
                continue
            lines.append("")
            lines.append(f"--- {s['name']} ---")
            stderr = s["stderr"] or ""
            if len(stderr) > 2000:
                stderr = stderr[-2000:]
            lines.append(stderr if stderr else "(no stderr)")
            if s["exception"]:
                lines.append("Exception:")
                lines.append(s["exception"])

    body = "\n".join(lines)

    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_list)
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.send_message(msg)
        LOGGER.info("Email sent.")
    except Exception as ex:
        LOGGER.error("Failed to send email: %s", ex)


# ----------------------------------------------------------------------
# CLI argument parsing
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run full UG Survey ETL and send email summary."
    )
    parser.add_argument(
        "--env",
        default="DEV",
        help="Environment label for logging/email only (e.g., DEV/TEST/ACPT/PROD).",
    )
    parser.add_argument(
        "--mode",
        default="test",
        help="Logical mode label (e.g., test/full/incremental).",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Override log directory (otherwise uses app.log_dir from settings).",
    )
    parser.add_argument(
        "--raw-file",
        required=True,
        help=(
            "Path to the UG Survey raw CSV file, or a glob pattern "
            'such as "F:/Dat/UGSurvey/UGSurveyData_*.csv".'
        ),
    )
    parser.add_argument(
        "--mssql-url",
        default=None,
        help="Optional MSSQL SQLAlchemy URL. If provided, overrides MSSQL_URL env var.",
    )
    parser.add_argument(
        "--settings",
        default="config/settings.yaml",
        help="Path to settings YAML (default: config/settings.yaml)",
    )
    return parser.parse_args()


# ----------------------------------------------------------------------
# Run ETL for a single raw file
# ----------------------------------------------------------------------
def run_full_etl_for_file(
    env: str,
    mode: str,
    raw_path: Path,
    logfile: Path,
    settings: Dict[str, Any],
) -> int:
    LOGGER.info("================================================================")
    LOGGER.info("Starting FULL ETL for file: %s (env=%s mode=%s)", raw_path, env, mode)
    LOGGER.info("================================================================")

    if not raw_path.is_file():
        LOGGER.error("Raw file does not exist: %s", raw_path)
        return 1

    # Detect survey type + inferred term (for logging / future branching)
    survey_type = detect_survey_type(str(raw_path))
    term_label = infer_term_from_filename(str(raw_path))

    LOGGER.info("Detected survey type: %s", survey_type)
    if term_label:
        LOGGER.info("Inferred term from file name: %s", term_label)

    steps_to_run = build_steps(str(raw_path))

    start_ts = datetime.now()
    step_results: List[Dict[str, Any]] = []

    for name, cmd in steps_to_run:
        res = run_step(name, cmd)
        step_results.append(res)
        if not res["ok"]:
            LOGGER.error("Stopping ETL for file %s due to failure in step: %s", raw_path, name)
            break

    end_ts = datetime.now()
    send_email_summary(env, mode, step_results, logfile, start_ts, end_ts, settings, raw_file=raw_path)

    if any(not s["ok"] for s in step_results):
        LOGGER.error("One or more steps FAILED for file %s. See log for details: %s", raw_path, logfile)
        return 1

    LOGGER.info("All steps completed successfully for file: %s", raw_path)
    return 0


# ----------------------------------------------------------------------
# Main entrypoint
# ----------------------------------------------------------------------
def main() -> int:
    args = parse_args()

    # Load settings first
    try:
        settings = load_settings(args.settings)
    except Exception as ex:
        # Basic fallback logging to stderr if settings can't be read
        print(f"FATAL: Failed to load settings from {args.settings}: {ex}", file=sys.stderr)
        return 1

    app_cfg = settings.get("app", {})
    log_dir = args.log_dir or app_cfg.get("log_dir", "logs")
    log_level = app_cfg.get("log_level", "INFO")

    logfile = setup_app_logging("UG_Survey_FullETL", log_dir, log_level)

    # Allow MSSQL URL override
    if args.mssql_url:
        os.environ["MSSQL_URL"] = args.mssql_url
        LOGGER.info("MSSQL_URL overridden via --mssql-url")

    incoming_dir = Path("F:/Dat/UGSurvey")  # or read from settings.yaml
    pattern = incoming_dir / "UGSurveyData_*.csv"

    LOGGER.info("Looking for incoming files matching %s", pattern)
    matched_files = sorted(glob.glob(str(pattern)))

    if not matched_files:
        LOGGER.info("No UGSurveyData_*.csv files found. Nothing to process.")
        return 0

    if len(matched_files) > 1:
        LOGGER.error("Multiple incoming files found. Please archive or remove extras:")
        for f in matched_files:
            LOGGER.error(" - %s", f)
        return 1

    raw_path = Path(matched_files[0])
    LOGGER.info("Found file to process: %s", raw_path)

    for f in matched_files:
        LOGGER.info(" - %s", f)

    overall_rc = 0
    for f in matched_files:
        rc = run_full_etl_for_file(
            env=args.env,
            mode=args.mode,
            raw_path=Path(f),
            logfile=logfile,
            settings=settings,
        )
        if rc != 0:
            overall_rc = rc

    return overall_rc


if __name__ == "__main__":
    raise SystemExit(main())
