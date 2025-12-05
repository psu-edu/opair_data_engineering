import csv
import re
from pathlib import Path
from typing import Literal, Optional

SurveyType = Literal["UG", "PG", "UNKNOWN"]


def detect_survey_type(csv_path: str) -> SurveyType:
    """
    Heuristic: look at header names and decide UG vs PG.
    You can tune this by adding/removing markers.
    """
    path = Path(csv_path)
    with path.open("r", newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        header_lower = [h.lower() for h in header]

    ug_markers = ["plans_cleaned", "fe_college", "indicator_internship"]
    pg_markers = ["wc_postgrad", "postgrad", "pg_"]  # example, adjust later

    if any(m in h for h in header_lower for m in ug_markers):
        return "UG"
    if any(m in h for h in header_lower for m in pg_markers):
        return "PG"
    return "UNKNOWN"


TERM_PATTERN = re.compile(r"(\d{6})(SP|FA|SU|WN)", re.IGNORECASE)


def infer_term_from_filename(path_str: str) -> Optional[str]:
    """
    Example: 'UGSurveyData_202324SP_11_5_25.csv' -> '2023-24 Spring'
    This does NOT produce STRM. It's just a human-friendly label.
    """
    name = Path(path_str).name
    m = TERM_PATTERN.search(name)
    if not m:
        return None

    ay_code, season = m.groups()
    # Example: 202324 -> 2023-24
    start_year = ay_code[:4]
    end_year = ay_code[2:]  # '2324' -> '24'
    season = season.upper()

    season_map = {"SP": "Spring", "FA": "Fall", "SU": "Summer", "WN": "Winter"}
    season_label = season_map.get(season, season)

    return f"{start_year}-{end_year} {season_label}"
