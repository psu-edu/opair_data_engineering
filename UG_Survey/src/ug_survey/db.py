import os
from typing import Any, Mapping, Optional, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection, Result

from ug_survey.config import load_settings

DEFAULT_SETTINGS_PATH = "config/settings.yaml"


def get_engine(settings: Union[dict, str, None] = None) -> Engine:
    """
    Create a SQLAlchemy engine for MSSQL.

    Accepts:
      - settings: dict -> use this dict directly
      - settings: str  -> treat as path to settings.yaml
      - settings: None -> use DEFAULT_SETTINGS_PATH

    In all cases, MSSQL_URL env var (if set) overrides database.url.
    """
    if isinstance(settings, dict):
        cfg = settings
    else:
        settings_path = settings or DEFAULT_SETTINGS_PATH
        cfg = load_settings(settings_path)

    url = os.getenv("MSSQL_URL") or cfg["database"]["url"]
    if not url:
        raise RuntimeError("database.url is empty after environment expansion.")

    engine = create_engine(url, fast_executemany=True)
    return engine


def mssql_engine(settings: Union[dict, str, None] = None) -> Engine:
    """
    Backward-compatible alias used by older modules (e.g., stage_to_ugs).
    """
    return get_engine(settings)


def exec_query(
    conn: Connection,
    sql: str,
    params: Optional[Mapping[str, Any]] = None,
) -> Result:
    """
    Convenience wrapper to execute raw SQL with optional parameters.
    """
    return conn.execute(text(sql), params or {})


def exec_scalar(
    conn: Connection,
    sql: str,
    params: Optional[Mapping[str, Any]] = None,
) -> Any:
    """
    Execute a scalar SQL query and return the first column of the first row,
    or None if no rows are returned.
    """
    result = exec_query(conn, sql, params)
    row = result.first()
    return row[0] if row is not None else None
