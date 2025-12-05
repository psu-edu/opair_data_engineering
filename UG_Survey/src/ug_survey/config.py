import os
import logging
from pathlib import Path
from typing import Any, Dict

import yaml

LOGGER = logging.getLogger("UG_Survey_Config")


def _expand_env_vars(value: Any) -> Any:
    """
    Recursively expand ${VAR} in strings using environment variables.
    Leaves values unchanged if no env var is found.
    """
    if isinstance(value, str):
        # Simple ${VAR} replacement
        out = ""
        i = 0
        s = value
        while i < len(s):
            if s[i] == "$" and i + 1 < len(s) and s[i + 1] == "{":
                end = s.find("}", i + 2)
                if end == -1:
                    # No closing brace – treat literally
                    out += s[i]
                    i += 1
                    continue
                var_name = s[i + 2:end]
                env_val = os.getenv(var_name, "")
                out += env_val
                i = end + 1
            else:
                out += s[i]
                i += 1
        return out
    elif isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_expand_env_vars(v) for v in value]
    else:
        return value


def load_settings(path: str = "config/settings.yaml") -> Dict[str, Any]:
    """
    Load YAML settings and expand ${VAR} using environment variables.
    """
    settings_path = Path(path)
    if not settings_path.is_file():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")

    with settings_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    cfg = _expand_env_vars(raw)

    # Basic sanity check
    db_url = cfg.get("database", {}).get("url", "").strip()
    if not db_url:
        raise RuntimeError("database.url is empty after environment expansion.")

    return cfg
