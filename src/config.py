import os
import json
from pathlib import Path

DATA_PATH = Path(os.getenv("DATA_PATH", "data"))

# Leaguepedia API Credentials
LEAGUEPEDIA_USER = os.getenv("LEAGUEPEDIA_USER")
LEAGUEPEDIA_PASS = os.getenv("LEAGUEPEDIA_PASS")

# PandaScore API Key
PANDASCORE_API_KEY = os.getenv("PANDASCORE_API_KEY")

# Reminder minutes list (comma-separated env var supported).
# Defaults: 5, 30, and 1440.


def _parse_reminder_minutes(env_val: str | None):
    if not env_val:
        return [5, 30, 1440]
    parts = [p.strip() for p in env_val.split(",") if p.strip()]
    result = []
    for p in parts:
        try:
            result.append(int(p))
        except ValueError:
            # ignore invalid values
            continue
    return result or [5, 30, 1440]


REMINDER_MINUTES = _parse_reminder_minutes(os.getenv("REMINDER_MINUTES"))


def _apply_json_flags(env_val: str, flags: dict):
    """Try to parse JSON and update flags dict."""
    try:
        user_flags = json.loads(env_val)
        if isinstance(user_flags, dict):
            for k, v in user_flags.items():
                if k in flags:
                    flags[k] = str(v).lower() == "true"
            return True
    except json.JSONDecodeError:
        pass
    return False


def _apply_csv_flags(env_val: str, flags: dict):
    """Fallback to comma-separated key=value pairs."""
    for part in env_val.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            k = k.strip()
            if k in flags:
                flags[k] = v.strip().lower() == "true"


def _parse_feature_flags(env_val: str | None) -> dict:
    """
    Parse feature flags from environment variable
    (JSON or KEY=TRUE,KEY2=FALSE).
    """
    flags = {
        "CS2_ENABLED": False,
        "VALORANT_ENABLED": False,
        "DOTA2_ENABLED": False,
        "ROCKET_LEAGUE_ENABLED": False,
        "USE_REAL_RATE_LIMITS": True,
    }
    if not env_val:
        return flags

    if not _apply_json_flags(env_val, flags):
        _apply_csv_flags(env_val, flags)

    return flags


FEATURE_FLAGS = _parse_feature_flags(os.getenv("FEATURE_FLAGS"))
