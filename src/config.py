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


def _apply_json_flags(env_val: str, default_flags: dict) -> bool:
    """Try to parse and apply JSON flags. Returns True if successful."""
    try:
        user_flags = json.loads(env_val)
        if not isinstance(user_flags, dict):
            return False

        # Validate types for known flags
        for k, v in user_flags.items():
            if k in default_flags and isinstance(default_flags[k], bool):
                # Coerce strings 'true'/'false' to bool
                if isinstance(v, str):
                    user_flags[k] = v.lower() == "true"
                elif not isinstance(v, bool):
                    # Force other types (int 0/1) to bool
                    user_flags[k] = bool(v)

        default_flags.update(user_flags)
        return True
    except json.JSONDecodeError:
        return False


def _parse_feature_flags(env_val: str | None) -> dict:
    """
    Parse feature flags from environment variable
    (JSON or KEY=TRUE,KEY2=FALSE).
    """
    default_flags = {
        "CS2_ENABLED": False,
        "VALORANT_ENABLED": False,
        "DOTA2_ENABLED": False,
        "ROCKET_LEAGUE_ENABLED": False,
        "USE_REAL_RATE_LIMITS": True,
    }
    if not env_val:
        return default_flags

    # Try parsing as JSON
    if _apply_json_flags(env_val, default_flags):
        return default_flags

    # Fallback: key=value
    for part in env_val.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            k = k.strip()
            v_bool = v.strip().lower() == "true"
            default_flags[k] = v_bool

    return default_flags


FEATURE_FLAGS = _parse_feature_flags(os.getenv("FEATURE_FLAGS"))
