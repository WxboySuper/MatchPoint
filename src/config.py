import os
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
