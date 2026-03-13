import os
from pathlib import Path

DATA_PATH = Path(os.getenv("DATA_PATH", "data"))

# Leaguepedia API Credentials
LEAGUEPEDIA_USER = os.getenv("LEAGUEPEDIA_USER")
LEAGUEPEDIA_PASS = os.getenv("LEAGUEPEDIA_PASS")

# PandaScore API Key
PANDASCORE_API_KEY = os.getenv("PANDASCORE_API_KEY")


# Default games to sync/poll when no per-guild configuration is present.
# Comma-separated env var supported (e.g. "lol,cs2").
def _parse_default_games(env_val: str | None):
    if not env_val:
        return ["lol"]
    parts = [p.strip() for p in env_val.split(",") if p.strip()]
    return parts or ["lol"]


DEFAULT_GAMES = _parse_default_games(os.getenv("DEFAULT_GAMES"))

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
