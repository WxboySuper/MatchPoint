from typing import Optional

from src.contest_tier import display_contest_tier
from src.parsers.game_slug import game_display_name

STATUS_BADGES = {
    "finished": "✅ Finished",
    "running": "🔴 Live",
    "live": "🔴 Live",
    "in_progress": "🔴 Live",
    "not_started": "⏳ Upcoming",
    "scheduled": "⏳ Upcoming",
    "canceled": "❌ Canceled",
    "postponed": "🕒 Postponed",
}
LIVE_STATUSES = ("running", "live", "in_progress")


def status_badge(status: Optional[str]) -> str:
    if not status:
        return "⏳ Upcoming"
    return STATUS_BADGES.get(status, status.capitalize())


def format_match_heading(match) -> str:
    return f"{match.team1} vs {match.team2} — {status_badge(match.status)}"


def format_match_metadata(match) -> str:
    contest = getattr(match, "contest", None)
    contest_name = getattr(contest, "name", "Unknown contest")
    parts = [contest_name]

    tier = display_contest_tier(getattr(contest, "tier", None))
    if tier:
        parts.append(tier)

    parts.append(game_display_name(getattr(match, "game", None)))
    if getattr(match, "best_of", None):
        parts.append(f"Bo{match.best_of}")

    return " • ".join(parts)


def format_match_time(match) -> str:
    timestamp = int(match.scheduled_time.timestamp())
    return f"<t:{timestamp}:F> • <t:{timestamp}:R>"


def format_match_result_or_score(match) -> Optional[str]:
    result = getattr(match, "result", None)
    if result:
        score = f" ({result.score})" if getattr(result, "score", None) else ""
        return f"Final: **{result.winner}** won{score}"

    if getattr(match, "status", None) in LIVE_STATUSES and getattr(
        match, "last_announced_score", None
    ):
        return f"Live score: {match.last_announced_score}"

    return None
