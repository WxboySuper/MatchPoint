"""
Processing helpers extracted from `pandascore_sync.py` to reduce
file-level complexity.

Contains lightweight parsing, extraction and per-match processing helpers
that operate on PandaScore API payloads and the database session.
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

from sqlmodel import select
from src.models import Result
from src.crud import (
    upsert_team_by_pandascore,
    upsert_contest_by_pandascore,
    upsert_match_by_pandascore,
)
from src.match_result_utils import save_result_and_update_picks
from src.parsers.base import PandaScoreParser

logger = logging.getLogger(__name__)


@dataclass
class PandaScoreSyncContext:
    db_session: Any
    summary: Dict[str, int]
    parser: PandaScoreParser
    matches_to_schedule: List[Any] = field(default_factory=list)
    notifications: List[Tuple[int, int]] = field(default_factory=list)
    time_change_notifications: List[Tuple[Any, datetime, datetime]] = field(
        default_factory=list
    )


async def _process_teams_from_match(
    match_data: Dict[str, Any], ctx: PandaScoreSyncContext
) -> None:
    opponents = match_data.get("opponents", [])

    for opponent in opponents:
        team_data = ctx.parser.extract_team_data(opponent)
        if team_data and team_data.get("pandascore_id"):
            team = await upsert_team_by_pandascore(ctx.db_session, team_data)
            if team:
                ctx.summary["teams"] += 1


async def _get_or_create_contest(
    match_data: Dict[str, Any], ctx: PandaScoreSyncContext
) -> Optional[Any]:
    contest_data = ctx.parser.extract_contest_data(match_data)

    if not contest_data.get("pandascore_league_id") or not contest_data.get(
        "pandascore_serie_id"
    ):
        logger.warning("Match missing league or serie info: %s", match_data)
        return None

    contest = await upsert_contest_by_pandascore(ctx.db_session, contest_data)
    if contest:
        ctx.summary["contests"] += 1
    return contest


def _should_notify_time_change(
    is_new: bool,
    time_changed: bool,
    old_time: Optional[datetime],
    new_time: Optional[datetime],
):
    is_existing_time_changed = not is_new and time_changed
    has_old_and_new_time = old_time is not None and new_time is not None

    if is_existing_time_changed and has_old_and_new_time:
        return True
    return False


async def _process_single_match(
    match_data: Dict[str, Any], ctx: PandaScoreSyncContext
) -> Optional[Any]:
    contest = await _get_or_create_contest(match_data, ctx)
    if not contest:
        return None

    await _process_teams_from_match(match_data, ctx)

    match_info = ctx.parser.extract_match_data(match_data, contest.id)
    if not match_info:
        return None

    match, is_new, time_changed, old_time = await upsert_match_by_pandascore(
        ctx.db_session, match_info
    )

    if match:
        ctx.summary["matches"] += 1
        if time_changed or is_new:
            ctx.matches_to_schedule.append(match)

        if _should_notify_time_change(
            is_new, time_changed, old_time, match.scheduled_time
        ):
            ctx.time_change_notifications.append(
                (match, old_time, match.scheduled_time)
            )

    return match


async def _detect_match_result(
    match_data: Dict[str, Any], match: Any, ctx: PandaScoreSyncContext
) -> None:
    status = match_data.get("status")
    if status != "finished":
        return

    # Check if result already exists without triggering lazy load.
    # We check the database directly to be 100% safe against MissingGreenlet.
    stmt = select(Result).where(Result.match_id == match.id)
    existing_result = (await ctx.db_session.exec(stmt)).first()
    if existing_result:
        return

    winner_id = match_data.get("winner_id")
    if not winner_id:
        return

    winner_name, team1_score, team2_score = (
        ctx.parser.extract_winner_and_scores(match_data, match, winner_id)
    )

    if not winner_name:
        logger.warning(
            "Could not determine winner name for match %s", match.id
        )
        return

    score_str = f"{team1_score}-{team2_score}"
    logger.info(
        "Detected result for match %s: %s wins %s",
        match.id,
        winner_name,
        score_str,
    )

    try:
        result = await save_result_and_update_picks(
            ctx.db_session, match, winner_name, score_str
        )
        await ctx.db_session.flush()
        ctx.notifications.append((match.id, result.id))
    except Exception:
        logger.exception("Failed to save result for match %s", match.id)
