"""
PandaScore Sync Logic for Esports Pickem Bot.

Provides functions to sync matches, teams, and contests from the
PandaScore API into the local database.
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from sqlalchemy.exc import OperationalError
from sqlmodel import select

from src.crud import get_match_by_pandascore_id
from src.db import get_async_session
from src.models import GuildConfig, Match, Result
from src.notification_batcher import batcher
from src.notifications import (
    send_match_time_change_notification,
    send_result_notification,
)
from src.pandascore_client import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    pandascore_client,
)
from src.pandascore_processing import (
    PandaScoreSyncContext,
    _detect_match_result,
    _process_single_match,
)
from src.pandascore_utils import (
    maybe_finish_running_match,
    maybe_start_running_match,
    safe_notify,
    safe_schedule,
)
from src.parsers.cs2 import normalize_counter_strike_slug
from src.parsers.factory import get_parser, get_supported_game_slugs

logger = logging.getLogger(__name__)


def _normalize_enabled_game(
    raw_game: str,
    supported: set[str],
    seen: set[str],
) -> Optional[str]:
    game = raw_game.strip().lower()
    if not game or game in seen:
        return None
    if game not in supported:
        return None
    return game


def _parse_enabled_games(raw_games: Optional[str]) -> list[str]:
    if raw_games is None:
        return []

    supported = set(get_supported_game_slugs())
    normalized = []
    seen = set()
    for raw in raw_games.split(","):
        game = _normalize_enabled_game(raw, supported, seen)
        if game is None:
            continue
        seen.add(game)
        normalized.append(game)
    return normalized


async def _sync_game_matches(
    league_ids: Optional[List[int]],
    game_slug: str,
) -> Optional[
    tuple[
        List[Any],
        List[Tuple[int, int]],
        List[Tuple[Any, Any, Any]],
        Dict[str, int],
    ]
]:
    matches_data = await _fetch_matches_for_sync(league_ids, game_slug)
    if matches_data is None:
        return None
    if not matches_data:
        logger.info("No %s matches found from PandaScore", game_slug)
        await _reconcile_finished_matches_for_game(game_slug)
        return [], [], [], {"contests": 0, "matches": 0, "teams": 0}

    logger.info(
        "Fetched total of %d %s matches from PandaScore",
        len(matches_data),
        game_slug,
    )
    async with get_async_session() as db_session:
        sync_result = await _process_matches_and_commit(
            matches_data, db_session, game_slug
        )

    await _reconcile_finished_matches_for_game(game_slug)
    return sync_result


def _merge_sync_summary(
    total_summary: Dict[str, int],
    game_summary: Dict[str, int],
) -> None:
    for key in total_summary:
        total_summary[key] += game_summary[key]


async def _sync_running_game(game_slug: str) -> Dict[str, Any]:
    try:
        running_matches = await pandascore_client.fetch_running_matches(
            game=game_slug
        )
    except Exception:
        logger.exception("Failed to fetch running matches for %s", game_slug)
        return {"started": [], "finished": [], "error": True}

    started = []
    finished = []
    async with get_async_session() as db_session:
        for match_data in running_matches:
            started_id = await maybe_start_running_match(
                db_session, match_data
            )
            if started_id:
                started.append(started_id)

            finished_id = await maybe_finish_running_match(
                db_session, match_data
            )
            if finished_id:
                finished.append(finished_id)
            await asyncio.sleep(0)

        await db_session.commit()

    await _reconcile_finished_matches_for_game(game_slug)
    return {"started": started, "finished": finished, "error": False}


async def _configured_sync_games(
    requested_game: Optional[str] = None,
) -> list[str]:
    if requested_game:
        return [requested_game]

    from src.config import DEFAULT_GAMES

    supported = set(get_supported_game_slugs())
    configured = {
        game for game in (DEFAULT_GAMES or ["lol"]) if game in supported
    }

    try:
        async with get_async_session() as db_session:
            rows = await db_session.exec(select(GuildConfig.enabled_games))
            for raw_games in rows.all():
                configured.update(_parse_enabled_games(raw_games))
    except OperationalError:
        logger.warning(
            "Guild configuration table unavailable; using default sync games."
        )

    ordered = [
        game for game in get_supported_game_slugs() if game in configured
    ]
    return ordered or ["lol"]


async def _run_post_sync_actions(
    matches_to_schedule: List[Any],
    notifications: List[Tuple[int, int]],
    time_change_notifications: List[Tuple[Any, Any, Any]],
) -> None:
    """
    Schedule reminders and send result/time-change notifications after sync.
    """

    async def _process_with_yield_calls(
        calls: List[Callable[[], Awaitable[None]]], batch: int = 5
    ) -> None:
        """Run callables and yield to event loop every `batch` items."""
        for i, call in enumerate(calls):
            await call()
            if (i + 1) % batch == 0:
                await asyncio.sleep(0)

    async with batcher.batching():
        match_calls: List[Callable[[], Awaitable[None]]] = [
            (lambda m=match: safe_schedule(m)) for match in matches_to_schedule
        ]
        await _process_with_yield_calls(match_calls, batch=5)

        logger.info("Sending %d result notifications...", len(notifications))
        notif_calls: List[Callable[[], Awaitable[None]]] = [
            (lambda mid=mid, rid=rid: safe_notify(mid, rid))
            for mid, rid in notifications
        ]
        await _process_with_yield_calls(notif_calls, batch=5)

        if time_change_notifications:
            logger.info(
                "Sending %d time change notifications...",
                len(time_change_notifications),
            )

            async def _safe_time_notify(m, old, new):
                try:
                    await send_match_time_change_notification(m, old, new)
                except Exception:
                    logger.exception(
                        "Failed to send time change notification for match %s",
                        m.id,
                    )

            time_calls: List[Callable[[], Awaitable[None]]] = [
                (lambda m=m, old=old, new=new: _safe_time_notify(m, old, new))
                for m, old, new in time_change_notifications
            ]
            await _process_with_yield_calls(time_calls, batch=5)


async def _fetch_matches_for_sync(
    league_ids: Optional[List[int]], game_slug: str
):
    """Fetch upcoming, running and recent past matches for sync.

    Returns combined list or None on failure.
    """
    try:
        upcoming_coro = pandascore_client.fetch_matches(
            "upcoming",
            {
                "filter_key": "league_id",
                "filter_values": league_ids,
                "page_size": MAX_PAGE_SIZE,
                "page": 1,
            },
            game=game_slug,
        )
        running_coro = pandascore_client.fetch_matches(
            "running",
            {"page_size": DEFAULT_PAGE_SIZE},
            game=game_slug,
        )
        past_coro = pandascore_client.fetch_matches(
            "recent_past",
            {
                "filter_key": "league_id",
                "filter_values": league_ids,
                "page_size": DEFAULT_PAGE_SIZE,
            },
            game=game_slug,
        )

        upcoming, running, past = await asyncio.gather(
            upcoming_coro, running_coro, past_coro
        )
        logger.info(
            "Fetched %s matches from PandaScore: %d upcoming, %d running, "
            "%d recent past",
            game_slug,
            len(upcoming),
            len(running),
            len(past),
        )

        return upcoming + running + past
    except Exception:
        logger.exception(
            "Failed to fetch matches from PandaScore for %s",
            game_slug,
        )
        return None


async def _reconcile_finished_matches_for_game(game_slug: str) -> None:
    async with get_async_session() as db_session:
        stmt = (
            select(Match)
            .where(Match.game == game_slug)
            .where(Match.status == "running")
        )
        running_matches = list((await db_session.exec(stmt)).all())

    for match in running_matches:
        pandascore_id = getattr(match, "pandascore_id", None)
        if not pandascore_id:
            continue

        match_data = await _fetch_pandascore_match(pandascore_id, game_slug)
        if not match_data:
            continue
        await fetch_and_update_match_result(pandascore_id, game_slug=game_slug)


async def _process_matches_and_commit(
    matches_data: List[Any], db_session, game: str
) -> Tuple[
    List[Any],
    List[Tuple[int, int]],
    List[Tuple[Any, Any, Any]],
    Dict[str, int],
]:
    """
    Process matches; commit DB changes; return schedules/notifications
    and summary.
    """
    summary = {"contests": 0, "matches": 0, "teams": 0}
    parser = get_parser(game)
    if parser is None:
        logger.error("No parser available for '%s'", game)
        return ([], [], [], summary)

    ctx = PandaScoreSyncContext(
        db_session=db_session, summary=summary, parser=parser
    )

    for i, match_data in enumerate(matches_data):
        try:
            match = await _process_single_match(match_data, ctx)
            if match:
                await _detect_match_result(match_data, match, ctx)
        except Exception:
            logger.exception("Error processing match %s", match_data.get("id"))

        if i % 10 == 0:
            await asyncio.sleep(0)

    await db_session.commit()
    return (
        ctx.matches_to_schedule,
        ctx.notifications,
        ctx.time_change_notifications,
        ctx.summary,
    )


async def perform_pandascore_sync(
    league_ids: Optional[List[int]] = None,
    game: Optional[str] = None,
) -> Optional[Dict[str, int]]:
    """
    Perform a full sync of upcoming matches from PandaScore.

    Fetches all upcoming LoL matches, creates/updates contests, teams,
    and matches in the database, and schedules reminders for new matches.

    Parameters:
        league_ids: Optional list of PandaScore league IDs to filter by.
            If None, fetches all upcoming LoL matches.

    Returns:
        Summary dict with counts of upserted entities, or None on failure.
    """
    logger.info("Starting PandaScore sync...")

    total_summary = {"contests": 0, "matches": 0, "teams": 0}
    matches_to_schedule: List[Any] = []
    notifications: List[Tuple[int, int]] = []
    time_changes: List[Tuple[Any, Any, Any]] = []
    failed_games: List[str] = []
    completed_games = 0

    for game_slug in await _configured_sync_games(game):
        sync_result = await _sync_game_matches(league_ids, game_slug)
        if sync_result is None:
            failed_games.append(game_slug)
            continue

        completed_games += 1
        (
            game_matches_to_schedule,
            game_notifications,
            game_time_changes,
            game_summary,
        ) = sync_result

        matches_to_schedule.extend(game_matches_to_schedule)
        notifications.extend(game_notifications)
        time_changes.extend(game_time_changes)
        _merge_sync_summary(total_summary, game_summary)

    await _run_post_sync_actions(
        matches_to_schedule,
        notifications,
        time_changes,
    )
    if failed_games:
        logger.error(
            "PandaScore sync failed for games: %s",
            ", ".join(failed_games),
        )
        if completed_games == 0:
            return None
    return total_summary


async def sync_running_matches() -> Dict[str, Any]:
    """
    Sync currently running (live) matches from PandaScore.

    Fetches all running matches and updates their status in the database.
    Detects newly started and finished matches.

    Returns:
        Summary with lists of started and finished match IDs
    """
    logger.info("Syncing running matches from PandaScore...")

    started = []
    finished = []
    error = False
    for game_slug in await _configured_sync_games():
        game_result = await _sync_running_game(game_slug)
        started.extend(game_result["started"])
        finished.extend(game_result["finished"])
        error = error or game_result["error"]

    logger.info(
        "Running matches sync complete: %d started, %d finished",
        len(started),
        len(finished),
    )
    return {"started": started, "finished": finished, "error": error}


def _match_data_game(match_data: Dict[str, Any]) -> Optional[str]:
    videogame = match_data.get("videogame") or {}
    raw_slug = (videogame.get("slug") or "").lower()
    title = str(match_data.get("videogame_title") or "").lower()
    return normalize_counter_strike_slug(raw_slug, title) or raw_slug or None


def _default_sync_game() -> str:
    from src.config import DEFAULT_GAMES

    supported = set(get_supported_game_slugs())
    for game_slug in DEFAULT_GAMES or ["lol"]:
        if game_slug in supported:
            return game_slug
    return "lol"


def _resolve_match_game(match, match_data: Dict[str, Any]) -> str:
    if getattr(match, "game", None):
        return match.game

    resolved_game = _match_data_game(match_data)
    if resolved_game:
        return resolved_game

    return _default_sync_game()


async def fetch_and_update_match_result(
    pandascore_id: int, game_slug: Optional[str] = None
) -> bool:
    """
    Fetch result for a specific match and update the database.

    Parameters:
        pandascore_id: PandaScore match ID

    Returns:
        True if result was successfully saved, False otherwise
    """
    logger.info("Fetching result for PandaScore match %s", pandascore_id)

    async with get_async_session() as db_session:
        match = await _load_db_match(db_session, pandascore_id)
        if not match:
            return False

        resolved_game = game_slug or _resolve_match_game(match, {})
        match_data = await _fetch_pandascore_match(
            pandascore_id, resolved_game
        )
        if not match_data:
            return False

        match.status = "finished"
        db_session.add(match)
        if await _result_exists(db_session, match.id):
            await db_session.commit()
            logger.info("Match %s already has a result", match.id)
            return True

        resolved_game = _resolve_match_game(match, match_data)
        parser = get_parser(resolved_game)
        if parser is None:
            logger.error("No parser available for '%s'", resolved_game)
            return False
        ctx = PandaScoreSyncContext(
            db_session=db_session,
            summary={"contests": 0, "matches": 0, "teams": 0},
            parser=parser,
        )

        await _detect_match_result(match_data, match, ctx)
        try:
            await db_session.commit()
        except Exception:
            logger.exception(
                "Failed to commit detected match result for %s", pandascore_id
            )
            return False

        await _send_notifications_if_any(ctx)
        return True


async def _fetch_pandascore_match(
    pandascore_id: int,
    game_slug: str,
) -> Optional[Dict[str, Any]]:
    try:
        match_data = await pandascore_client.fetch_match_by_id(
            pandascore_id, game=game_slug
        )
    except Exception:
        logger.exception("Failed to fetch match %s", pandascore_id)
        return None

    if not match_data:
        logger.warning("Match %s not found in PandaScore", pandascore_id)
        return None

    if match_data.get("status") != "finished":
        logger.info("Match %s is not finished yet", pandascore_id)
        return None

    return match_data


async def _load_db_match(db_session, pandascore_id: int):
    match = await get_match_by_pandascore_id(db_session, pandascore_id)
    if not match:
        logger.warning(
            "Match with PandaScore ID %s not in database", pandascore_id
        )
        return None
    return match


async def _result_exists(db_session, match_id: int) -> bool:
    stmt = select(Result).where(Result.match_id == match_id)
    res = await db_session.exec(stmt)
    return bool(res.first())


async def _send_notifications_if_any(ctx: PandaScoreSyncContext) -> None:
    if not ctx.notifications:
        return
    for match_id, result_id in ctx.notifications:
        await send_result_notification(match_id, result_id)
