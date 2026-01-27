"""
PandaScore Sync Logic for Esports Pickem Bot.

Provides functions to sync matches, teams, and contests from the
PandaScore API into the local database.
"""

import logging
import asyncio
from typing import Optional, List, Dict, Any, Tuple, Callable, Awaitable

from sqlmodel import select
from src.models import Result
from src.pandascore_client import (
    pandascore_client,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
)
from src.db import get_async_session
from src.crud import get_match_by_pandascore_id
from src.notifications import (
    send_result_notification,
    send_match_time_change_notification,
)
from src.notification_batcher import batcher
from src.pandascore_utils import (
    safe_schedule,
    safe_notify,
    maybe_start_running_match,
    maybe_finish_running_match,
)
from src.pandascore_processing import (
    PandaScoreSyncContext,
    _process_single_match,
    _detect_match_result,
)
from src.parsers.lol import LoLParser

logger = logging.getLogger(__name__)


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

    # Run all notification-related actions within a batching context
    # so they are flushed as a single announcement (per type) at the end.
    async with batcher.batching():
        # 1. Schedule reminders
        match_calls: List[Callable[[], Awaitable[None]]] = [
            (lambda m=match: safe_schedule(m)) for match in matches_to_schedule
        ]
        await _process_with_yield_calls(match_calls, batch=5)

        # 2. Send result notifications
        logger.info("Sending %d result notifications...", len(notifications))
        notif_calls: List[Callable[[], Awaitable[None]]] = [
            (lambda mid=mid, rid=rid: safe_notify(mid, rid))
            for mid, rid in notifications
        ]
        await _process_with_yield_calls(notif_calls, batch=5)

        # 3. Send time change notifications
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


async def _fetch_matches_for_sync(league_ids: Optional[List[int]]):
    """Fetch upcoming, running and recent past matches for sync.

    Returns combined list or None on failure.
    """
    try:
        game_slug = "lol"
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

        return upcoming + running + past
    except Exception:
        logger.exception("Failed to fetch matches from PandaScore")
        return None


async def _process_matches_and_commit(
    matches_data: List[Any], db_session
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
    # Initialize the parser here.
    # Future enhancement: allow passing parser from caller.
    parser = LoLParser()
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

    matches_data = await _fetch_matches_for_sync(league_ids)
    if matches_data is None:
        return None
    if not matches_data:
        logger.info("No matches found from PandaScore")
        return {"contests": 0, "matches": 0, "teams": 0}

    logger.info(
        "Fetched total of %d matches from PandaScore", len(matches_data)
    )

    async with get_async_session() as db_session:
        (
            all_matches_to_schedule,
            all_notifications,
            all_time_changes,
            summary,
        ) = await _process_matches_and_commit(matches_data, db_session)

    await _run_post_sync_actions(
        all_matches_to_schedule, all_notifications, all_time_changes
    )
    return summary


async def sync_running_matches() -> Dict[str, Any]:
    """
    Sync currently running (live) matches from PandaScore.

    Fetches all running matches and updates their status in the database.
    Detects newly started and finished matches.

    Returns:
        Summary with lists of started and finished match IDs
    """
    logger.info("Syncing running matches from PandaScore...")

    try:
        running_matches = await pandascore_client.fetch_running_matches(
            game="lol"
        )
    except Exception:
        logger.exception("Failed to fetch running matches")
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

            # Detect finished matches in the same session and collect ids
            finished_id = await maybe_finish_running_match(
                db_session, match_data
            )
            if finished_id:
                finished.append(finished_id)

            # Yield to the event loop to avoid blocking long syncs
            await asyncio.sleep(0)

        await db_session.commit()

    logger.info(
        "Running matches sync complete: %d started, %d finished",
        len(started),
        len(finished),
    )
    return {"started": started, "finished": finished}


async def fetch_and_update_match_result(pandascore_id: int) -> bool:
    """
    Fetch result for a specific match and update the database.

    Parameters:
        pandascore_id: PandaScore match ID

    Returns:
        True if result was successfully saved, False otherwise
    """
    logger.info("Fetching result for PandaScore match %s", pandascore_id)

    match_data = await _fetch_pandascore_match(pandascore_id)
    if not match_data:
        return False

    async with get_async_session() as db_session:
        match = await _load_db_match(db_session, pandascore_id)
        if not match:
            return False

        if await _result_exists(db_session, match.id):
            logger.info("Match %s already has a result", match.id)
            return True

        ctx = PandaScoreSyncContext(
            db_session=db_session,
            summary={"contests": 0, "matches": 0, "teams": 0},
            parser=LoLParser(),
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
) -> Optional[Dict[str, Any]]:
    try:
        match_data = await pandascore_client.fetch_match_by_id(
            pandascore_id, game="lol"
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
