"""
PandaScore-based live match polling for Esports Pickem Bot.

Polls the PandaScore API to detect match starts, score changes, and
final results. Replaces the Leaguepedia-based polling logic.
"""

import logging
import inspect
import asyncio

from src.db import get_async_session
from src.pandascore_client import pandascore_client
from src import crud
from src.pandascore_polling_core import (
    _process_running_match,
    _handle_finished_pandascore_id,
    _process_pandascore_match_data,
    _should_continue_polling,
    _fetch_match_from_pandascore,
    get_known_running_matches,
    remove_known_running_matches,
    remove_known_running_match_by_match_id,
    _remove_job_if_exists as _core_remove,
)

logger = logging.getLogger(__name__)


# Note: running-match state is tracked in
# pandascore_polling_core._known_running_matches


async def poll_live_match_job(match_db_id: int) -> None:
    """
    Poll a single match via PandaScore API and handle result detection.

    Parameters:
        match_db_id: Database ID of the match to poll
    """
    job_id = f"poll_match_{match_db_id}"
    logger.info("Polling for match ID %s (Job: %s)", match_db_id, job_id)

    async with get_async_session() as session:
        match = await crud.get_match_with_result_by_id(session, match_db_id)

        if not await _should_continue_polling(match, job_id, session=session):
            return

        if await _handle_missing_pandascore_id(match):
            await _unschedule_job(job_id)
            return

        match_data = await _fetch_match_data(match)
        if match_data is None:
            logger.info("No data returned for match %s. Will retry.", match.id)
            return

        committed = await _process_pandascore_match_data(
            session,
            match,
            match_data,
            job_id,
        )

        # Persist any changes made during processing (status updates,
        # last_announced_score, etc.). Processing helpers return a boolean
        # indicating whether they already committed a session. Only commit
        # here if no inner commit occurred.
        await _finalize_session_commit(session, committed, match.id)


async def poll_running_matches_job() -> None:
    """
    Poll all currently running matches from PandaScore.

    This job fetches the list of running matches and:
    1. Detects newly started matches
    2. Updates scores for ongoing matches
    3. Detects finished matches (disappeared from running list)
    """
    logger.debug("Running poll_running_matches_job...")

    running_matches = await _fetch_running_matches()
    running_ids = {m.get("id") for m in running_matches if m.get("id")}

    # Process running matches
    async with get_async_session() as session:
        await _process_running_matches(session, running_matches)

    # Process finished matches (were running but no longer are)
    # Use async accessors to safely read and modify the shared running set
    await _handle_finished_matches(running_ids)


async def _fetch_running_matches():
    """Fetch running matches for all configured default games.

    Returns a de-duplicated list of running match dicts. On error returns
    an empty list.
    """
    try:
        from src.config import DEFAULT_GAMES

        games = DEFAULT_GAMES or ["lol"]
        results = await _fetch_running_matches_for_games(games)
        return _merge_running_match_results(results)
    except Exception:
        logger.exception("Failed to fetch running matches")
        return []


async def _process_running_matches(session, running_matches):
    """Process running matches and commit if no inner commit occurred."""
    any_committed = False
    for match_data in running_matches:
        committed = await _process_running_match(session, match_data)
        any_committed = any_committed or bool(committed)

    await _commit_if_needed(session, any_committed)


async def _handle_finished_matches(running_ids):
    """Detect finished matches (were known running but no longer are).

    Removes finished IDs from the known set and dispatches handlers for
    each finished pandascore id.
    """
    known = await get_known_running_matches()
    finished_ids = known - running_ids
    if not finished_ids:
        return

    await remove_known_running_matches(finished_ids)
    for pandascore_id in finished_ids:
        await _handle_finished_pandascore_id(pandascore_id)


async def _handle_missing_pandascore_id(match) -> bool:
    if match.pandascore_id:
        return False

    logger.warning(
        "Match %s has no pandascore_id, cannot poll. Unscheduling.",
        match.id,
    )
    await _remove_stale_running_mapping(match.id)
    return True


async def _remove_stale_running_mapping(match_id: int) -> None:
    try:
        await remove_known_running_match_by_match_id(match_id)
    except Exception:
        logger.exception(
            "Failed to remove stale pandascore mapping for match %s",
            match_id,
        )


async def _fetch_match_data(match):
    return await _fetch_match_from_pandascore(match.pandascore_id)


async def _fetch_running_matches_for_games(games):
    coros = [
        pandascore_client.fetch_running_matches(game=game) for game in games
    ]
    return await asyncio.gather(*coros, return_exceptions=True)


def _merge_running_match_results(results):
    combined = []
    seen = set()
    for result in results:
        if isinstance(result, Exception):
            logger.exception(
                "Error fetching running matches for a game: %s", result
            )
            continue
        for match_data in result:
            _append_running_match(combined, seen, match_data)
    return combined


def _append_running_match(combined, seen, match_data) -> None:
    match_id = match_data.get("id")
    if not match_id or match_id in seen:
        return
    seen.add(match_id)
    combined.append(match_data)


async def _commit_if_needed(session, any_committed: bool) -> None:
    if any_committed:
        return

    maybe = session.commit()
    if inspect.isawaitable(maybe):
        await maybe


async def _unschedule_job(job_id: str) -> None:
    """Unschedule a job by delegating to the polling core helper.

    Kept as a small async wrapper so callers can await any future
    unscheduling implementation without inlining import logic.
    """

    # _core_remove is synchronous; keep wrapper async for future-proofing.
    _core_remove(job_id)


async def _finalize_session_commit(
    session, committed: bool, match_id: int
) -> None:
    """Commit the session if no inner commit occurred.

    This helper centralizes the awaitable vs non-awaitable handling so the
    primary job code stays small and focused.
    """
    try:
        if not committed:
            maybe = session.commit()
            # Await only if commit returned an awaitable (real AsyncSession)
            if inspect.isawaitable(maybe):
                await maybe
            try:
                setattr(session, "_committed", True)
            except Exception:
                logger.exception(
                    "Failed to set _committed on session for match %s",
                    match_id,
                )
    except Exception:
        logger.exception(
            "Failed to commit session after processing match %s", match_id
        )
        raise
