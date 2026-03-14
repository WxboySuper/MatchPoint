import logging
from typing import List, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass
from sqlmodel import Session, select
from sqlalchemy.orm import selectinload
from sqlmodel.ext.asyncio.session import AsyncSession
from src.models import Match
from .base import _save_and_refresh, _save_all_and_refresh, _delete_and_commit

logger = logging.getLogger(__name__)


@dataclass
class MatchCreateParams:
    contest_id: int
    team1: str
    team2: str
    scheduled_time: datetime
    leaguepedia_id: str


@dataclass
class MatchUpdateParams:
    team1: Optional[str] = None
    team2: Optional[str] = None
    scheduled_time: Optional[datetime] = None


async def upsert_match(
    session: AsyncSession, match_data: dict
) -> Tuple[Optional[Match], bool]:
    """
    Create or update a Match identified by its `leaguepedia_id`.

    Parameters:
        match_data (dict): Mapping with match fields. Must include
            `leaguepedia_id`, `team1`, `team2`, and `scheduled_time`.
            May include `best_of` and other Match fields.

    Returns:
        tuple[Optional[Match], bool]: A tuple containing the upserted
            Match (or `None` on failure) and a boolean that is `true`
            if the match's scheduled time changed or the match was
            newly created, `false` otherwise.
    """
    leaguepedia_id = match_data.get("leaguepedia_id")
    if not leaguepedia_id:
        logger.error("Missing leaguepedia_id in match_data")
        return None, False

    try:
        async with session.begin_nested():
            existing_match = await session.exec(
                select(Match)
                .where(Match.leaguepedia_id == leaguepedia_id)
                .options(selectinload(Match.result))
            )
            match = existing_match.first()
            time_changed = False

            if match:
                # Update existing match
                logger.info("Updating existing match ID: %s", match.id)
                match.team1 = match_data["team1"]
                match.team2 = match_data["team2"]
                match.best_of = match_data.get("best_of")
                original_time = match.scheduled_time
                new_time = match_data["scheduled_time"]
                if original_time != new_time:
                    logger.info(
                        "Match %s time changed from %s to %s",
                        match.id,
                        original_time,
                        new_time,
                    )
                    time_changed = True
                match.scheduled_time = new_time
            else:
                # Create new match
                logger.info("Creating new match: %s", match_data)
                match = Match(**match_data)
                time_changed = True  # It's a new match, so schedule it

            session.add(match)
            await session.flush()  # Flush to get the match.id if it's new
        logger.info("Upserted match ID: %s", match.id)

        return match, time_changed
    except KeyError as e:
        logger.error("Missing key in match_data: %s", e)
        return None, False
    except Exception:
        logger.exception("Error upserting match with data: %s", match_data)
        return None, False


async def upsert_match_by_pandascore(
    session: AsyncSession, match_data: dict
) -> Tuple[Optional[Match], bool, bool, Optional[datetime]]:
    """
    Create or update a Match identified by its PandaScore ID.

    Parameters:
        match_data (dict): Mapping with match fields. Must include
            `pandascore_id`, `team1`, `team2`, and `scheduled_time`.
            May include `best_of`, `status`, `team1_id`, `team2_id`.

    Returns:
        tuple[Optional[Match], bool, bool, Optional[datetime]]:
        A tuple containing:
            1. The upserted Match (or None on failure)
            2. A boolean indicating if the match is newly created
            3. A boolean indicating if the match's scheduled time changed
            4. The original scheduled time (if updated) or None
    """
    pandascore_id = match_data.get("pandascore_id")
    if pandascore_id is None:
        logger.error("Missing pandascore_id in match_data")
        return None, False, False, None

    try:
        async with session.begin_nested():
            result = await session.exec(
                select(Match)
                .where(Match.pandascore_id == pandascore_id)
                .options(selectinload(Match.result))
            )
            match = result.first()
            is_new = False
            original_time = None

            if match:
                time_changed, original_time = _update_match_from_data(
                    match, match_data
                )
            else:
                match, time_changed = _create_match_from_data(match_data)
                is_new = True

            session.add(match)
            await session.flush()
        logger.info(
            "Upserted match ID: %s (PandaScore: %s)", match.id, pandascore_id
        )

        return match, is_new, time_changed, original_time
    except KeyError as e:
        logger.error("Missing key in match_data: %s", e)
        return None, False, False, None
    except Exception:
        logger.exception("Error upserting match with data: %s", match_data)
        return None, False, False, None


def _update_match_from_data(
    match: Match, match_data: dict
) -> Tuple[bool, Optional[datetime]]:
    """
    Updates existing match fields and returns
    (time_changed, original_time).
    """
    logger.info(
        "Updating existing match (PandaScore ID: %s)", match.pandascore_id
    )
    for key in [
        "team1",
        "team2",
        "team1_id",
        "team2_id",
        "best_of",
        "status",
        "game",
    ]:
        if key in match_data and match_data[key] is not None:
            setattr(match, key, match_data[key])

    original_time = match.scheduled_time
    new_time = match_data.get("scheduled_time")
    time_changed = False
    if new_time and original_time != new_time:
        logger.info(
            "Match %s time changed from %s to %s",
            match.id,
            original_time,
            new_time,
        )
        match.scheduled_time = new_time
        time_changed = True

    return time_changed, original_time


def _create_match_from_data(match_data: dict) -> Tuple[Match, bool]:
    """Creates a new match instance from data."""
    logger.info(
        "Creating new match (PandaScore ID: %s): %s vs %s",
        match_data.get("pandascore_id"),
        match_data.get("team1"),
        match_data.get("team2"),
    )
    return Match(**match_data), True


async def get_match_by_pandascore_id(
    session: AsyncSession, pandascore_id: int
) -> Optional[Match]:
    """
    Fetch a match by its PandaScore ID.

    Parameters:
        pandascore_id: The PandaScore match ID

    Returns:
        Optional[Match]: The Match if found, None otherwise
    """
    result = await session.exec(
        select(Match)
        .where(Match.pandascore_id == pandascore_id)
        .options(selectinload(Match.result), selectinload(Match.contest))
    )
    return result.first()


def create_match(session: Session, params: MatchCreateParams) -> Match:
    """
    Create a Match from the given MatchCreateParams and persist it
    to the database.

    Parameters:
        params (MatchCreateParams): Parameter object containing
            contest_id, team1, team2, scheduled_time, and
            leaguepedia_id used to construct the Match.

    Returns:
        Match: The persisted Match instance with database-generated
            fields (e.g., `id`) populated.
    """
    logger.info(
        "Creating match: %s vs %s for contest %s",
        params.team1,
        params.team2,
        params.contest_id,
    )
    match = Match(
        contest_id=params.contest_id,
        team1=params.team1,
        team2=params.team2,
        scheduled_time=params.scheduled_time,
        leaguepedia_id=params.leaguepedia_id,
    )
    _save_and_refresh(session, match)
    logger.info("Created match with ID: %s", match.id)
    return match


def bulk_create_matches(
    session: Session, matches_data: List[dict]
) -> List[Match]:
    """
    Create multiple Match records from a list of dictionaries and
    persist them.

    Parameters:
        session (Session): Database session used to persist matches.
        matches_data (List[dict]): List of dictionaries with keys
            required to initialize a `Match` (for example:
            `contest_id`, `team1`, `team2`, `scheduled_time`,
            `leaguepedia_id`).

    Returns:
        List[Match]: The created and refreshed `Match` instances.
    """
    logger.info("Bulk creating %s matches", len(matches_data))
    matches = [Match(**data) for data in matches_data]
    _save_all_and_refresh(session, matches)
    logger.info("Bulk created matches.")
    return matches


def get_matches_by_date(session: Session, date: datetime) -> List[Match]:
    logger.debug("Fetching matches for date: %s", date.strftime("%Y-%m-%d"))
    start = datetime(date.year, date.month, date.day, tzinfo=timezone.utc)
    end = datetime(
        date.year, date.month, date.day, 23, 59, 59, tzinfo=timezone.utc
    )
    statement = (
        select(Match)
        .where(Match.scheduled_time >= start)
        .where(Match.scheduled_time <= end)
        .options(selectinload(Match.result), selectinload(Match.contest))
        .order_by(Match.scheduled_time)
    )
    return list(session.exec(statement))


def list_matches_for_contest(session: Session, contest_id: int) -> List[Match]:
    logger.debug("Listing matches for contest ID: %s", contest_id)
    stmt = (
        select(Match)
        .where(Match.contest_id == contest_id)
        .options(selectinload(Match.result), selectinload(Match.contest))
        .order_by(Match.scheduled_time)
    )
    return list(session.exec(stmt))


async def get_match_with_result_by_id(
    session: AsyncSession, match_id: int
) -> Optional[Match]:
    """
    Fetches a match by its ID, eagerly loading the related result and contest.
    """
    logger.debug("Fetching match with result by ID: %s", match_id)
    result = await session.exec(
        select(Match)
        .where(Match.id == match_id)
        .options(selectinload(Match.result), selectinload(Match.contest))
    )
    return result.first()


def get_match_by_id(session: Session, match_id: int) -> Optional[Match]:
    logger.debug("Fetching match by ID: %s", match_id)
    return session.get(Match, match_id)


def list_all_matches(session: Session) -> List[Match]:
    """Returns all matches, sorted by most recent first, with results
    loaded."""
    logger.debug("Listing all matches")
    return list(
        session.exec(
            select(Match)
            .options(selectinload(Match.result))
            .order_by(Match.scheduled_time.desc())
        )
    )


def update_match(
    session: Session, match_id: int, params: MatchUpdateParams
) -> Optional[Match]:
    """
    Apply provided match update fields to an existing Match and persist the
    changes.

    Parameters:
        params (MatchUpdateParams): Update container whose non-None
            fields (team1, team2, scheduled_time) will be applied to the match.

    Returns:
        Updated Match if a match with the given id was found and
        updated, `None` if no such match exists.
    """
    logger.info("Updating match ID: %s", match_id)
    match = session.get(Match, match_id)
    if not match:
        logger.warning("Match with ID %s not found for update.", match_id)
        return None
    if params.team1 is not None:
        match.team1 = params.team1
    if params.team2 is not None:
        match.team2 = params.team2
    if params.scheduled_time is not None:
        match.scheduled_time = params.scheduled_time
    _save_and_refresh(session, match)
    logger.info("Updated match ID: %s", match_id)
    return match


def delete_match(session: Session, match_id: int) -> bool:
    """
    Delete the Match with the given primary key and commit the change.

    Deletes the matching Match record from the database and commits the
    transaction.

    Parameters:
        match_id (int): Primary key of the Match to delete.

    Returns:
        bool: `True` if the match was deleted, `False` if no match with
            the given id was found.
    """
    logger.info("Deleting match ID: %s", match_id)
    match = session.get(Match, match_id)
    if not match:
        logger.warning("Match with ID %s not found for deletion.", match_id)
        return False
    _delete_and_commit(session, match)
    logger.info("Deleted match ID: %s", match_id)
    return True
