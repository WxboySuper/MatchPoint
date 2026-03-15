import logging
from typing import List, Optional
from datetime import datetime
from dataclasses import dataclass
from sqlmodel import Session, select
from sqlmodel.ext.asyncio.session import AsyncSession
from src.models import Contest
from .sync_utils import _upsert_by_leaguepedia
from .base import _save_and_refresh, _delete_and_commit

logger = logging.getLogger(__name__)


@dataclass
class ContestUpdateParams:
    name: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    tier: Optional[str] = None


async def upsert_contest(
    session: AsyncSession, contest_data: dict
) -> Optional[Contest]:
    """
    Create or update a Contest using its Leaguepedia identifier.

    The function upserts a Contest by `leaguepedia_id`, creating a new
    record if none exists or updating the existing record. Only
    `name`, `start_date`, `end_date`, and `tier` are considered for
    updates.

    Parameters:
        contest_data (dict): Mapping of contest fields. Must include
            `leaguepedia_id`. May include `name`, `start_date`,
            `end_date`, and `tier`.

    Returns:
        Contest or None: The created or updated Contest, or `None` if
            the upsert failed or `leaguepedia_id` was missing.
    """
    return await _upsert_by_leaguepedia(
        session,
        Contest,
        contest_data,
        update_keys=["name", "start_date", "end_date", "tier"],
    )


async def upsert_contest_by_pandascore(
    session: AsyncSession, contest_data: dict
) -> Optional[Contest]:
    """
    Create or update a Contest using PandaScore identifiers.

    The function upserts a Contest by `pandascore_league_id` and
    `pandascore_serie_id`, creating a new record if none exists or
    updating the existing record.

    Parameters:
        contest_data (dict): Mapping of contest fields. Must include
            `pandascore_league_id` and `pandascore_serie_id`. May include
            `name`, `start_date`, `end_date`, and `tier`.

    Returns:
        Contest or None: The created or updated Contest, or None if
            the upsert failed.
    """
    league_id = contest_data.get("pandascore_league_id")
    serie_id = contest_data.get("pandascore_serie_id")

    missing = []
    if league_id is None:
        missing.append("pandascore_league_id")
    if serie_id is None:
        missing.append("pandascore_serie_id")
    if missing:
        logger.error(
            "Missing required PandaScore IDs in data: %s", ", ".join(missing)
        )
        return None

    try:
        async with session.begin_nested():
            contest = await get_contest_by_pandascore_ids(
                session, league_id, serie_id
            )

            if contest:
                _update_contest_from_data(contest, contest_data)
            else:
                contest = _create_contest_from_data(contest_data)

            session.add(contest)
            await session.flush()
        logger.info("Upserted contest: %s (ID: %s)", contest.name, contest.id)
        return contest
    except Exception:
        logger.exception("Error upserting contest with data: %s", contest_data)
        return None


def _update_contest_from_data(contest: Contest, contest_data: dict) -> None:
    """Updates existing contest fields from data."""
    logger.info("Updating existing contest: %s", contest.name)
    for key in ["name", "start_date", "end_date", "image_url", "tier"]:
        if key in contest_data and contest_data[key] is not None:
            setattr(contest, key, contest_data[key])


def _create_contest_from_data(contest_data: dict) -> Contest:
    """Creates a new contest instance from data."""
    logger.info("Creating new contest: %s", contest_data.get("name"))
    return Contest(**contest_data)


async def get_contest_by_pandascore_ids(
    session: AsyncSession, league_id: int, serie_id: int
) -> Optional[Contest]:
    """
    Fetch a contest by its PandaScore league and serie IDs.

    Parameters:
        league_id: The PandaScore league ID
        serie_id: The PandaScore serie ID

    Returns:
        Optional[Contest]: The Contest if found, None otherwise
    """
    result = await session.exec(
        select(Contest).where(
            Contest.pandascore_league_id == league_id,
            Contest.pandascore_serie_id == serie_id,
        )
    )
    return result.first()


def create_contest(session: Session, contest_data: dict) -> Contest:
    """
    Create a Contest record from a dictionary of contest fields.

    Parameters:
        contest_data (dict): Dictionary with contest attributes. Expected keys:
            - "name" (str): Contest name.
            - "start_date" (datetime): Contest start date/time.
            - "end_date" (datetime): Contest end date/time.
            - "leaguepedia_id" (str): External leaguepedia identifier.

    Returns:
        Contest: The persisted Contest instance with
            database-generated fields (e.g., `id`) populated.
    """
    name = contest_data.get("name")
    start_date = contest_data.get("start_date")
    end_date = contest_data.get("end_date")
    leaguepedia_id = contest_data.get("leaguepedia_id")
    tier = contest_data.get("tier")

    logger.info("Creating contest: %s", name)
    contest = Contest(
        name=name,
        start_date=start_date,
        end_date=end_date,
        leaguepedia_id=leaguepedia_id,
        tier=tier,
    )
    _save_and_refresh(session, contest)
    logger.info("Created contest with ID: %s", contest.id)
    return contest


def get_contest_by_id(session: Session, contest_id: int) -> Optional[Contest]:
    logger.debug("Fetching contest by ID: %s", contest_id)
    return session.get(Contest, contest_id)


def list_contests(session: Session) -> List[Contest]:
    logger.debug("Listing all contests")
    return list(session.exec(select(Contest)))


def update_contest(
    session: Session, contest_id: int, params: ContestUpdateParams
) -> Optional[Contest]:
    """
    Update fields of an existing Contest using values provided in `params`.

    Parameters:
        contest_id (int): Primary key of the Contest to update.
        params (ContestUpdateParams): Dataclass containing optional
            fields (`name`, `start_date`, `end_date`) to apply; only
            non-None fields are written.

    Returns:
        Contest | None: The updated Contest if found and modified,
            `None` if no Contest with the given `contest_id` exists.
    """
    logger.info("Updating contest ID: %s", contest_id)
    contest = session.get(Contest, contest_id)
    if not contest:
        logger.warning("Contest with ID %s not found for update.", contest_id)
        return None
    if params.name is not None:
        contest.name = params.name
    if params.start_date is not None:
        contest.start_date = params.start_date
    if params.end_date is not None:
        contest.end_date = params.end_date
    if params.tier is not None:
        contest.tier = params.tier
    _save_and_refresh(session, contest)
    logger.info("Updated contest ID: %s", contest_id)
    return contest


def delete_contest(session: Session, contest_id: int) -> bool:
    """
    Delete the contest with the given ID from the database.

    Parameters:
        contest_id (int): Primary key of the contest to delete.

    Returns:
        bool: True if the contest was found and deleted, False otherwise.
    """
    logger.info("Deleting contest ID: %s", contest_id)
    contest = session.get(Contest, contest_id)
    if not contest:
        logger.warning(
            "Contest with ID %s not found for deletion.", contest_id
        )
        return False
    _delete_and_commit(session, contest)
    logger.info("Deleted contest ID: %s", contest_id)
    return True
