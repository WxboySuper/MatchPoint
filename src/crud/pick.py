import logging
from typing import List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select
from src.models import Pick
from .base import _save_and_refresh, _delete_and_commit

logger = logging.getLogger(__name__)


@dataclass
class PickCreateParams:
    user_id: int
    contest_id: int
    match_id: int
    chosen_team: str
    timestamp: Optional[datetime] = None


# Allow this function to have multiple explicit args for clarity
# despite lint rules
def create_pick(session: Session, params: PickCreateParams) -> Pick:
    """
    Create and persist a Pick for a user in a contest match.

    Parameters:
        params (PickCreateParams): Parameter object containing
            `user_id`, `contest_id`, `match_id`, `chosen_team`, and optional
            `timestamp`.

    Returns:
        Pick: The persisted Pick instance with database-populated
            fields (for example, `id`) refreshed.
    """
    logger.info(
        "Creating pick for user %s, match %s, team %s",
        params.user_id,
        params.match_id,
        params.chosen_team,
    )
    pick_args = dict(
        user_id=params.user_id,
        contest_id=params.contest_id,
        match_id=params.match_id,
        chosen_team=params.chosen_team,
    )
    if params.timestamp is not None:
        pick_args["timestamp"] = params.timestamp
    pick = Pick(**pick_args)
    _save_and_refresh(session, pick)
    logger.info("Created pick with ID: %s", pick.id)
    return pick


def _update_pick_if_changed(
    session: Session, pick: Pick, new_team: str
) -> Pick:
    """
    Update the chosen team for a pick if it differs from the current value.

    Parameters:
        session (Session): Database session.
        pick (Pick): The existing Pick object.
        new_team (str): The new team name.

    Returns:
        Pick: The updated (or unchanged) Pick object.
    """
    if pick.chosen_team != new_team:
        pick.chosen_team = new_team
        _save_and_refresh(session, pick)
    return pick


def _handle_integrity_error(
    session: Session, params: PickCreateParams
) -> Pick:
    """
    Handle IntegrityError by checking if the pick exists (race condition).

    If the pick was created concurrently, update and return it.
    Otherwise, re-raise the exception (e.g., foreign key violation).
    """
    stmt = select(Pick).where(
        Pick.user_id == params.user_id, Pick.match_id == params.match_id
    )
    existing_pick = session.exec(stmt).first()

    if existing_pick:
        logger.info(
            "Race condition detected/handled for user %s, match %s",
            params.user_id,
            params.match_id,
        )
        return _update_pick_if_changed(
            session, existing_pick, params.chosen_team
        )

    # Re-raise the original IntegrityError
    raise


def upsert_pick(session: Session, params: PickCreateParams) -> Pick:
    """
    Create or update a pick for a user in a contest match.
    Handles potential race conditions using the unique constraint.
    """
    # 1. Try to find existing pick first
    stmt = select(Pick).where(
        Pick.user_id == params.user_id, Pick.match_id == params.match_id
    )
    existing_pick = session.exec(stmt).first()

    if existing_pick:
        return _update_pick_if_changed(
            session, existing_pick, params.chosen_team
        )

    # 2. Try to create new pick
    try:
        return create_pick(session, params)
    except IntegrityError:
        session.rollback()
        # 3. Handle race condition
        return _handle_integrity_error(session, params)


def get_pick_by_id(session: Session, pick_id: int) -> Optional[Pick]:
    logger.debug("Fetching pick by ID: %s", pick_id)
    return session.get(Pick, pick_id)


def list_picks_for_user(session: Session, user_id: int) -> List[Pick]:
    logger.debug("Listing picks for user ID: %s", user_id)
    statement = select(Pick).where(Pick.user_id == user_id)
    return list(session.exec(statement))


def list_picks_for_match(session: Session, match_id: int) -> List[Pick]:
    logger.debug("Listing picks for match ID: %s", match_id)
    statement = (
        select(Pick)
        .options(selectinload(Pick.user))
        .where(Pick.match_id == match_id)
    )
    return list(session.exec(statement))


def update_pick(
    session: Session, pick_id: int, chosen_team: Optional[str] = None
) -> Optional[Pick]:
    """
    Update an existing Pick's chosen team.

    Parameters:
        session (Session): Database session used for the update.
        pick_id (int): Primary key of the Pick to update.
        chosen_team (Optional[str]): New team name to set; if None the
            field is left unchanged.

    Returns:
        Optional[Pick]: The updated Pick if found, otherwise None.
    """
    logger.info("Updating pick ID: %s", pick_id)
    pick = session.get(Pick, pick_id)
    if not pick:
        logger.warning("Pick with ID %s not found for update.", pick_id)
        return None
    if chosen_team is not None:
        pick.chosen_team = chosen_team
    _save_and_refresh(session, pick)
    logger.info("Updated pick ID: %s", pick_id)
    return pick


def delete_pick(session: Session, pick_id: int) -> bool:
    """
    Delete a Pick record identified by its primary key.

    Attempts to remove the Pick with the given id from the database and
    commit the change.

    Parameters:
        pick_id (int): Primary key of the Pick to delete.

    Returns:
        bool: `True` if the pick was deleted, `False` if no pick with
            the given id existed.
    """
    logger.info("Deleting pick ID: %s", pick_id)
    pick = session.get(Pick, pick_id)
    if not pick:
        logger.warning("Pick with ID %s not found for deletion.", pick_id)
        return False
    _delete_and_commit(session, pick)
    logger.info("Deleted pick ID: %s", pick_id)
    return True


def get_user_pick_stats(session: Session, user_id: int) -> Tuple[int, int]:
    """
    Get statistics for a user's picks.

    Parameters:
        session (Session): Database session.
        user_id (int): The ID of the user.

    Returns:
        Tuple[int, int]: A tuple containing (total_picks, correct_picks).
    """
    logger.debug("Fetching pick stats for user ID: %s", user_id)
    # Total picks
    total_query = select(func.count(Pick.id)).where(Pick.user_id == user_id)
    total_picks = session.scalar(total_query) or 0

    # Correct picks
    correct_query = select(func.count(Pick.id)).where(
        Pick.user_id == user_id, Pick.status == "correct"
    )
    correct_picks = session.scalar(correct_query) or 0

    return total_picks, correct_picks


def get_user_picks_for_matches(
    session: Session, user_id: int, match_ids: List[int]
) -> List[Pick]:
    """
    Fetch picks for a user restricted to a specific list of matches.

    Parameters:
        session (Session): Database session.
        user_id (int): The ID of the user.
        match_ids (List[int]): The IDs of the matches to fetch picks for.

    Returns:
        List[Pick]: A list of Pick objects for the specified matches.
    """
    logger.debug(
        "Fetching picks for user %s and matches %s", user_id, match_ids
    )
    if not match_ids:
        return []
    statement = select(Pick).where(
        Pick.user_id == user_id, Pick.match_id.in_(match_ids)
    )
    return list(session.exec(statement))
