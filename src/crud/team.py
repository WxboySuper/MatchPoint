import logging
from typing import Optional
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from src.models import Team
from .sync_utils import _upsert_by_leaguepedia

logger = logging.getLogger(__name__)


async def upsert_team(
    session: AsyncSession, team_data: dict
) -> Optional[Team]:
    """
    Create or update a Team using its `leaguepedia_id`.

    Parameters:
        team_data (dict): Mapping containing team fields. Must include
            `leaguepedia_id`; other keys may include `name`,
            `image_url`, `roster`, and any other Team fields to set.

    Returns:
        team (Optional[Team]): The created or updated Team instance,
            or `None` if `leaguepedia_id` is missing or an error
            occurred during upsert.
    """
    return await _upsert_by_leaguepedia(
        session,
        Team,
        team_data,
        update_keys=["name", "image_url", "roster"],
    )


async def upsert_team_by_pandascore(
    session: AsyncSession, team_data: dict
) -> Optional[Team]:
    """
    Create or update a Team using its PandaScore ID.

    Parameters:
        team_data (dict): Mapping containing team fields. Must include
            `pandascore_id`; other keys may include `name`, `acronym`,
            `image_url`, and other Team fields.

    Returns:
        Optional[Team]: The created or updated Team instance,
            or None if pandascore_id is missing or an error occurred.
    """
    pandascore_id = team_data.get("pandascore_id")
    game = _resolved_team_game(team_data)
    name = team_data.get("name")
    if pandascore_id is None:
        logger.error("Missing pandascore_id in team_data")
        return None
    if not name:
        logger.error("Missing team name in team_data: %s", team_data)
        return None

    try:
        async with session.begin_nested():
            team = await _find_team_by_pandascore_or_name(session, team_data)

            if team:
                _log_existing_team_match(team, team_data)
                _update_team_from_data(team, team_data)
            else:
                team = _create_team_from_data(team_data)

            session.add(team)
            await session.flush()
        logger.info("Upserted team: %s (ID: %s)", team.name, team.id)
        return team
    except Exception:
        logger.exception(
            "Error upserting team with game=%s name=%s pandascore_id=%s",
            game,
            name,
            pandascore_id,
        )
        return None


async def _find_team_by_pandascore_or_name(
    session: AsyncSession, team_data: dict
) -> Optional[Team]:
    """Find a team by PandaScore ID or exact same-game name."""
    game = _resolved_team_game(team_data)
    pandascore_id = team_data.get("pandascore_id")
    if pandascore_id is not None:
        team = await _get_team_by_pandascore(session, pandascore_id, game)
        if team:
            return team

    name = team_data.get("name")
    if not name:
        return None
    return await _get_team_by_name(session, name, game)


async def _get_team_by_pandascore(
    session: AsyncSession, pandascore_id: int, game: str
) -> Optional[Team]:
    result = await session.exec(
        select(Team).where(
            Team.pandascore_id == pandascore_id,
            Team.game == game,
        )
    )
    return result.first()


async def _get_team_by_name(
    session: AsyncSession, name: str, game: str
) -> Optional[Team]:
    query = select(Team).where(Team.name == name, Team.game == game)
    result = await session.exec(query)
    return result.first()


def _update_team_from_data(team: Team, team_data: dict) -> None:
    """Updates existing team fields from data."""
    logger.info("Updating existing team: %s", team.name)
    for key in ["name", "acronym", "image_url", "pandascore_id", "game"]:
        if key in team_data and team_data[key] is not None:
            setattr(team, key, team_data[key])


def _create_team_from_data(team_data: dict) -> Team:
    """Creates a new team instance from data."""
    logger.info("Creating new team: %s", team_data.get("name"))
    return Team(**team_data)


async def get_team_by_pandascore_id(
    session: AsyncSession,
    pandascore_id: int,
    game: Optional[str] = None,
) -> Optional[Team]:
    """
    Fetch a team by its PandaScore ID.

    Parameters:
        pandascore_id: The PandaScore team ID

    Returns:
        Optional[Team]: The Team if found, None otherwise
    """
    stmt = select(Team).where(Team.pandascore_id == pandascore_id)
    if game:
        stmt = stmt.where(Team.game == game)
    result = await session.exec(stmt)
    return result.first()


def _resolved_team_game(team_data: dict) -> str:
    return str(team_data.get("game") or "lol").strip().lower() or "lol"


def _log_existing_team_match(team: Team, team_data: dict) -> None:
    incoming_id = team_data.get("pandascore_id")
    incoming_game = _resolved_team_game(team_data)
    if team.pandascore_id == incoming_id:
        return
    logger.warning(
        "Matched existing team row id=%s for game=%s name=%s "
        "(existing pandascore_id=%s, incoming pandascore_id=%s)",
        team.id,
        incoming_game,
        team.name,
        team.pandascore_id,
        incoming_id,
    )
