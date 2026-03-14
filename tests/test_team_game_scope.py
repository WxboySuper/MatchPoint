from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.crud.contest import upsert_contest_by_pandascore
from src.crud.team import upsert_team_by_pandascore
from src.match_result_utils import fetch_teams
from src.models import Match, Team
from src.notification_batcher import _bulk_fetch_teams, _resolve_teams


@pytest_asyncio.fixture
async def db_session():
    db_path = Path.cwd() / f"test-team-game-scope-{uuid4().hex}.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    try:
        async with async_session() as session:
            yield session
    finally:
        await engine.dispose()
        if db_path.exists():
            db_path.unlink()


@pytest.mark.asyncio
async def test_upsert_team_by_pandascore_allows_same_name_across_games(
    db_session,
):
    db_session.add(Team(name="Misa Esports", pandascore_id=1, game="lol"))
    await db_session.commit()

    team = await upsert_team_by_pandascore(
        db_session,
        {
            "name": "Misa Esports",
            "pandascore_id": 137075,
            "acronym": "MISA",
            "game": "cs2",
        },
    )
    await db_session.commit()

    teams = (
        await db_session.exec(select(Team).where(Team.name == "Misa Esports"))
    ).all()

    assert team is not None
    assert {(row.game, row.pandascore_id) for row in teams} == {
        ("lol", 1),
        ("cs2", 137075),
    }


@pytest.mark.asyncio
async def test_upsert_team_by_pandascore_updates_same_game_name_match(
    db_session,
):
    db_session.add(
        Team(
            name="Misa Esports",
            pandascore_id=None,
            acronym="OLD",
            game="cs2",
        )
    )
    await db_session.commit()

    team = await upsert_team_by_pandascore(
        db_session,
        {
            "name": "Misa Esports",
            "pandascore_id": 137075,
            "acronym": "MISA",
            "game": "cs2",
        },
    )
    await db_session.commit()

    teams = (
        await db_session.exec(
            select(Team).where(
                Team.name == "Misa Esports",
                Team.game == "cs2",
            )
        )
    ).all()

    assert team is not None
    assert len(teams) == 1
    assert teams[0].pandascore_id == 137075
    assert teams[0].acronym == "MISA"


@pytest.mark.asyncio
async def test_team_upsert_failure_does_not_poison_session(db_session):
    with patch.object(
        db_session,
        "flush",
        new=AsyncMock(side_effect=[IntegrityError("dup", None, None), None]),
    ):
        team = await upsert_team_by_pandascore(
            db_session,
            {
                "name": "Misa Esports",
                "pandascore_id": 137075,
                "acronym": "MISA",
                "game": "cs2",
            },
        )
        contest = await upsert_contest_by_pandascore(
            db_session,
            {
                "pandascore_league_id": 5501,
                "pandascore_serie_id": 10273,
                "name": "BetBoom Storm Season 1 2026",
                "start_date": datetime.now(timezone.utc),
                "end_date": datetime.now(timezone.utc),
            },
        )

    assert team is None
    assert contest is not None
    assert contest.name == "BetBoom Storm Season 1 2026"


@pytest.mark.asyncio
async def test_game_scoped_team_lookups_return_matching_rows(db_session):
    lol_team = Team(name="Misa Esports", pandascore_id=1, game="lol")
    cs2_team = Team(name="Misa Esports", pandascore_id=2, game="cs2")
    other_team = Team(name="Other", pandascore_id=3, game="cs2")
    db_session.add(lol_team)
    db_session.add(cs2_team)
    db_session.add(other_team)
    await db_session.commit()

    cs2_match = Match(
        contest_id=1,
        team1="Misa Esports",
        team2="Other",
        team1_id=2,
        team2_id=3,
        game="cs2",
        scheduled_time=datetime.now(timezone.utc),
    )
    teams = await fetch_teams(db_session, cs2_match)
    teams_map = await _bulk_fetch_teams(db_session, [cs2_match])
    resolved = _resolve_teams(cs2_match, teams_map)

    assert teams[0] is not None
    assert teams[0].game == "cs2"
    assert teams[0].pandascore_id == 2
    assert resolved[0] is not None
    assert resolved[0].game == "cs2"
    assert resolved[1] is not None
    assert resolved[1].name == "Other"
