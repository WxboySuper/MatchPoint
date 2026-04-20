from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlmodel import SQLModel, create_engine
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from src.crud.contest import (
    get_contest_by_pandascore_ids,
    upsert_contest_by_pandascore,
)
from src.parsers.lol import LoLParser


def test_lol_parser_extract_contest_data_includes_tier():
    parser = LoLParser()
    match_data = {
        "league": {
            "id": 1,
            "name": "LCS",
            "image_url": "http://img",
            "tier": "S-tier",
        },
        "serie": {
            "id": 10,
            "name": "Spring",
            "full_name": "Spring Split 2024",
        },
        "scheduled_at": "2024-03-15T10:00:00Z",
    }

    result = parser.extract_contest_data(match_data)
    assert result["pandascore_league_id"] == 1
    assert result["pandascore_serie_id"] == 10
    assert "LCS" in result["name"]
    assert "Spring" in result["name"]
    assert result["image_url"] == "http://img"
    assert result["tier"] == "S"


def test_lol_parser_extract_contest_data_uses_tournament_tier():
    parser = LoLParser()
    match_data = {
        "league": {
            "id": 4553,
            "name": "LCK Challengers League",
            "image_url": "https://cdn.pandascore.co/images/league/image.png",
        },
        "serie": {
            "id": 8889,
            "name": "Kickoff",
            "full_name": "Kickoff 2025",
        },
        "tournament": {
            "id": 15744,
            "name": "Playoffs",
            "tier": "c",
        },
        "scheduled_at": "2025-02-17T09:00:00Z",
    }

    result = parser.extract_contest_data(match_data)
    assert result["tier"] == "C"


@pytest.mark.asyncio
async def test_upsert_contest_by_pandascore_persists_tier(tmp_path):
    db_path = tmp_path / f"contest-tier-{uuid4().hex}.db"
    sync_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(sync_engine)
    async_engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")

    try:
        async with AsyncSession(async_engine) as session:
            contest = await upsert_contest_by_pandascore(
                session,
                {
                    "pandascore_league_id": 1,
                    "pandascore_serie_id": 2,
                    "name": "LCK Spring",
                    "start_date": datetime(
                        2026, 3, 14, 10, 0, tzinfo=timezone.utc
                    ),
                    "end_date": datetime(
                        2026, 3, 14, 10, 0, tzinfo=timezone.utc
                    ),
                    "tier": "S",
                },
            )
            assert contest is not None
            await session.commit()

        async with AsyncSession(async_engine) as session:
            persisted = await get_contest_by_pandascore_ids(
                session,
                1,
                2,
            )

        assert persisted is not None
        assert persisted.tier == "S"
    finally:
        sync_engine.dispose()
        await async_engine.dispose()
        if db_path.exists():
            db_path.unlink()
