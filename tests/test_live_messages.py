from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest
from sqlmodel import SQLModel, Session, create_engine

import src.live_messages as live_messages
from src.models import Contest, Match, Result


@pytest.mark.asyncio
async def test_refresh_scope_creates_missing_live_message():
    guild = MagicMock()
    guild.id = 111
    guild.me = None

    channel = AsyncMock()
    channel.id = 222
    sent_message = AsyncMock()
    sent_message.id = 333
    channel.send.return_value = sent_message

    guild.get_channel.return_value = channel
    cfg = MagicMock(
        live_updates_channel_id=channel.id,
        announcement_channel_id=None,
        enabled_games="lol",
    )
    embed = discord.Embed(title="Upcoming")

    with patch.object(
        live_messages,
        "_load_live_record",
        new_callable=AsyncMock,
        return_value=None,
    ), patch.object(
        live_messages,
        "_build_live_message_embed",
        new_callable=AsyncMock,
        return_value=embed,
    ), patch.object(
        live_messages,
        "_persist_live_message_pointer",
        new_callable=AsyncMock,
    ) as mock_persist:
        await live_messages._refresh_guild_scope(
            cfg,
            live_messages.LiveMessageScope(
                guild=guild,
                game="lol",
                scope="upcoming",
            ),
        )

    channel.send.assert_awaited_once_with(embed=embed)
    mock_persist.assert_awaited_once_with(
        live_messages.LiveMessageScope(
            guild=guild,
            game="lol",
            scope="upcoming",
        ),
        channel.id,
        sent_message.id,
    )


@pytest.mark.asyncio
async def test_refresh_scope_edits_existing_live_message():
    guild = MagicMock()
    guild.id = 222
    guild.me = None

    channel = AsyncMock()
    channel.id = 444
    fetched_message = AsyncMock()
    fetched_message.id = 555
    channel.fetch_message.return_value = fetched_message

    guild.get_channel.return_value = channel
    cfg = MagicMock(
        live_updates_channel_id=channel.id,
        announcement_channel_id=None,
        enabled_games="lol",
    )
    live_record = MagicMock(channel_id=channel.id, message_id=555)
    embed = discord.Embed(title="Running")

    with patch.object(
        live_messages,
        "_load_live_record",
        new_callable=AsyncMock,
        return_value=live_record,
    ), patch.object(
        live_messages,
        "_build_live_message_embed",
        new_callable=AsyncMock,
        return_value=embed,
    ), patch.object(
        live_messages,
        "_persist_live_message_pointer",
        new_callable=AsyncMock,
    ) as mock_persist:
        await live_messages._refresh_guild_scope(
            cfg,
            live_messages.LiveMessageScope(
                guild=guild,
                game="lol",
                scope="running",
            ),
        )

    fetched_message.edit.assert_awaited_once_with(embed=embed)
    channel.send.assert_not_called()
    mock_persist.assert_awaited_once_with(
        live_messages.LiveMessageScope(
            guild=guild,
            game="lol",
            scope="running",
        ),
        channel.id,
        live_record.message_id,
    )


def test_build_running_embed_keeps_empty_state_message():
    embed = live_messages._build_running_embed("lol", [])

    assert embed.title == "LoL Live Matches"
    assert embed.description == "No matches are currently live."


def test_build_upcoming_embed_uses_count_based_empty_state_message():
    embed = live_messages._build_upcoming_embed("cs2", [])

    assert embed.title == "CS2 Upcoming Matches"
    assert embed.description == "No upcoming matches are scheduled."


@pytest.mark.asyncio
async def test_fetch_running_matches_excludes_finished_results(
    async_session_for_engine,
):
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        contest = Contest(
            pandascore_league_id=1,
            pandascore_serie_id=2,
            name="LCS Spring",
            start_date=datetime.now(timezone.utc),
            end_date=datetime.now(timezone.utc),
        )
        session.add(contest)
        session.commit()
        session.refresh(contest)

        running_match = Match(
            contest_id=contest.id,
            pandascore_id=1,
            team1="A",
            team2="B",
            status="running",
            game="lol",
            scheduled_time=datetime.now(timezone.utc),
        )
        stale_match = Match(
            contest_id=contest.id,
            pandascore_id=2,
            team1="C",
            team2="D",
            status="running",
            game="lol",
            scheduled_time=datetime.now(timezone.utc),
        )
        session.add(running_match)
        session.add(stale_match)
        session.commit()
        session.refresh(running_match)
        session.refresh(stale_match)

        session.add(
            Result(
                match_id=stale_match.id,
                winner="C",
                score="2-0",
            )
        )
        session.commit()

    with patch.object(
        live_messages,
        "get_async_session",
        return_value=async_session_for_engine(engine),
    ):
        matches = await live_messages._fetch_running_matches("lol")

    assert [match.pandascore_id for match in matches] == [1]


async def _assert_match_query_ids(
    async_session_for_engine,
    matches_to_create,
    fetcher,
    game: str,
    expected_ids: list[int],
) -> None:
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        contest = Contest(
            pandascore_league_id=10,
            pandascore_serie_id=20,
            name="Test Contest",
            start_date=datetime.now(timezone.utc),
            end_date=datetime.now(timezone.utc),
        )
        session.add(contest)
        session.commit()
        session.refresh(contest)

        for match in matches_to_create:
            match.contest_id = contest.id
            session.add(match)
        session.commit()

    with patch.object(
        live_messages,
        "get_async_session",
        return_value=async_session_for_engine(engine),
    ):
        matches = await fetcher(game)

    assert [match.pandascore_id for match in matches] == expected_ids


@pytest.mark.asyncio
async def test_fetch_upcoming_matches_includes_future_scheduled_statuses(
    async_session_for_engine,
):
    await _assert_match_query_ids(
        async_session_for_engine,
        [
            Match(
                pandascore_id=10,
                team1="Alpha",
                team2="Beta",
                status="scheduled",
                game="lol",
                scheduled_time=datetime.now(timezone.utc)
                + timedelta(hours=1),
            ),
            Match(
                pandascore_id=11,
                team1="Gamma",
                team2="Delta",
                status="finished",
                game="lol",
                scheduled_time=datetime.now(timezone.utc)
                + timedelta(hours=2),
            ),
        ],
        live_messages._fetch_upcoming_matches,
        "lol",
        [10],
    )


@pytest.mark.asyncio
async def test_fetch_upcoming_matches_includes_legacy_lol_slug_rows(
    async_session_for_engine,
):
    await _assert_match_query_ids(
        async_session_for_engine,
        [
            Match(
                pandascore_id=20,
                team1="T1",
                team2="GEN",
                status="not_started",
                game="league-of-legends",
                scheduled_time=datetime.now(timezone.utc)
                + timedelta(hours=1),
            )
        ],
        live_messages._fetch_upcoming_matches,
        "lol",
        [20],
    )


@pytest.mark.asyncio
async def test_fetch_running_matches_accepts_live_status_aliases(
    async_session_for_engine,
):
    await _assert_match_query_ids(
        async_session_for_engine,
        [
            Match(
                pandascore_id=30,
                team1="BLG",
                team2="TES",
                status="live",
                game="league-of-legends",
                scheduled_time=datetime.now(timezone.utc),
            )
        ],
        live_messages._fetch_running_matches,
        "lol",
        [30],
    )
