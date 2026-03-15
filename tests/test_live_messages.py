from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest
from sqlmodel import SQLModel, Session, create_engine

import src.live_messages as live_messages
from src.models import Contest, Match, Result


@dataclass(frozen=True)
class _RefreshScopeCase:
    guild_id: int
    channel_id: int
    message_id: int
    scope: str
    embed_title: str
    has_live_record: bool = False


@dataclass(frozen=True)
class _MatchQueryCase:
    fetcher: object
    game: str
    expected_ids: list[int]
    matches_to_create: list[Match]


async def _assert_refresh_scope(case: _RefreshScopeCase) -> None:
    guild = MagicMock()
    guild.id = case.guild_id
    guild.me = None

    channel = AsyncMock()
    channel.id = case.channel_id
    message = AsyncMock()
    message.id = case.message_id

    guild.get_channel.return_value = channel
    cfg = MagicMock(
        live_updates_channel_id=channel.id,
        announcement_channel_id=None,
        enabled_games="lol",
    )
    embed = discord.Embed(title=case.embed_title)
    live_record = None
    if case.has_live_record:
        channel.fetch_message.return_value = message
        live_record = MagicMock(
            channel_id=channel.id,
            message_id=case.message_id,
        )
    else:
        channel.send.return_value = message

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
                scope=case.scope,
            ),
        )

    if case.has_live_record:
        message.edit.assert_awaited_once_with(embed=embed)
        channel.send.assert_not_called()
    else:
        channel.send.assert_awaited_once_with(embed=embed)

    mock_persist.assert_awaited_once_with(
        live_messages.LiveMessageScope(
            guild=guild,
            game="lol",
            scope=case.scope,
        ),
        channel.id,
        message.id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        _RefreshScopeCase(
            guild_id=111,
            channel_id=222,
            message_id=333,
            scope="upcoming",
            embed_title="Upcoming",
        ),
        _RefreshScopeCase(
            guild_id=222,
            channel_id=444,
            message_id=555,
            scope="running",
            embed_title="Running",
            has_live_record=True,
        ),
    ],
)
async def test_refresh_scope_behaviors(case):
    await _assert_refresh_scope(case)


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
    async_session_for_engine, case: _MatchQueryCase
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

        for match in case.matches_to_create:
            match.contest_id = contest.id
            session.add(match)
        session.commit()

    with patch.object(
        live_messages,
        "get_async_session",
        return_value=async_session_for_engine(engine),
    ):
        matches = await case.fetcher(case.game)

    assert [match.pandascore_id for match in matches] == case.expected_ids


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        _MatchQueryCase(
            fetcher=live_messages._fetch_upcoming_matches,
            game="lol",
            expected_ids=[10],
            matches_to_create=[
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
        ),
        _MatchQueryCase(
            fetcher=live_messages._fetch_upcoming_matches,
            game="lol",
            expected_ids=[20],
            matches_to_create=[
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
        ),
        _MatchQueryCase(
            fetcher=live_messages._fetch_running_matches,
            game="lol",
            expected_ids=[30],
            matches_to_create=[
                Match(
                    pandascore_id=30,
                    team1="BLG",
                    team2="TES",
                    status="live",
                    game="league-of-legends",
                    scheduled_time=datetime.now(timezone.utc),
                )
            ],
        ),
    ],
)
async def test_match_query_behaviors(async_session_for_engine, case):
    await _assert_match_query_ids(async_session_for_engine, case)
