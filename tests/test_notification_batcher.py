import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
import src.notification_batcher
from src.notification_batcher import NotificationBatcher
from src.models import Match, Contest


@pytest.mark.asyncio
async def test_batch_reminders():
    # Setup
    batcher = NotificationBatcher()

    # Mocks
    mock_bot = MagicMock()
    mock_session = AsyncMock()

    with patch.object(
        src.notification_batcher, "get_bot_instance", return_value=mock_bot
    ), patch.object(
        src.notification_batcher, "get_async_session"
    ) as mock_batcher_session_cls, patch.object(
        src.notification_batcher,
        "refresh_live_messages_for_games",
        new_callable=AsyncMock,
    ) as mock_refresh, patch.object(
        src.notification_batcher, "_bulk_fetch_matches", new_callable=AsyncMock
    ) as mock_bulk_matches, patch.object(
        src.notification_batcher, "_bulk_fetch_teams", new_callable=AsyncMock
    ) as mock_bulk_teams, patch.object(
        src.notification_batcher, "_resolve_teams"
    ) as mock_resolve_teams:

        mock_batcher_session_cls.return_value.__aenter__.return_value = (
            mock_session
        )

        now = datetime.now(timezone.utc)
        contest = Contest(
            name="C1",
            image_url="http://example.com/icon.png",
            start_date=now,
            end_date=now,
        )
        match1 = Match(
            id=1,
            team1="Team A",
            team2="Team B",
            scheduled_time=now,
            contest_id=1,
        )
        match1.contest = contest
        match2 = Match(
            id=2,
            team1="Team C",
            team2="Team D",
            scheduled_time=now,
            contest_id=1,
        )
        match2.contest = contest
        match3 = Match(
            id=3,
            team1="Team E",
            team2="Team F",
            scheduled_time=now,
            contest_id=1,
            game="cs2",
        )
        match3.contest = MagicMock(image_url=None)

        mock_bulk_matches.return_value = [match1, match2, match3]
        mock_bulk_teams.return_value = {}  # Mock dict
        mock_resolve_teams.return_value = (None, None)

        # Action: Add reminders
        await batcher.add_reminder(1, 5)
        await batcher.add_reminder(2, 5)

        assert len(batcher._pending["reminder_5"]) == 2
        await asyncio.sleep(1.1)

        mock_refresh.assert_awaited_once_with({"lol", "cs2"})


@pytest.mark.asyncio
async def test_batch_results():
    batcher = NotificationBatcher()
    mock_bot = MagicMock()
    mock_session = AsyncMock()

    with patch.object(
        src.notification_batcher, "get_bot_instance", return_value=mock_bot
    ), patch.object(
        src.notification_batcher, "get_async_session"
    ) as mock_batcher_session_cls, patch.object(
        src.notification_batcher,
        "refresh_live_messages_for_games",
        new_callable=AsyncMock,
    ) as mock_refresh, patch.object(
        src.notification_batcher, "_bulk_fetch_matches", new_callable=AsyncMock
    ) as mock_bulk_matches, patch.object(
        src.notification_batcher, "_bulk_fetch_teams", new_callable=AsyncMock
    ) as mock_bulk_teams, patch.object(
        src.notification_batcher,
        "_bulk_fetch_pick_stats",
        new_callable=AsyncMock,
    ) as mock_bulk_stats, patch.object(
        src.notification_batcher, "_resolve_teams"
    ) as mock_resolve_teams:

        mock_batcher_session_cls.return_value.__aenter__.return_value = (
            mock_session
        )
        now = datetime.now(timezone.utc)
        contest = Contest(
            name="C1",
            image_url="http://example.com/icon.png",
            start_date=now,
            end_date=now,
        )
        match1 = Match(
            id=1, team1="A", team2="B", scheduled_time=now, contest_id=1
        )
        match1.contest = contest
        match2 = Match(
            id=2, team1="C", team2="D", scheduled_time=now, contest_id=1
        )
        match2.contest = contest

        mock_bulk_matches.return_value = [match1, match2]
        mock_bulk_teams.return_value = {}
        mock_resolve_teams.return_value = (None, None)

        # Mock results - manually because _process_results does query
        res1 = MagicMock(id=101, winner="A", score="2-0")
        res2 = MagicMock(id=102, winner="D", score="1-2")

        # We need to mock session.exec for results query
        mock_exec_res = MagicMock()
        mock_exec_res.all.return_value = [res1, res2]
        mock_session.exec.return_value = mock_exec_res

        # Mock stats: match_id -> (total, {team: count})
        mock_bulk_stats.return_value = {1: (10, {"A": 5}), 2: (20, {"D": 15})}

        await batcher.add_result(1, 101)
        await batcher.add_result(2, 102)

        await asyncio.sleep(1.1)

        mock_refresh.assert_awaited_once_with({"lol"})


@pytest.mark.asyncio
async def test_explicit_batching_mode():
    batcher = NotificationBatcher()
    mock_session = AsyncMock()

    with patch.object(
        src.notification_batcher, "get_bot_instance", return_value=MagicMock()
    ), patch.object(
        src.notification_batcher, "get_async_session"
    ) as mock_batcher_session_cls, patch.object(
        src.notification_batcher,
        "refresh_live_messages_for_games",
        new_callable=AsyncMock,
    ) as mock_refresh, patch.object(
        src.notification_batcher, "_bulk_fetch_matches", new_callable=AsyncMock
    ) as mock_bulk_matches, patch.object(
        src.notification_batcher, "_bulk_fetch_teams", new_callable=AsyncMock
    ) as mock_bulk_teams, patch.object(
        src.notification_batcher, "_resolve_teams"
    ) as mock_resolve_teams:

        mock_batcher_session_cls.return_value.__aenter__.return_value = (
            mock_session
        )

        match1 = MagicMock(
            id=1,
            scheduled_time=datetime.now(timezone.utc),
            game="lol",
        )
        match1.contest = MagicMock(image_url=None)
        match2 = MagicMock(
            id=2,
            scheduled_time=datetime.now(timezone.utc),
            game="lol",
        )
        match2.contest = MagicMock(image_url=None)
        match3 = MagicMock(
            id=3,
            scheduled_time=datetime.now(timezone.utc),
            game="cs2",
        )
        match3.contest = MagicMock(image_url=None)

        mock_bulk_matches.return_value = [match1, match2, match3]
        mock_bulk_teams.return_value = {}
        mock_resolve_teams.return_value = (None, None)

        async with batcher.batching():
            await batcher.add_reminder(1, 5)
            await asyncio.sleep(1.1)
            assert mock_refresh.call_count == 0
            assert len(batcher._pending["reminder_5"]) == 1
            await batcher.add_reminder(2, 5)

        mock_refresh.assert_awaited_once_with({"lol", "cs2"})
        assert len(batcher._pending["reminder_5"]) == 0
