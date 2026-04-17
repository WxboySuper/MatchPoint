import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta, timezone

from sqlmodel import SQLModel, Session, create_engine

import src.watchlist_reminder as watchlist_reminder
from src import crud
from src.crud.watchlist import add_watch
from src.crud import guild_config as gc_crud
from src.bot_instance import set_bot_instance


@pytest.mark.asyncio
async def test_channel_reminder_sent_to_configured_guild(async_session_for_engine):
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    # Setup DB records
    with Session(engine) as session:
        user = crud.create_user(session, discord_id="9999", username="tester")
        contest = crud.create_contest(
            session,
            {
                "name": "TestContest",
                "start_date": datetime.now(timezone.utc),
                "end_date": datetime.now(timezone.utc) + timedelta(days=1),
                "leaguepedia_id": "tc",
            },
        )
        match = crud.create_match(
            session,
            crud.MatchCreateParams(
                contest_id=contest.id,
                team1="A",
                team2="B",
                scheduled_time=datetime.now(timezone.utc) + timedelta(minutes=10),
                leaguepedia_id="m1",
            ),
        )

        # User watches this match
        _ = add_watch(session, str(user.discord_id), match.id)
        user_discord_id = user.discord_id

        # Create a guild config with reminder_channel_id set
        gc_crud.upsert_guild_config(session, guild_id=111, reminder_channel_id=222)

    # Prepare a mocked bot with a guild that contains the user and a channel
    mock_channel = AsyncMock()
    mock_channel.send = AsyncMock()

    guild = MagicMock()
    guild.id = 111
    # Simulate that the user is a member of this guild
    guild.get_member.return_value = MagicMock(id=int(user_discord_id))

    # Ensure channel resolution works: bot.get_channel -> channel, fetch_channel -> channel
    mock_bot = MagicMock()
    mock_bot.guilds = [guild]
    mock_bot.get_channel = MagicMock(return_value=mock_channel)
    mock_bot.fetch_channel = AsyncMock(return_value=mock_channel)
    mock_bot.fetch_user = AsyncMock(return_value=MagicMock())

    set_bot_instance(mock_bot)

    # Patch the async DB session factory used by the job
    with patch.object(
        watchlist_reminder,
        "get_async_session",
        return_value=async_session_for_engine(engine),
    ):
        # Run the reminder job (it should find the upcoming match and send to channel)
        await watchlist_reminder.send_watchlist_reminders_job(reminder_window_minutes=15)

    # Assert the channel was used for delivery
    assert mock_channel.send.await_count >= 1
    # Ensure DM path was not used in this case (fetch_user not awaited)
    assert mock_bot.fetch_user.await_count == 0
