import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

import src.notification_batcher as nb
import src.notification_delivery as delivery
from src.notification_batcher import NotificationBatcher
from src.models import Match, Contest


class _BatcherMockBundle:
    def __init__(
        self,
        batcher_session_cls,
        delivery_session_cls,
        bulk_matches,
        bulk_teams,
        resolve_teams,
    ):
        self.batcher_session_cls = batcher_session_cls
        self.delivery_session_cls = delivery_session_cls
        self.bulk_matches = bulk_matches
        self.bulk_teams = bulk_teams
        self.resolve_teams = resolve_teams


def _build_live_message_test_match(match_id: int) -> Match:
    now = datetime.now(timezone.utc)
    contest = Contest(name="C1", image_url=None, start_date=now, end_date=now)
    match = Match(
        id=match_id,
        team1="A" if match_id == 1 else "X",
        team2="B" if match_id == 1 else "Y",
        scheduled_time=now,
        contest_id=1,
        game="lol",
    )
    match.contest = contest
    return match


def _configure_batcher_mocks(
    bundle: _BatcherMockBundle,
    mock_session,
    match,
) -> None:
    bundle.batcher_session_cls.return_value.__aenter__.return_value = (
        mock_session
    )
    bundle.delivery_session_cls.return_value.__aenter__.return_value = (
        mock_session
    )
    bundle.bulk_matches.return_value = [match]
    bundle.bulk_teams.return_value = {}
    bundle.resolve_teams.return_value = (None, None)


@pytest.mark.asyncio
async def test_scoped_live_message_editing_and_creation():
    """Verify that the batcher will attempt to edit an existing live message
    for a guild scoped to (scope_type, game_slug) and will create/persist a
    new message pointer when none exists.
    """
    batcher = NotificationBatcher()

    # Prepare mocks
    mock_bot = MagicMock()
    guild = MagicMock()
    guild.id = 111
    guild.text_channels = []
    # Simulate guild.get_channel and fetch_channel
    channel = AsyncMock()
    channel.id = 222
    channel.send = AsyncMock()

    # Create a fake message object returned by channel.send
    fake_msg = AsyncMock()
    fake_msg.id = 333
    channel.send.return_value = fake_msg

    mock_bot.guilds = [guild]
    guild.get_channel.return_value = channel

    # Mock DB session to initially return no live message, then ensure set is called
    mock_session = AsyncMock()

    match = _build_live_message_test_match(1)

    # Patch get_bot_instance and session helpers and bulk fetches
    cfg_obj = MagicMock()
    cfg_obj.live_updates_channel_id = channel.id
    cfg_obj.enabled_games = None

    with patch.object(
        nb, "get_bot_instance", return_value=mock_bot
    ), patch.object(
        nb, "get_async_session"
    ) as mock_batcher_session_cls, patch.object(
        delivery, "get_async_session"
    ) as mock_delivery_session_cls, patch.object(
        nb, "_bulk_fetch_matches", new_callable=AsyncMock
    ) as mock_bulk_matches, patch.object(
        nb, "_bulk_fetch_teams", new_callable=AsyncMock
    ) as mock_bulk_teams, patch.object(
        nb, "_resolve_teams"
    ) as mock_resolve_teams, patch.object(
        delivery, "set_live_message_async", new_callable=AsyncMock
    ) as mock_set_live_message, patch.object(
        delivery,
        "get_guild_config_async",
        new_callable=AsyncMock,
        return_value=cfg_obj,
    ), patch.object(
        delivery,
        "get_live_message_async",
        new_callable=AsyncMock,
        return_value=None,
    ):
        bundle = _BatcherMockBundle(
            mock_batcher_session_cls,
            mock_delivery_session_cls,
            mock_bulk_matches,
            mock_bulk_teams,
            mock_resolve_teams,
        )

        _configure_batcher_mocks(
            bundle,
            mock_session,
            match,
        )

        # Trigger a reminder (mapped to upcoming scope)
        await batcher.add_reminder(match.id, 5)

        # Wait for debounce flush
        await asyncio.sleep(1.1)

        # Assert channel.send was called (no existing live message existed)
        assert channel.send.await_count >= 1
        # Ensure we persisted the live message pointer for the guild/scope
        assert mock_set_live_message.await_count >= 1


@pytest.mark.asyncio
async def test_mid_series_updates_edit_existing_message():
    """When a live (running) message exists, mid-series updates should edit
    the existing message rather than sending a new one.
    """
    batcher = NotificationBatcher()

    mock_bot = MagicMock()
    guild = MagicMock()
    guild.id = 2222
    channel = MagicMock()
    channel.id = 4444

    # Simulate existing live message record and message fetch/edit
    live_rec = MagicMock()
    live_rec.channel_id = channel.id
    live_rec.message_id = 5555

    # make channel.fetch_message return an object with edit coroutine
    fetched_msg = AsyncMock()
    fetched_msg.edit = AsyncMock()
    channel.fetch_message = AsyncMock(return_value=fetched_msg)

    mock_bot.guilds = [guild]
    guild.get_channel.return_value = channel

    mock_session = AsyncMock()

    match = _build_live_message_test_match(11)

    with patch.object(
        nb, "get_bot_instance", return_value=mock_bot
    ), patch.object(
        nb, "get_async_session"
    ) as mock_batcher_session_cls, patch.object(
        delivery, "get_async_session"
    ) as mock_delivery_session_cls, patch.object(
        nb, "_bulk_fetch_matches", new_callable=AsyncMock
    ) as mock_bulk_matches, patch.object(
        nb, "_bulk_fetch_teams", new_callable=AsyncMock
    ) as mock_bulk_teams, patch.object(
        nb, "_resolve_teams"
    ) as mock_resolve_teams, patch.object(
        delivery, "get_live_message_async", new_callable=AsyncMock
    ) as mock_get_live, patch.object(
        delivery, "set_live_message_async", new_callable=AsyncMock
    ) as mock_set_live:
        # Provide a guild config so per-guild channel resolution works in tests
        with patch.object(
            delivery,
            "get_guild_config_async",
            new_callable=AsyncMock,
            return_value=MagicMock(
                live_updates_channel_id=channel.id,
                enabled_games=None,
            ),
        ):
            bundle = _BatcherMockBundle(
                mock_batcher_session_cls,
                mock_delivery_session_cls,
                mock_bulk_matches,
                mock_bulk_teams,
                mock_resolve_teams,
            )

            _configure_batcher_mocks(
                bundle,
                mock_session,
                match,
            )

            mock_get_live.return_value = live_rec

            # Trigger mid-series update (mapped to running scope)
            await batcher.add_mid_series_update(match.id, "1-0")
            await asyncio.sleep(1.1)

            # Expect we edited the existing message
            fetched_msg.edit.assert_awaited()
            # And did not call set_live_message since pointer already existed
            assert mock_set_live.await_count == 0
