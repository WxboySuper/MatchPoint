from unittest.mock import AsyncMock, patch

import discord
import pytest

from src.commands.sync_matches import SyncMatches


@pytest.mark.asyncio
async def test_refresh_live_messages_command_triggers_refresh():
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()

    cog = SyncMatches(bot=AsyncMock())

    with patch(
        "src.commands.sync_matches.refresh_all_live_messages",
        new_callable=AsyncMock,
    ) as mock_refresh:
        await cog.refresh_live_messages.callback(cog, interaction)

    mock_refresh.assert_awaited_once()
    interaction.followup.send.assert_awaited_once_with(
        "Live messages refreshed.",
        ephemeral=True,
    )


@pytest.mark.asyncio
async def test_sync_matches_refreshes_live_messages_after_success():
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()

    cog = SyncMatches(bot=AsyncMock())

    with patch(
        "src.commands.sync_matches.perform_pandascore_sync",
        new_callable=AsyncMock,
        return_value={"contests": 1, "matches": 2, "teams": 3},
    ), patch(
        "src.commands.sync_matches.refresh_all_live_messages",
        new_callable=AsyncMock,
    ) as mock_refresh:
        await cog.sync_matches.callback(cog, interaction)

    mock_refresh.assert_awaited_once()
