from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

import src.commands.config as config_commands


@pytest.mark.asyncio
async def test_view_formats_mentions_and_games():
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.guild = MagicMock(id=123)
    interaction.response = AsyncMock()

    cfg = MagicMock(
        announcement_channel_id=111,
        live_updates_channel_id=222,
        enabled_games="lol,cs2",
    )

    with patch.object(
        config_commands, "get_async_session"
    ) as mock_session_factory, patch.object(
        config_commands,
        "get_guild_config_async",
        new_callable=AsyncMock,
        return_value=cfg,
    ):
        mock_session_factory.return_value.__aenter__.return_value = AsyncMock()
        await config_commands.view.callback(interaction)

    message = interaction.response.send_message.call_args.args[0]
    assert "<#111>" in message
    assert "<#222>" in message
    assert "LoL, CS2" in message


@pytest.mark.asyncio
async def test_set_channel_updates_live_updates_field():
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.guild = MagicMock(id=456)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()

    channel = MagicMock(id=999, mention="<#999>")
    kind = discord.app_commands.Choice(
        name="Live Updates",
        value="live_updates",
    )

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ), patch.object(
        config_commands,
        "_update_guild_channel",
        new_callable=AsyncMock,
    ) as mock_update:
        await config_commands.set_channel.callback(
            interaction,
            kind,
            channel,
        )

    mock_update.assert_awaited_once_with(
        interaction.guild.id,
        "live_updates_channel_id",
        channel.id,
    )
    interaction.followup.send.assert_awaited_once_with(
        "Updated live updates channel to <#999>.",
        ephemeral=True,
    )


@pytest.mark.asyncio
async def test_set_games_rejects_unsupported_slug():
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.guild = MagicMock(id=789)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ):
        await config_commands.set_games.callback(interaction, "lol,badgame")

    interaction.followup.send.assert_awaited_once()
    message = interaction.followup.send.call_args.args[0]
    assert "Invalid games list." in message
