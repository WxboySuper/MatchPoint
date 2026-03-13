from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

import src.commands.config as config_commands


@pytest.mark.asyncio
async def test_view_formats_mentions_and_games(mocked_interaction):
    mocked_interaction.guild.id = 123
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
        await config_commands.view.callback(mocked_interaction)

    message = mocked_interaction.response.send_message.call_args.args[0]
    assert "<#111>" in message
    assert "<#222>" in message
    assert "LoL, CS2" in message


@pytest.mark.asyncio
async def test_set_channel_updates_live_updates_field(mocked_interaction):
    mocked_interaction.guild.id = 456
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
            mocked_interaction,
            kind,
            channel,
        )

    mock_update.assert_awaited_once_with(
        mocked_interaction.guild.id,
        "live_updates_channel_id",
        channel.id,
    )
    mocked_interaction.followup.send.assert_awaited_once_with(
        "Updated live updates channel to <#999>.",
        ephemeral=True,
    )


@pytest.mark.asyncio
async def test_set_games_rejects_unsupported_slug(mocked_interaction):
    mocked_interaction.guild.id = 789

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ):
        await config_commands.set_games.callback(
            mocked_interaction, "lol,badgame"
        )

    mocked_interaction.followup.send.assert_awaited_once()
    message = mocked_interaction.followup.send.call_args.args[0]
    assert "Invalid games list." in message


@pytest.mark.asyncio
async def test_add_game_updates_enabled_games(mocked_interaction):
    mocked_interaction.guild.id = 101

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ), patch.object(
        config_commands,
        "_load_enabled_games",
        new_callable=AsyncMock,
        return_value=["lol"],
    ), patch.object(
        config_commands,
        "_update_enabled_games",
        new_callable=AsyncMock,
        return_value="lol,cs2",
    ) as mock_update:
        await config_commands.add_game.callback(mocked_interaction, "cs2")

    mock_update.assert_awaited_once_with(101, ["lol", "cs2"])
    mocked_interaction.followup.send.assert_awaited_once_with(
        "Enabled games: LoL, CS2",
        ephemeral=True,
    )


@pytest.mark.asyncio
async def test_add_game_handles_unset_enabled_games(mocked_interaction):
    mocked_interaction.guild.id = 111

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ), patch.object(
        config_commands,
        "_load_enabled_games",
        new_callable=AsyncMock,
        return_value=None,
    ), patch.object(
        config_commands,
        "_update_enabled_games",
        new_callable=AsyncMock,
        return_value="cs2",
    ) as mock_update:
        await config_commands.add_game.callback(mocked_interaction, "cs2")

    mock_update.assert_awaited_once_with(111, ["cs2"])
    mocked_interaction.followup.send.assert_awaited_once_with(
        "Enabled games: CS2",
        ephemeral=True,
    )


@pytest.mark.asyncio
async def test_remove_game_removes_enabled_slug(mocked_interaction):
    mocked_interaction.guild.id = 202

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ), patch.object(
        config_commands,
        "_load_enabled_games",
        new_callable=AsyncMock,
        return_value=["lol", "cs2"],
    ), patch.object(
        config_commands,
        "_update_enabled_games",
        new_callable=AsyncMock,
        return_value="lol",
    ) as mock_update:
        await config_commands.remove_game.callback(mocked_interaction, "cs2")

    mock_update.assert_awaited_once_with(202, ["lol"])
    mocked_interaction.followup.send.assert_awaited_once_with(
        "Enabled games: LoL",
        ephemeral=True,
    )


@pytest.mark.asyncio
async def test_remove_game_filters_stale_games(mocked_interaction):
    mocked_interaction.guild.id = 303

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ), patch.object(
        config_commands,
        "_load_enabled_games",
        new_callable=AsyncMock,
        return_value=["lol", "stale", "cs2"],
    ), patch.object(
        config_commands,
        "_update_enabled_games",
        new_callable=AsyncMock,
        return_value="lol",
    ) as mock_update:
        await config_commands.remove_game.callback(mocked_interaction, "cs2")

    mock_update.assert_awaited_once_with(303, ["lol"])
    mocked_interaction.followup.send.assert_awaited_once_with(
        "Enabled games: LoL",
        ephemeral=True,
    )
