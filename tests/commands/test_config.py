from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest
from dataclasses import dataclass

import src.commands.config as config_commands


@dataclass(frozen=True)
class _GameCommandCase:
    guild_id: int
    command_name: str
    game: str
    loaded_games: object
    stored_games: str
    expected_games: list[str]
    expected_message: str


async def _assert_game_command_result(
    mocked_interaction,
    case: _GameCommandCase,
):
    mocked_interaction.guild.id = case.guild_id
    command = getattr(config_commands, case.command_name)

    with patch.object(
        config_commands,
        "_has_config_permission",
        new_callable=AsyncMock,
        return_value=True,
    ), patch.object(
        config_commands,
        "_load_enabled_games",
        new_callable=AsyncMock,
        return_value=case.loaded_games,
    ), patch.object(
        config_commands,
        "_update_enabled_games",
        new_callable=AsyncMock,
        return_value=case.stored_games,
    ) as mock_update:
        await command.callback(mocked_interaction, case.game)

    mock_update.assert_awaited_once_with(case.guild_id, case.expected_games)
    mocked_interaction.followup.send.assert_awaited_once_with(
        case.expected_message,
        ephemeral=True,
    )


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
    await _assert_game_command_result(
        mocked_interaction,
        _GameCommandCase(
            guild_id=101,
            command_name="add_game",
            game="cs2",
            loaded_games=["lol"],
            stored_games="lol,cs2",
            expected_games=["lol", "cs2"],
            expected_message="Enabled games: LoL, CS2",
        ),
    )


@pytest.mark.asyncio
async def test_add_game_handles_unset_enabled_games(mocked_interaction):
    await _assert_game_command_result(
        mocked_interaction,
        _GameCommandCase(
            guild_id=111,
            command_name="add_game",
            game="cs2",
            loaded_games=None,
            stored_games="cs2",
            expected_games=["cs2"],
            expected_message="Enabled games: CS2",
        ),
    )


@pytest.mark.asyncio
async def test_remove_game_removes_enabled_slug(mocked_interaction):
    await _assert_game_command_result(
        mocked_interaction,
        _GameCommandCase(
            guild_id=202,
            command_name="remove_game",
            game="cs2",
            loaded_games=["lol", "cs2"],
            stored_games="lol",
            expected_games=["lol"],
            expected_message="Enabled games: LoL",
        ),
    )


@pytest.mark.asyncio
async def test_remove_game_filters_stale_games(mocked_interaction):
    await _assert_game_command_result(
        mocked_interaction,
        _GameCommandCase(
            guild_id=303,
            command_name="remove_game",
            game="cs2",
            loaded_games=["lol", "stale", "cs2"],
            stored_games="lol",
            expected_games=["lol"],
            expected_message="Enabled games: LoL",
        ),
    )
