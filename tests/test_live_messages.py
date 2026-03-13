from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

import src.live_messages as live_messages


@pytest.mark.asyncio
async def test_refresh_scope_creates_missing_live_message():
    session = AsyncMock()
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
        "get_live_message_async",
        new_callable=AsyncMock,
        return_value=None,
    ), patch.object(
        live_messages,
        "_build_live_message_embed",
        new_callable=AsyncMock,
        return_value=embed,
    ), patch.object(
        live_messages,
        "set_live_message_async",
        new_callable=AsyncMock,
    ) as mock_set_live:
        await live_messages._refresh_guild_scope(
            session,
            guild,
            cfg,
            "lol",
            "upcoming",
        )

    channel.send.assert_awaited_once_with(embed=embed)
    mock_set_live.assert_awaited_once_with(
        session,
        guild.id,
        channel.id,
        sent_message.id,
        "upcoming",
        "lol",
    )


@pytest.mark.asyncio
async def test_refresh_scope_edits_existing_live_message():
    session = AsyncMock()
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
        "get_live_message_async",
        new_callable=AsyncMock,
        return_value=live_record,
    ), patch.object(
        live_messages,
        "_build_live_message_embed",
        new_callable=AsyncMock,
        return_value=embed,
    ), patch.object(
        live_messages,
        "set_live_message_async",
        new_callable=AsyncMock,
    ) as mock_set_live:
        await live_messages._refresh_guild_scope(
            session,
            guild,
            cfg,
            "lol",
            "running",
        )

    fetched_message.edit.assert_awaited_once_with(embed=embed)
    channel.send.assert_not_called()
    mock_set_live.assert_awaited_once_with(
        session,
        guild.id,
        channel.id,
        live_record.message_id,
        "running",
        "lol",
    )


def test_build_running_embed_keeps_empty_state_message():
    embed = live_messages._build_running_embed("lol", [])

    assert embed.title == "LoL Live Matches"
    assert embed.description == "No matches are currently live."
