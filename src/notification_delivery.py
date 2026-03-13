import asyncio
import inspect
import logging
from dataclasses import dataclass
from typing import Any, Optional

import discord

from src.announcements import broadcast_embed_to_guilds, send_announcement
from src.crud import (
    get_guild_config_async,
    get_live_message_async,
    set_live_message_async,
)
from src.db import get_async_session

logger = logging.getLogger(__name__)

_GUILD_COLLECTION_TYPES = (list, tuple, set)


@dataclass(frozen=True)
class DeliveryRequest:
    embed: discord.Embed
    context: str
    game_slug: str = "lol"
    scope_type: Optional[str] = None


@dataclass(frozen=True)
class DeliveryTarget:
    guild: discord.Guild
    channel: Any
    live: Any
    request: DeliveryRequest


async def deliver_embed(bot: discord.Client, request: DeliveryRequest) -> None:
    if not bot:
        return

    guilds = _get_concrete_guilds(bot)
    if guilds is None:
        await broadcast_embed_to_guilds(bot, request.embed, request.context)
        return

    guilds_to_broadcast = await _deliver_embed_to_guilds(guilds, request)
    await _broadcast_delivery_fallbacks(
        bot, guilds, guilds_to_broadcast, request
    )


def _get_concrete_guilds(
    bot: discord.Client,
) -> Optional[list[discord.Guild]]:
    guilds = getattr(bot, "guilds", None)
    if not isinstance(guilds, _GUILD_COLLECTION_TYPES):
        return None
    return list(guilds)


async def _await_if_needed(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _load_delivery_state(session, guild, request: DeliveryRequest):
    guild_id = getattr(guild, "id", None)
    cfg = await _await_if_needed(get_guild_config_async(session, guild_id))
    live = await _await_if_needed(
        get_live_message_async(
            session, guild_id, request.scope_type, request.game_slug
        )
    )
    return cfg, live


def _guild_accepts_game(cfg: Any, game_slug: str) -> bool:
    if not cfg or not cfg.enabled_games:
        return True

    allowed = [g.strip() for g in cfg.enabled_games.split(",") if g.strip()]
    return not allowed or game_slug in allowed


def _get_channel_id(cfg: Any, live: Any) -> Optional[int]:
    if cfg and cfg.live_updates_channel_id:
        return cfg.live_updates_channel_id
    if live and live.channel_id:
        return live.channel_id
    return None


async def _resolve_delivery_target(
    session, guild, request: DeliveryRequest
) -> Optional[DeliveryTarget]:
    cfg, live = await _load_delivery_state(session, guild, request)
    if not _guild_accepts_game(cfg, request.game_slug):
        return DeliveryTarget(
            guild=guild, channel=None, live=live, request=request
        )

    channel_id = _get_channel_id(cfg, live)
    if channel_id is None:
        return None

    try:
        channel = guild.get_channel(channel_id)
        if channel is None:
            channel = await guild.fetch_channel(channel_id)
    except Exception:
        logger.exception(
            "Failed to resolve channel %s for guild %s",
            channel_id,
            getattr(guild, "id", None),
        )
        return None

    return DeliveryTarget(
        guild=guild, channel=channel, live=live, request=request
    )


async def _try_edit_live_message(target: DeliveryTarget) -> bool:
    live = target.live
    if not live or not getattr(live, "message_id", None):
        return False

    msg = await target.channel.fetch_message(live.message_id)
    await msg.edit(embed=target.request.embed)
    return True


async def _send_and_track_live_message(
    session, target: DeliveryTarget
) -> None:
    new_msg = await target.channel.send(embed=target.request.embed)
    await set_live_message_async(
        session,
        getattr(target.guild, "id", None),
        target.channel.id,
        new_msg.id,
        target.request.scope_type or "guild_live",
        target.request.game_slug,
    )


async def _deliver_to_guild(
    session, guild: discord.Guild, request: DeliveryRequest
) -> bool:
    target = await _resolve_delivery_target(session, guild, request)
    if target is None:
        return False
    if target.channel is None:
        return True

    try:
        if await _try_edit_live_message(target):
            return True
    except discord.NotFound:
        pass
    except Exception:
        logger.exception(
            "Failed to edit live message %s in guild %s",
            getattr(target.live, "message_id", None),
            getattr(guild, "id", None),
        )
        return False

    try:
        await _send_and_track_live_message(session, target)
        return True
    except Exception:
        logger.exception(
            "Failed to send live update to guild %s in channel %s",
            getattr(guild, "id", None),
            getattr(target.channel, "id", None),
        )
        return False


async def _deliver_embed_to_guilds(
    guilds: list[discord.Guild], request: DeliveryRequest
) -> list[discord.Guild]:
    guilds_to_broadcast = []
    async with get_async_session() as session:
        for guild in guilds:
            try:
                delivered = await _deliver_to_guild(session, guild, request)
                if not delivered:
                    guilds_to_broadcast.append(guild)
            except Exception:
                logger.exception(
                    "Unexpected error while delivering to guild %s",
                    getattr(guild, "id", None),
                )
                guilds_to_broadcast.append(guild)
    return guilds_to_broadcast


async def _yield_to_event_loop() -> None:
    await asyncio.sleep(0)


async def _broadcast_delivery_fallbacks(
    bot: discord.Client,
    guilds: list[discord.Guild],
    guilds_to_broadcast: list[discord.Guild],
    request: DeliveryRequest,
) -> None:
    if not guilds_to_broadcast:
        return

    if len(guilds_to_broadcast) == len(guilds):
        await broadcast_embed_to_guilds(bot, request.embed, request.context)
        return

    for idx, guild in enumerate(guilds_to_broadcast):
        try:
            await send_announcement(guild, request.embed)
            logger.info("Sent %s to guild %s.", request.context, guild.id)
        except Exception:
            logger.exception(
                "Failed to send %s to guild %s",
                request.context,
                guild.id,
            )
        if (idx + 1) % 3 == 0:
            await _yield_to_event_loop()
