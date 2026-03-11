import logging
import asyncio
import os
from typing import Optional

import discord

from src.bot_instance import get_bot_instance
from src.crud import get_guild_config, get_live_message, set_live_message

logger = logging.getLogger(__name__)

ANNOUNCEMENT_CHANNEL_NAME = "pickem-announcements"
ADMIN_CHANNEL_NAME = "admin-updates"


def _find_existing_channel(
    guild: discord.Guild,
) -> Optional[discord.TextChannel]:
    """
    Finds a text channel in the guild with the announcement channel name
    (case-insensitive).

    Parameters:
        guild (discord.Guild): The guild to search for the channel.

    Returns:
        The discord.TextChannel if found, or None otherwise.
    """
    name_lower = ANNOUNCEMENT_CHANNEL_NAME.lower()
    return discord.utils.find(
        lambda c: getattr(c, "name", "").lower() == name_lower,
        guild.text_channels,
    )


def _get_bot_member(guild: discord.Guild) -> Optional[discord.Member]:
    """
    Resolve the bot's Member object for the given guild.

    Parameters:
        guild (discord.Guild): The guild for which to resolve the bot
            member.

    Returns:
        The discord.Member object for the bot in the specified guild,
            or None if it cannot be resolved.
    """
    if bot_member := getattr(guild, "me", None):
        return bot_member

    bot = get_bot_instance()
    if bot and bot.user:
        try:
            return guild.get_member(bot.user.id)
        except Exception:
            logger.exception(
                "Failed to resolve bot member for guild %s",
                getattr(guild, "id", "unknown"),
            )
    return None


def _can_send(
    channel: discord.TextChannel,
    bot_member: Optional[discord.Member],
) -> bool:
    """
    Determine whether the bot can send messages in the given text
    channel.

    If `bot_member` is provided, evaluate the channel permissions for
    that member. If `bot_member` is `None`, the function treats the
    channel as usable.

    Parameters:
        bot_member (discord.Member | None): The guild-specific Member
            object for the bot; may be `None` if unavailable.

    Returns:
        True if the bot can send messages in the channel, False
            otherwise.
    """
    try:
        if bot_member is None:
            # If we don't know the bot member, assume channel is usable
            return True
        perms = channel.permissions_for(bot_member)
        return perms.send_messages
    except Exception:
        return False


def _find_first_writable_channel(
    guild: discord.Guild, bot_member: Optional[discord.Member]
) -> Optional[discord.TextChannel]:
    """
    Find the first text channel in the guild where the bot has
    permissions to send messages.

    Parameters:
        guild (discord.Guild): The guild to search for a writable
            channel.
        bot_member (discord.Member | None): The guild-specific Member
            object for the bot; used to evaluate permissions.

    Returns:
        The first writable discord.TextChannel found, or None if no
            such channel exists.
    """
    for channel in guild.text_channels:
        if _can_send(channel, bot_member):
            return channel
    return None


async def _try_create_announcement_channel(
    guild: discord.Guild,
    bot_member: Optional[discord.Member],
) -> Optional[discord.TextChannel]:
    """
    Attempt to create the dedicated announcement text channel with
    restricted send permissions.

    Parameters:
        guild (discord.Guild): The guild where the channel should be
            created.
        bot_member (discord.Member | None): The bot's member object in
            the guild, used to set specific permissions.

    Returns:
        The newly created discord.TextChannel, or None if creation
            failed.
    """
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(send_messages=False)
    }
    if bot_member:
        overwrites[bot_member] = discord.PermissionOverwrite(
            send_messages=True
        )

    try:
        return await guild.create_text_channel(
            ANNOUNCEMENT_CHANNEL_NAME, overwrites=overwrites
        )
    except (discord.Forbidden, discord.HTTPException) as e:
        logger.warning(
            "Could not create announcement channel in guild %s: %s",
            getattr(guild, "id", "unknown"),
            e,
        )
    except Exception:
        logger.exception(
            "Unexpected error while creating announcement channel in guild %s",
            getattr(guild, "id", "unknown"),
        )
    return None


async def get_announcement_channel(
    guild: discord.Guild,
) -> Optional[discord.TextChannel]:
    """
    Resolve a suitable text channel in a guild for posting
    announcements.

    Parameters:
        guild (discord.Guild): The guild to search for or create an
            announcement channel.

    Returns:
        A discord.TextChannel suitable for announcements, or None if
            none could be found or created.
    """
    # 1) If a GuildConfig exists, prefer the configured announcement channel
    try:
        # Synchronous path: many command handlers call this from sync context
        from src.db import get_session

        with get_session() as session:
            cfg = get_guild_config(session, getattr(guild, "id", None))
            if cfg and cfg.announcement_channel_id:
                ch = discord.utils.get(guild.text_channels, id=cfg.announcement_channel_id)
                if ch and _can_send(ch, _get_bot_member(guild)):
                    return ch
    except Exception:
        # Fall back to existing discovery logic
        logger.debug("Guild config lookup failed; falling back to discovery")

    if existing := _find_existing_channel(guild):
        return existing

    bot_member = _get_bot_member(guild)

    if created := await _try_create_announcement_channel(guild, bot_member):
        return created

    if writable := _find_first_writable_channel(guild, bot_member):
        return writable

    # As a last resort, return the first text channel if we don't know perms
    if bot_member is None and guild.text_channels:
        return guild.text_channels[0]

    return None


async def send_announcement(
    guild: discord.Guild, embed: discord.Embed
) -> bool:
    """
    Send an embed announcement to an appropriate channel in the guild.

    Parameters:
        guild (discord.Guild): The guild where the announcement should
            be sent.
        embed (discord.Embed): The embed content to send.

    Returns:
        bool: True if the embed was sent successfully, False otherwise.
    """
    channel = await get_announcement_channel(guild)
    if channel is None:
        logger.error(
            "No available announcement channel in guild %s",
            getattr(guild, "id", "unknown"),
        )
        return False

    try:
        await channel.send(embed=embed)
        return True
    except (discord.Forbidden, discord.HTTPException) as e:
        logger.error(
            "Failed to send announcement in guild %s, channel %s: %s",
            getattr(guild, "id", "unknown"),
            getattr(channel, "id", "unknown"),
            e,
        )
        return False


async def get_admin_channel(
    guild: discord.Guild,
) -> Optional[discord.TextChannel]:
    """
    Locate or create the administrator updates channel in the given guild.

    Parameters:
        guild (discord.Guild): The guild to search for or create the
            admin channel.

    Returns:
        The discord.TextChannel for admin updates, or None if it
            could not be found or created.
    """
    if channel := discord.utils.get(
        guild.text_channels, name=ADMIN_CHANNEL_NAME
    ):
        return channel

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(send_messages=False),
        guild.me: discord.PermissionOverwrite(send_messages=True),
    }
    try:
        return await guild.create_text_channel(
            ADMIN_CHANNEL_NAME, overwrites=overwrites
        )
    except (discord.Forbidden, discord.HTTPException) as e:
        logger.warning(
            "Could not create admin channel in guild %s: %s",
            getattr(guild, "id", "unknown"),
            e,
        )
    except Exception:
        logger.exception(
            "Unexpected error while creating admin channel in guild %s",
            getattr(guild, "id", "unknown"),
        )
    return None


def _resolve_dev_guild(
    bot: discord.Client, guild_id_str: str
) -> Optional[discord.Guild]:
    """
    Resolve the developer guild from its ID string.

    Parameters:
        bot (discord.Client): The bot instance used to fetch the guild.
        guild_id_str (str): The string representation of the guild ID.

    Returns:
        The discord.Guild object if found and valid, or None otherwise.
    """
    try:
        guild = bot.get_guild(int(guild_id_str))
        if not guild:
            logger.warning("Developer guild %s not found.", guild_id_str)
        return guild
    except (ValueError, TypeError):
        logger.warning("Invalid DEVELOPER_GUILD_ID: %s", guild_id_str)
    except Exception:
        logger.exception("Failed to resolve developer guild %s", guild_id_str)
    return None


async def send_admin_update(message: str, mention_user_id: int | None = None):
    """
    Send a plain-text admin update to the developer guild's
    "admin-updates" channel.

    Parameters:
        message (str): The message content to send.
        mention_user_id (int | None): Optional user ID to mention in
            the update.
    """
    bot = get_bot_instance()
    dev_guild_id = os.getenv("DEVELOPER_GUILD_ID")

    if not bot or not dev_guild_id:
        logger.debug("Bot or DEVELOPER_GUILD_ID not available.")
        return

    guild = _resolve_dev_guild(bot, dev_guild_id)
    if not guild:
        return

    try:
        if channel := await get_admin_channel(guild):
            body = (
                f"<@{mention_user_id}> {message}"
                if mention_user_id
                else message
            )
            await channel.send(body)
            logger.info("Sent admin update to %s", channel.name)
    except Exception as e:
        logger.exception("Failed sending admin update: %s", e)


async def broadcast_embed_to_guilds(
    bot: discord.Client, embed: discord.Embed, context: str
):
    """
    Broadcast an embed to every guild the bot is a member of and
    record success or failure for each delivery.

    Parameters:
        bot (discord.Client): The bot instance used to access guilds.
        embed (discord.Embed): The embed to broadcast.
        context (str): Short description included in log messages to
            identify this broadcast.
    """
    for i, guild in enumerate(bot.guilds):
        try:
            await send_announcement(guild, embed)
            logger.info("Sent %s to guild %s.", context, guild.id)
        except Exception as e:
            msg = "Failed to send %s to guild %s: %s"
            logger.error(msg, context, guild.id, e)

        # Yield to event loop every 3 guilds to avoid heartbeat blocking.
        # Use (i+1) % 3 == 0 to process the first guild before yielding.
        if (i + 1) % 3 == 0:
            await asyncio.sleep(0)
