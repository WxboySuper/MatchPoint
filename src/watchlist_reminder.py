import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from sqlmodel import select

from src.db import get_async_session
from src.crud.watchlist import list_watchers_for_match_async
from src.models import Match, GuildConfig
from src.bot_instance import get_bot_instance

logger = logging.getLogger(__name__)


async def _fetch_upcoming_matches(session, start, end):
    stmt = select(Match).where(
        Match.scheduled_time >= start,
        Match.scheduled_time <= end,
    )
    res = await session.exec(stmt)
    return res.all()


def _build_guild_configs(cfgs) -> Dict[int, GuildConfig]:
    return {c.guild_id: c for c in cfgs}


async def _get_watchers(session, pandascore_id):
    return await list_watchers_for_match_async(session, pandascore_id)


def _find_guild_id_for_watcher(bot, guild_configs, watcher):
    try:
        user_id_int = int(watcher.user_id)
    except Exception:
        return None
    for guild in bot.guilds:
        cfg = guild_configs.get(guild.id)
        if not cfg or not getattr(cfg, "reminder_channel_id", None):
            continue
        member = guild.get_member(user_id_int)
        if member is None:
            continue
        return guild.id
    return None


def _partition_watchers(bot, guild_configs, watchers):
    guild_watch_map = {}
    dm_watchers = []
    for w in watchers:
        if getattr(w, "is_watched", False) or getattr(
            w, "reminder_sent_at", None
        ):
            continue
        gid = _find_guild_id_for_watcher(bot, guild_configs, w)
        if gid:
            guild_watch_map.setdefault(gid, []).append(w)
        else:
            dm_watchers.append(w)
    return guild_watch_map, dm_watchers


async def _fetch_channel_safe(bot, channel_id, guild_id):
    channel = bot.get_channel(channel_id)
    if channel is not None:
        return channel
    try:
        return await bot.fetch_channel(channel_id)
    except Exception:
        logger.exception(
            "Failed to fetch channel %s for guild %s",
            channel_id,
            guild_id,
        )
        return None


async def _send_channel_message(session, channel, match, watches):
    """Send a single channel message for the provided watches and mark them.

    Guild id is inferred from the channel when available (used for logging).
    """
    lines: List[str] = [
        (
            f"<@{w.user_id}> — match {match.team1} vs {match.team2} "
            f"at {match.scheduled_time} (watch id: {w.id})"
        )
        for w in watches
    ]
    text = "\n".join(lines)
    try:
        await channel.send(text)
        now = datetime.now(timezone.utc)
        for w in watches:
            w.reminder_sent_at = now
        await session.commit()
    except Exception:
        guild_id = None
        try:
            guild = getattr(channel, "guild", None)
            guild_id = getattr(guild, "id", None) if guild else None
        except Exception:
            guild_id = None
        logger.exception(
            "Failed to send channel reminders for guild %s", guild_id
        )


async def _send_channel_reminders(session, bot, match, guild_data):
    guild_watch_map, guild_configs = guild_data
    for guild_id, watches in guild_watch_map.items():
        cfg = guild_configs.get(guild_id)
        if not cfg:
            continue
        channel = await _fetch_channel_safe(
            bot,
            cfg.reminder_channel_id,
            guild_id,
        )
        if not channel:
            continue
        await _send_channel_message(session, channel, match, watches)


async def _send_dm_reminders(session, bot, match, dm_watchers):
    for w in dm_watchers:
        try:
            user = await bot.fetch_user(int(w.user_id))
        except Exception:
            logger.exception(
                "Failed to fetch user %s", getattr(w, "user_id", None)
            )
            continue
        if not user:
            continue
        dm_text = (
            f"Reminder: upcoming match {match.team1} vs {match.team2} "
            f"starting at {match.scheduled_time} (watch id: {w.id})."
        )
        try:
            await user.send(dm_text)
            w.reminder_sent_at = datetime.now(timezone.utc)
            await session.commit()
        except Exception:
            logger.exception("Failed to send DM to user %s", w.user_id)


async def send_watchlist_reminders_job(
    reminder_window_minutes: int = 15,
) -> None:
    """Send reminders for users watching matches starting soon.

    Delivery order:
    1. If the watcher is a member of a guild that has
       `reminder_channel_id` set, send a single message to that guild
       channel (mentioning the user).
    2. Otherwise, fall back to sending a DM to the user.

    This keeps behavior predictable while allowing guilds to opt-in to
    channel reminders via `GuildConfig.reminder_channel_id`.
    """
    bot = get_bot_instance()
    if bot is None:
        logger.debug("No bot instance available; skipping watchlist reminders")
        return

    now = datetime.now(timezone.utc)
    end = now + timedelta(minutes=reminder_window_minutes)

    async with get_async_session() as session:
        try:
            matches = await _fetch_upcoming_matches(session, now, end)
        except Exception:
            logger.exception("Failed querying upcoming matches for reminders")
            return

        try:
            cfg_res = await session.exec(select(GuildConfig))
            guild_configs = _build_guild_configs(cfg_res.all())
        except Exception:
            guild_configs = {}

        for match in matches:
            pandascore_id = getattr(match, "pandascore_id", None)
            if pandascore_id is None:
                logger.debug(
                    "Skipping match without pandascore_id: %s",
                    getattr(match, "id", None),
                )
                continue
            try:
                watchers = await _get_watchers(session, pandascore_id)
            except Exception:
                logger.exception(
                    "Failed fetching watchers for match %s", pandascore_id
                )
                continue
            guild_watch_map, dm_watchers = _partition_watchers(
                bot, guild_configs, watchers
            )
            guild_data = (guild_watch_map, guild_configs)
            await _send_channel_reminders(session, bot, match, guild_data)
            await _send_dm_reminders(session, bot, match, dm_watchers)
