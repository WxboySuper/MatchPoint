import logging
from datetime import datetime, timedelta, timezone

from sqlmodel import select

from src.db import get_async_session
from src.crud.watchlist import list_watchers_for_match_async
from src.models import Match, GuildConfig
from src.bot_instance import get_bot_instance

logger = logging.getLogger(__name__)


async def send_watchlist_reminders_job(reminder_window_minutes: int = 15) -> None:
    """Send reminders to users who are watching matches starting within the next window.

    Delivery order:
    1. If the watcher is a member of a guild that has `reminder_channel_id` set,
       send a single message to that guild channel (mentioning the user).
    2. Otherwise, fall back to sending a DM to the user.

    This keeps behavior predictable while allowing guilds to opt-in to channel
    reminders via `GuildConfig.reminder_channel_id`.
    """
    bot = get_bot_instance()
    if bot is None:
        logger.debug("No bot instance available; skipping watchlist reminders")
        return

    now = datetime.now(timezone.utc)
    end = now + timedelta(minutes=reminder_window_minutes)

    async with get_async_session() as session:
        stmt = select(Match).where(
            Match.scheduled_time >= now,
            Match.scheduled_time <= end,
        )
        try:
            res = await session.exec(stmt)
        except Exception:
            logger.exception("Failed querying upcoming matches for reminders")
            return

        matches = res.all()

        # Load guild configs once for quick lookup: {guild_id: GuildConfig}
        try:
            cfg_res = await session.exec(select(GuildConfig))
            guild_configs = {c.guild_id: c for c in cfg_res.all()}
        except Exception:
            guild_configs = {}

        for match in matches:
            # Use the pandascore_id if present; watchlist stores the external id
            pandascore_id = getattr(match, "pandascore_id", None) or match.id
            try:
                watchers = await list_watchers_for_match_async(session, pandascore_id)
            except Exception:
                logger.exception("Failed fetching watchers for match %s", pandascore_id)
                continue

            for w in watchers:
                # Skip already-marked watched entries
                if getattr(w, "is_watched", False):
                    continue

                sent_to_channel = False

                # Try to deliver via guild channel reminders where possible
                try:
                    for guild in bot.guilds:
                        cfg = guild_configs.get(guild.id)
                        if not cfg or not getattr(cfg, "reminder_channel_id", None):
                            continue

                        # Fast path: check cache for member presence
                        member = guild.get_member(int(w.user_id))
                        if member is None:
                            continue

                        # Get the channel object; try cache first, then fetch
                        channel = bot.get_channel(cfg.reminder_channel_id)
                        if channel is None:
                            try:
                                channel = await bot.fetch_channel(cfg.reminder_channel_id)
                            except Exception:
                                logger.exception(
                                    "Failed to fetch channel %s in guild %s",
                                    cfg.reminder_channel_id,
                                    guild.id,
                                )
                                continue

                        channel_text = (
                            f"Reminder: upcoming match {match.team1} vs {match.team2} "
                            f"starting at {match.scheduled_time} (watch id: {w.id}). <@{w.user_id}>"
                        )
                        try:
                            await channel.send(channel_text)
                            sent_to_channel = True
                            break
                        except Exception:
                            logger.exception(
                                "Failed to send channel reminder in guild %s for user %s",
                                guild.id,
                                w.user_id,
                            )

                    # Fallback to DM if not sent to any guild channel
                    if not sent_to_channel:
                        try:
                            user = await bot.fetch_user(int(w.user_id))
                            if not user:
                                continue
                            dm_text = (
                                f"Reminder: upcoming match {match.team1} vs {match.team2} "
                                f"starting at {match.scheduled_time} (watch id: {w.id})."
                            )
                            try:
                                await user.send(dm_text)
                            except Exception:
                                logger.exception("Failed to send DM to user %s", w.user_id)
                        except Exception:
                            logger.exception(
                                "Failed to fetch/send DM for user %s",
                                getattr(w, "user_id", None),
                            )

                except Exception:
                    logger.exception("Failed to deliver reminder for user %s", getattr(w, "user_id", None))
