import logging
from datetime import datetime, timedelta, timezone

from sqlmodel import select

from src.db import get_async_session
from src.crud.watchlist import list_watchers_for_match_async
from src.models import Match
from src.bot_instance import get_bot_instance

logger = logging.getLogger(__name__)


async def send_watchlist_reminders_job(reminder_window_minutes: int = 15) -> None:
    """Send DMs to users who are watching matches starting within the next window.

    This is a lightweight job intended to be scheduled (e.g., every minute). It
    respects only DM delivery for now; guild-level channel reminders require a
    GuildConfig.reminder_channel field which is not yet implemented.
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
                    logger.exception("Failed to fetch/send DM for user %s", getattr(w, "user_id", None))
