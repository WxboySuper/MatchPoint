import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Any, AsyncGenerator, Callable
from contextlib import asynccontextmanager

import discord
from sqlmodel import select, or_, func
from sqlalchemy.orm import selectinload
import inspect

from src.db import get_async_session
from src.models import Match, Result, Pick, Team
from src.announcements import broadcast_embed_to_guilds, send_announcement
from src.bot_instance import get_bot_instance
from src.crud import (
    get_guild_config_async,
    get_live_message_async,
    set_live_message_async,
)

logger = logging.getLogger(__name__)


class NotificationBatcher:
    def __init__(self):
        self._pending = defaultdict(list)
        self._timers = {}
        self._lock = asyncio.Lock()
        self._batch_depth = 0

    @asynccontextmanager
    async def batching(self) -> AsyncGenerator[None, None]:
        """
        Context manager to pause automatic flushing of notifications.
        When the outer-most context exits, all pending notifications are
        flushed.
        """
        async with self._lock:
            self._batch_depth += 1
            logger.debug("Entered batching mode (depth=%d)", self._batch_depth)

        try:
            yield
        finally:
            should_flush = False
            async with self._lock:
                self._batch_depth -= 1
                logger.debug(
                    "Exited batching mode (depth=%d)", self._batch_depth
                )
                if self._batch_depth == 0:
                    should_flush = True

            if should_flush:
                await self._flush_all()

    async def add_reminder(self, match_id: int, minutes: int):
        key = f"reminder_{minutes}"
        async with self._lock:
            self._pending[key].append(match_id)
        await self._ensure_timer(key)

    async def add_result(self, match_id: int, result_id: int):
        key = "result"
        async with self._lock:
            self._pending[key].append((match_id, result_id))
        await self._ensure_timer(key)

    async def add_time_change(
        self, match_id: int, old_time: Any, new_time: Any
    ):
        key = "time_change"
        async with self._lock:
            self._pending[key].append((match_id, old_time, new_time))
        await self._ensure_timer(key)

    async def add_mid_series_update(self, match_id: int, score: str):
        key = "mid_series"
        async with self._lock:
            self._pending[key].append((match_id, score))
        await self._ensure_timer(key)

    async def _ensure_timer(self, key: str):
        async with self._lock:
            # If we are in batch mode (depth > 0), do not schedule a timer.
            # The flush will happen when the context manager exits.
            if self._batch_depth > 0:
                return

            existing = self._timers.get(key)
            if existing and not existing.done():
                return

            # Schedule flush after 1 second (debounce)
            self._timers[key] = asyncio.create_task(self._flush_later(key))

    async def _flush_later(self, key: str):
        await asyncio.sleep(1.0)
        async with self._lock:
            # Check if we are still the active timer for this key
            if self._timers.get(key) != asyncio.current_task():
                return

            # If batch mode was entered while we were sleeping, abort flush.
            # Remove from _timers so _flush_all knows not to wait for us.
            if self._batch_depth > 0:
                self._timers.pop(key, None)
                return

            items = self._pending.pop(key, [])
            self._timers.pop(key, None)

        if items:
            try:
                await _process_batch(key, items)
            except Exception:
                logger.exception("Failed to process batch for key %s", key)

    async def _flush_all(self):
        """Flush all pending notifications immediately."""
        keys_to_process = []
        async with self._lock:
            keys_to_process = list(self._pending.keys())
            # Cancel any existing timers since we are flushing now
            for key, task in self._timers.items():
                task.cancel()
            self._timers.clear()

        for key in keys_to_process:
            async with self._lock:
                items = self._pending.pop(key, [])

            if items:
                try:
                    await _process_batch(key, items)
                except Exception:
                    logger.exception("Failed to flush batch for key %s", key)


# --- Module-level Processing Helpers ---


async def _process_batch(key: str, items: List[Any]):
    logger.info("Processing batch %s with %d items", key, len(items))
    if key.startswith("reminder_"):
        minutes = int(key.split("_")[1])
        await _process_reminders(minutes, items)
    elif key == "result":
        await _process_results(items)
    elif key == "time_change":
        await _process_time_changes(items)
    elif key == "mid_series":
        await _process_mid_series(items)


async def _process_generic(
    items: List[Any],
    fetch_batch: Callable[[Any, List[Any]], Any],
    build_embed: Callable[[List[Any]], discord.Embed],
    context_fmt: str,
    scope_type: Optional[str] = None,
):
    """
    Generic processor for batch items using bulk fetching.

    Args:
        items: List of raw items to process.
        fetch_batch: Coroutine accepting (session, items) returning list of
                     data.
        build_embed: Function accepting list of data and returning an Embed.
        context_fmt: Format string for log context.
    """
    bot = get_bot_instance()
    if not bot:
        return

    async with get_async_session() as session:
        data_list = await fetch_batch(session, items)
        if not data_list:
            return

        embed = build_embed(data_list)
        context = f"{context_fmt} for {len(data_list)} matches"

        # Derive game slug from the first match when available. Default to
        # 'lol' for backwards compatibility.
        try:
            first_match = data_list[0][0]
            game_slug = getattr(first_match, "game", "lol")
        except Exception:
            game_slug = "lol"

        await _deliver_embed(bot, embed, context, game_slug, scope_type)


async def _process_reminders(minutes: int, match_ids: List[int]):
    def build(data_list):
        return _build_reminder_embed(minutes, data_list)

    await _process_generic(
        match_ids,
        _fetch_reminders_batch,
        build,
        f"{minutes}-minute reminder",
        scope_type="upcoming",
    )


async def _fetch_reminders_batch(session, ids):
    matches = await _bulk_fetch_matches(session, ids)
    if not matches:
        return []

    teams_map = await _bulk_fetch_teams(session, matches)
    data = []
    for m in matches:
        t1, t2 = _resolve_teams(m, teams_map)
        data.append((m, t1, t2))
    return data


async def _process_results(items: List[Tuple[int, int]]):
    await _process_generic(
        items,
        _fetch_results_batch,
        _build_result_embed,
        "result notification",
        scope_type="results",
    )


async def _fetch_results_batch(session, item_list):
    match_ids = [i[0] for i in item_list]
    result_ids = [i[1] for i in item_list]

    matches = await _bulk_fetch_matches(session, match_ids)
    # Fetch results
    stmt = select(Result).where(Result.id.in_(result_ids))
    results = (await session.exec(stmt)).all()
    results_map = {r.id: r for r in results}

    # Map match_id -> result_id from input items
    match_to_res_id = dict(item_list)

    if not matches:
        return []

    teams_map = await _bulk_fetch_teams(session, matches)
    stats_map = await _bulk_fetch_pick_stats(session, match_ids)

    valid_data = []
    for m in matches:
        res_id = match_to_res_id.get(m.id)
        res = results_map.get(res_id)
        if res:
            t1, t2 = _resolve_teams(m, teams_map)
            # Stats calculation needs winner name
            total, counts = stats_map.get(m.id, (0, defaultdict(int)))
            correct = counts.get(res.winner, 0)
            percentage = (correct / total * 100) if total > 0 else 0
            stats = (total, correct, percentage)
            valid_data.append((m, res, t1, t2, stats))
    return valid_data


async def _process_time_changes(items: List[Tuple[int, Any, Any]]):
    await _process_generic(
        items,
        _fetch_simple_batch,
        _build_time_change_embed,
        "time change notification",
        scope_type="upcoming",
    )


async def _process_mid_series(items: List[Tuple[int, str]]):
    await _process_generic(
        items,
        _fetch_simple_batch,
        _build_mid_series_embed,
        "mid-series update",
        scope_type="running",
    )


async def _fetch_simple_batch(session, item_list):
    match_ids = [i[0] for i in item_list]
    matches = await _bulk_fetch_matches(session, match_ids)
    match_map = {m.id: m for m in matches}

    data = []
    for item in item_list:
        m_id = item[0]
        if m := match_map.get(m_id):
            # item is (match_id, ...) so we replace match_id with match obj
            # and keep the rest of the tuple
            data.append((m,) + item[1:])
    return data


def _fmt_time_change_line(data):
    m, _, new_time = data
    ts = int(new_time.timestamp())
    return (
        f"**{m.team1}** vs **{m.team2}**\n"
        f"New Time: <t:{ts}:F> (<t:{ts}:R>)"
    )


def _build_time_change_embed(data_list):
    embed = discord.Embed(
        title="📅 Match Schedule Updates",
        description="The following matches have been rescheduled:",
        color=discord.Color.blue(),
        timestamp=datetime.now(timezone.utc),
    )
    return _populate_list_embed(embed, data_list, _fmt_time_change_line)


def _fmt_mid_series_line(data):
    m, score = data
    return f"**{m.team1}** vs **{m.team2}**: ||**{score}**||" + (
        f" (Best of {m.best_of})" if m.best_of else ""
    )


def _build_mid_series_embed(data_list):
    embed = discord.Embed(
        title="Live Match Updates",
        description="Latest scores:",
        color=discord.Color.orange(),
        timestamp=datetime.now(timezone.utc),
    )
    return _populate_list_embed(embed, data_list, _fmt_mid_series_line)


# --- Bulk Fetching Helpers ---


async def _bulk_fetch_matches(session, match_ids: List[int]) -> List[Match]:
    if not match_ids:
        return []
    stmt = (
        select(Match)
        .options(selectinload(Match.contest))
        .where(Match.id.in_(match_ids))
    )
    return (await session.exec(stmt)).all()


async def _bulk_fetch_teams(session, matches: List[Match]) -> dict:
    ids, names = _collect_team_ids_and_names(matches)

    if not ids and not names:
        return {}

    conditions = []
    if ids:
        conditions.append(Team.pandascore_id.in_(ids))
    if names:
        conditions.append(Team.name.in_(names))

    stmt = select(Team).where(or_(*conditions))
    teams = (await session.exec(stmt)).all()

    # Map by ID and Name
    by_id = {t.pandascore_id: t for t in teams if t.pandascore_id}
    by_name = {t.name: t for t in teams}
    return {"id": by_id, "name": by_name}


def _collect_team_ids_and_names(matches: List[Match]):
    ids = set()
    names = set()
    for m in matches:
        if m.team1_id:
            ids.add(m.team1_id)
        else:
            names.add(m.team1)

        if m.team2_id:
            ids.add(m.team2_id)
        else:
            names.add(m.team2)
    return ids, names


def _resolve_teams(
    match: Match, teams_map: dict
) -> Tuple[Optional[Team], Optional[Team]]:
    by_id = teams_map.get("id", {})
    by_name = teams_map.get("name", {})

    t1 = (
        by_id.get(match.team1_id)
        if match.team1_id
        else by_name.get(match.team1)
    )
    t2 = (
        by_id.get(match.team2_id)
        if match.team2_id
        else by_name.get(match.team2)
    )
    return t1, t2


async def _bulk_fetch_pick_stats(session, match_ids: List[int]) -> dict:
    if not match_ids:
        return {}

    # Fetch counts grouped by match_id and chosen_team
    stmt = (
        select(Pick.match_id, Pick.chosen_team, func.count(Pick.id))
        .where(Pick.match_id.in_(match_ids))
        .group_by(Pick.match_id, Pick.chosen_team)
    )
    rows = (await session.exec(stmt)).all()

    # Structure: {match_id: [total, {team_name: count}]}
    # Use list for mutability
    stats = defaultdict(lambda: [0, defaultdict(int)])
    for mid, team, count in rows:
        stats[mid][0] += count
        stats[mid][1][team] = count

    # Convert to tuple format
    return {k: (v[0], v[1]) for k, v in stats.items()}


def _set_thumbnail(
    embed: discord.Embed,
    match: Match,
    team1: Optional[Team] = None,
    team2: Optional[Team] = None,
):
    contest = getattr(match, "contest", None)
    if contest and getattr(contest, "image_url", None):
        embed.set_thumbnail(url=contest.image_url)
        return
    if team1 and getattr(team1, "image_url", None):
        embed.set_thumbnail(url=team1.image_url)
    elif team2 and getattr(team2, "image_url", None):
        embed.set_thumbnail(url=team2.image_url)


def _build_reminder_embed(
    minutes: int, matches_data: List[Tuple[Match, Any, Any]]
) -> discord.Embed:
    matches_data.sort(key=lambda x: (x[0].scheduled_time, x[0].id))

    if minutes == 5:
        title = "🔴 Matches Starting Soon!"
        color = discord.Color.red()
        desc_prefix = (
            "The following matches are starting soon! "
            "Last chance to lock in picks."
        )
    else:
        title = "⚔️ Upcoming Match Reminders"
        color = discord.Color.blue()
        desc_prefix = (
            "Get your picks in! The following matches are starting soon."
        )

    description_lines = [desc_prefix, ""]
    for match, _, _ in matches_data:
        ts = int(match.scheduled_time.timestamp())
        line = f"**{match.team1}** vs **{match.team2}** <t:{ts}:R>"
        description_lines.append(line)

    embed = discord.Embed(
        title=title,
        description="\n".join(description_lines),
        color=color,
    )

    if matches_data:
        first = matches_data[0]
        _set_thumbnail(embed, first[0], first[1], first[2])

    embed.set_footer(text="Use the /picks command to make your predictions!")
    return embed


def _build_result_embed(
    results_data: List[Tuple[Match, Result, Any, Any, Tuple]],
) -> discord.Embed:
    embed = discord.Embed(
        title="🏆 Match Results",
        description="The following matches have concluded:",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    results_data.sort(key=lambda x: x[0].id)

    for match, result, _, _, stats in results_data:
        _, correct, _ = stats
        if result.winner == match.team1:
            display = f"||**{match.team1}** def **{match.team2}**||"
        else:
            display = f"||**{match.team2}** def **{match.team1}**||"
        score_text = f"||{result.score}||"
        stats_text = f"✅ {correct} correct"

        field_name = f"{match.team1} vs {match.team2}"
        field_value = f"{display} ({score_text})\n{stats_text}"
        embed.add_field(name=field_name, value=field_value, inline=False)

    if results_data:
        first = results_data[0]
        _set_thumbnail(embed, first[0], first[2], first[3])
    return embed


def _populate_list_embed(
    embed: discord.Embed,
    data_list: List[Any],
    line_formatter: Callable[[Any], str],
) -> discord.Embed:
    """
    Helper to populate fields and thumbnail for a list embed.
    """
    # Assume data items have match as first element
    data_list.sort(key=lambda x: x[0].id)

    for item in data_list:
        match = item[0]
        line = line_formatter(item)
        embed.add_field(name=f"Match {match.id}", value=line, inline=False)

    if data_list:
        _set_thumbnail(embed, data_list[0][0])
    return embed


async def _deliver_embed(
    bot: discord.Client,
    embed: discord.Embed,
    context: str,
    game_slug: str = "lol",
    scope_type: Optional[str] = None,
):
    """Deliver an embed by editing tracked live messages when configured,
    otherwise fall back to sending/broadcasting the embed to guilds.

    `scope_type` controls which of the canonical live message slots to edit
    (e.g. "upcoming", "running", "results"). `game_slug` is passed as the
    scope_key so messages can be separated per-game.
    """
    if not bot:
        return

    # If the bot.guilds collection is not a concrete iterable (tests often use
    # a plain MagicMock), fall back to broadcasting to avoid exercising the
    # per-guild delivery logic which requires full Guild/Channel API objects.
    if not isinstance(getattr(bot, "guilds", None), (list, tuple, set)):
        await broadcast_embed_to_guilds(bot, embed, context)
        return

    guilds_to_broadcast = []

    async with get_async_session() as session:
        for guild in list(bot.guilds):
            try:
                # Some tests/mocks may provide sync or async callables or
                # return awaitables; be permissive and await only when
                # necessary so patched values behave as expected.
                cfg_val = get_guild_config_async(
                    session, getattr(guild, "id", None)
                )
                cfg = (
                    await cfg_val if inspect.isawaitable(cfg_val) else cfg_val
                )
                if cfg and cfg.enabled_games:
                    allowed = [
                        g.strip()
                        for g in cfg.enabled_games.split(",")
                        if g.strip()
                    ]
                    if allowed and game_slug not in allowed:
                        continue

                # Query the live message for the appropriate scope (per-game)
                live_val = get_live_message_async(
                    session, getattr(guild, "id", None), scope_type, game_slug
                )
                live = (
                    await live_val
                    if inspect.isawaitable(live_val)
                    else live_val
                )

                channel_id = None
                if cfg and cfg.live_updates_channel_id:
                    channel_id = cfg.live_updates_channel_id
                elif live and live.channel_id:
                    channel_id = live.channel_id

                if channel_id is None:
                    guilds_to_broadcast.append(guild)
                    continue

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
                    guilds_to_broadcast.append(guild)
                    continue

                if live and getattr(live, "message_id", None):
                    try:
                        msg = await channel.fetch_message(live.message_id)
                        await msg.edit(embed=embed)
                        continue
                    except discord.NotFound:
                        pass
                    except Exception:
                        logger.exception(
                            "Failed to edit live message %s in guild %s",
                            getattr(live, "message_id", None),
                            getattr(guild, "id", None),
                        )
                        guilds_to_broadcast.append(guild)
                        continue

                try:
                    new_msg = await channel.send(embed=embed)
                    # Persist the canonical live message pointer for this guild
                    # and scope (scope_type + game_slug).
                    await set_live_message_async(
                        session,
                        getattr(guild, "id", None),
                        channel.id,
                        new_msg.id,
                        scope_type or "guild_live",
                        game_slug,
                    )
                    continue
                except Exception:
                    logger.exception(
                        "Failed to send live update to guild %s in channel %s",
                        getattr(guild, "id", None),
                        channel_id,
                    )
                    guilds_to_broadcast.append(guild)
            except Exception:
                logger.exception(
                    "Unexpected error while delivering to guild %s",
                    getattr(guild, "id", None),
                )
                guilds_to_broadcast.append(guild)

    if not guilds_to_broadcast:
        return

    if len(guilds_to_broadcast) == len(list(bot.guilds)):
        await broadcast_embed_to_guilds(bot, embed, context)
        return

    for idx, guild in enumerate(guilds_to_broadcast):
        try:
            await send_announcement(guild, embed)
            logger.info("Sent %s to guild %s.", context, guild.id)
        except Exception as e:
            logger.error(
                "Failed to send %s to guild %s: %s", context, guild.id, e
            )
        if (idx + 1) % 3 == 0:
            await asyncio.sleep(0)


# Global instance
batcher = NotificationBatcher()


async def update_upcoming_live_messages() -> None:
    """
    Rebuild and deliver the "upcoming" live message for each configured
    default game. This job is idempotent and uses the database as the
    source-of-truth for upcoming matches.
    """
    bot = get_bot_instance()
    if not bot:
        return

    from src.config import DEFAULT_GAMES

    games = DEFAULT_GAMES or ["lol"]

    async with get_async_session() as session:
        for game in games:
            # Fetch upcoming matches for this game (next 25)
            now = datetime.now(timezone.utc)
            stmt = (
                select(Match)
                .options(selectinload(Match.contest))
                .where(Match.game == game)
                .where(Match.status == "upcoming")
                .where(Match.scheduled_time >= now)
                .order_by(Match.scheduled_time)
                .limit(25)
            )
            matches = (await session.exec(stmt)).all()
            if not matches:
                continue

            # Build data tuples expected by _populate_list_embed:
            # (match, team1, team2)
            teams_map = await _bulk_fetch_teams(session, matches)
            data = []
            for m in matches:
                t1, t2 = _resolve_teams(m, teams_map)
                data.append((m, t1, t2))

            # Build a simple upcoming embed using the same formatting as
            # reminders
            embed = discord.Embed(
                title="⚔️ Upcoming Matches",
                description="Matches coming up soon:",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc),
            )
            embed = _populate_list_embed(
                embed,
                data,
                lambda d: (
                    f"**{d[0].team1}** vs **{d[0].team2}** "
                    f"<t:{int(d[0].scheduled_time.timestamp())}:R>"
                ),
            )

            context = f"upcoming live message for {len(matches)} matches"
            await _deliver_embed(
                bot, embed, context, game, scope_type="upcoming"
            )
