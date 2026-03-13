import logging
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Optional

import discord
from sqlalchemy.orm import selectinload
from sqlmodel import select

from src.bot_instance import get_bot_instance
from src.config import DEFAULT_GAMES
from src.crud import (
    get_guild_config_async,
    get_live_message_async,
    set_live_message_async,
)
from src.db import get_async_session
from src.models import Match, Result
from src.parsers.factory import get_supported_game_slugs

logger = logging.getLogger(__name__)

LIVE_MESSAGE_SCOPES = ("upcoming", "running", "results")
UPCOMING_WINDOW_DAYS = 5
UPCOMING_MATCH_LIMIT = 25
RUNNING_MATCH_LIMIT = 25
RESULTS_MATCH_LIMIT = 10
GAME_DISPLAY_NAMES = {
    "lol": "LoL",
    "cs2": "CS2",
}


def normalize_enabled_games(
    raw_games: Optional[str],
    *,
    default_games: Optional[Sequence[str]] = None,
) -> list[str]:
    supported = set(get_supported_game_slugs())
    defaults = list(default_games or DEFAULT_GAMES or ("lol",))
    if not raw_games:
        return [game for game in defaults if game in supported] or ["lol"]

    seen = set()
    normalized = []
    for raw in raw_games.split(","):
        game = raw.strip().lower()
        if not game or game not in supported or game in seen:
            continue
        seen.add(game)
        normalized.append(game)
    return (
        normalized
        or [game for game in defaults if game in supported]
        or ["lol"]
    )


def format_enabled_games(raw_games: Optional[str]) -> str:
    return ", ".join(
        _game_display_name(game) for game in normalize_enabled_games(raw_games)
    )


async def refresh_all_live_messages() -> None:
    bot = get_bot_instance()
    if not bot:
        return

    guilds = getattr(bot, "guilds", None)
    if not isinstance(guilds, (list, tuple, set)):
        return

    for guild in guilds:
        await refresh_live_messages_for_guild(guild)


async def refresh_live_messages_for_games(games: Iterable[str]) -> None:
    bot = get_bot_instance()
    if not bot:
        return

    guilds = getattr(bot, "guilds", None)
    if not isinstance(guilds, (list, tuple, set)):
        return

    target_games = {
        game.strip().lower()
        for game in games
        if game and game.strip().lower() in get_supported_game_slugs()
    }
    if not target_games:
        return

    for guild in guilds:
        await refresh_live_messages_for_guild(guild, games=target_games)


async def refresh_live_messages_for_guild(
    guild: discord.Guild, games: Optional[Iterable[str]] = None
) -> None:
    async with get_async_session() as session:
        cfg = await get_guild_config_async(session, getattr(guild, "id", None))
        enabled_games = set(
            normalize_enabled_games(getattr(cfg, "enabled_games", None))
        )
        target_games = enabled_games
        if games is not None:
            requested = {game.strip().lower() for game in games if game}
            target_games = enabled_games.intersection(requested)

        if not target_games:
            return

        for game in sorted(target_games):
            for scope in LIVE_MESSAGE_SCOPES:
                try:
                    await _refresh_guild_scope(
                        session,
                        guild,
                        cfg,
                        game,
                        scope,
                    )
                except Exception:
                    logger.exception(
                        "Failed refreshing %s live message for guild %s (%s)",
                        scope,
                        getattr(guild, "id", None),
                        game,
                    )


async def _refresh_guild_scope(
    session, guild, cfg, game: str, scope: str
) -> None:
    live_record = await get_live_message_async(session, guild.id, scope, game)
    channel = await _resolve_live_channel(guild, cfg, live_record)
    if channel is None:
        logger.debug(
            "Skipping %s live message for guild %s (%s): no writable channel",
            scope,
            getattr(guild, "id", None),
            game,
        )
        return

    embed = await _build_live_message_embed(session, game, scope)
    await _edit_or_create_live_message(
        session,
        guild,
        channel,
        live_record,
        embed,
        scope,
        game,
    )


def _get_preferred_live_channel_id(cfg, live_record) -> Optional[int]:
    if cfg and getattr(cfg, "live_updates_channel_id", None):
        return cfg.live_updates_channel_id
    if cfg and getattr(cfg, "announcement_channel_id", None):
        return cfg.announcement_channel_id
    if live_record and getattr(live_record, "channel_id", None):
        return live_record.channel_id
    return None


async def _resolve_live_channel(guild, cfg, live_record):
    channel_id = _get_preferred_live_channel_id(cfg, live_record)
    if channel_id is None:
        return None

    try:
        channel = guild.get_channel(channel_id)
        if channel is None:
            channel = await guild.fetch_channel(channel_id)
    except Exception:
        logger.exception(
            "Failed resolving live message channel %s for guild %s",
            channel_id,
            getattr(guild, "id", None),
        )
        return None

    if not _channel_supports_live_messages(channel, guild):
        return None
    return channel


def _channel_supports_live_messages(channel, guild) -> bool:
    try:
        if not hasattr(channel, "permissions_for"):
            return True
        member = getattr(guild, "me", None)
        if member is None:
            return True
        perms = channel.permissions_for(member)
        return bool(perms.send_messages and perms.embed_links)
    except Exception:
        logger.exception(
            "Failed checking channel permissions for guild %s",
            getattr(guild, "id", None),
        )
        return False


async def _edit_or_create_live_message(
    session,
    guild,
    channel,
    live_record,
    embed: discord.Embed,
    scope: str,
    game: str,
) -> None:
    should_try_edit = bool(
        live_record
        and getattr(live_record, "message_id", None)
        and getattr(live_record, "channel_id", None)
        == getattr(channel, "id", None)
    )
    if should_try_edit:
        try:
            message = await channel.fetch_message(live_record.message_id)
            await message.edit(embed=embed)
            await set_live_message_async(
                session,
                guild.id,
                channel.id,
                message.id,
                scope,
                game,
            )
            return
        except discord.NotFound:
            logger.info(
                "Tracked live message %s missing in guild %s; recreating",
                getattr(live_record, "message_id", None),
                guild.id,
            )
        except Exception:
            logger.exception(
                "Failed editing %s live message for guild %s (%s)",
                scope,
                guild.id,
                game,
            )
            return

    try:
        message = await channel.send(embed=embed)
    except Exception:
        logger.exception(
            "Failed creating %s live message for guild %s (%s) in channel %s",
            scope,
            guild.id,
            game,
            getattr(channel, "id", None),
        )
        return

    await set_live_message_async(
        session,
        guild.id,
        channel.id,
        message.id,
        scope,
        game,
    )


async def _build_live_message_embed(
    session, game: str, scope: str
) -> discord.Embed:
    if scope == "upcoming":
        matches = await _fetch_upcoming_matches(session, game)
        return _build_upcoming_embed(game, matches)
    if scope == "running":
        matches = await _fetch_running_matches(session, game)
        return _build_running_embed(game, matches)
    results = await _fetch_recent_results(session, game)
    return _build_results_embed(game, results)


async def _fetch_upcoming_matches(session, game: str) -> list[Match]:
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=UPCOMING_WINDOW_DAYS)
    stmt = (
        select(Match)
        .options(selectinload(Match.contest))
        .where(Match.game == game)
        .where(Match.status == "not_started")
        .where(Match.scheduled_time >= now)
        .where(Match.scheduled_time <= cutoff)
        .order_by(Match.scheduled_time, Match.id)
        .limit(UPCOMING_MATCH_LIMIT)
    )
    return list((await session.exec(stmt)).all())


async def _fetch_running_matches(session, game: str) -> list[Match]:
    stmt = (
        select(Match)
        .options(selectinload(Match.contest))
        .where(Match.game == game)
        .where(Match.status == "running")
        .order_by(Match.scheduled_time, Match.id)
        .limit(RUNNING_MATCH_LIMIT)
    )
    return list((await session.exec(stmt)).all())


async def _fetch_recent_results(
    session, game: str
) -> list[tuple[Match, Result]]:
    stmt = (
        select(Match, Result)
        .join(Result, Result.match_id == Match.id)
        .options(selectinload(Match.contest))
        .where(Match.game == game)
        .order_by(Match.scheduled_time.desc(), Match.id.desc())
        .limit(RESULTS_MATCH_LIMIT)
    )
    return list((await session.exec(stmt)).all())


def _build_upcoming_embed(
    game: str, matches: Sequence[Match]
) -> discord.Embed:
    title = f"{_game_display_name(game)} Upcoming Matches"
    if not matches:
        description = "No matches are scheduled in the next 5 days."
    else:
        description = "\n\n".join(
            _format_upcoming_line(match) for match in matches
        )
    return _build_live_embed(title, description, discord.Color.blue())


def _build_running_embed(game: str, matches: Sequence[Match]) -> discord.Embed:
    title = f"{_game_display_name(game)} Live Matches"
    if not matches:
        description = "No matches are currently live."
    else:
        description = "\n\n".join(
            _format_running_line(match) for match in matches
        )
    return _build_live_embed(title, description, discord.Color.orange())


def _build_results_embed(
    game: str, results: Sequence[tuple[Match, Result]]
) -> discord.Embed:
    title = f"{_game_display_name(game)} Recent Results"
    if not results:
        description = "No recent results to show yet."
    else:
        description = "\n\n".join(
            _format_result_line(match, result) for match, result in results
        )
    return _build_live_embed(title, description, discord.Color.gold())


def _build_live_embed(
    title: str, description: str, color: discord.Color
) -> discord.Embed:
    return discord.Embed(
        title=title,
        description=description,
        color=color,
        timestamp=datetime.now(timezone.utc),
    )


def _format_upcoming_line(match: Match) -> str:
    ts = int(match.scheduled_time.timestamp())
    contest_name = _contest_name(match)
    return (
        f"**{match.team1}** vs **{match.team2}**\n"
        f"{contest_name} • <t:{ts}:F> • "
        f"<t:{ts}:R>{_best_of_suffix(match)}"
    )


def _format_running_line(match: Match) -> str:
    score = match.last_announced_score or "Live now"
    contest_name = _contest_name(match)
    return (
        f"**{match.team1}** vs **{match.team2}**\n"
        f"{contest_name} • ||{score}||{_best_of_suffix(match)}"
    )


def _format_result_line(match: Match, result: Result) -> str:
    contest_name = _contest_name(match)
    winner = result.winner or "Winner TBD"
    score = result.score or "Final"
    return (
        f"**{match.team1}** vs **{match.team2}**\n"
        f"{contest_name} • Winner: ||{winner}|| • "
        f"Score: ||{score}||"
    )


def _contest_name(match: Match) -> str:
    contest = getattr(match, "contest", None)
    return getattr(contest, "name", "Unknown contest")


def _best_of_suffix(match: Match) -> str:
    if getattr(match, "best_of", None):
        return f" • Bo{match.best_of}"
    return ""


def _game_display_name(game: str) -> str:
    return GAME_DISPLAY_NAMES.get(game, game.upper())
