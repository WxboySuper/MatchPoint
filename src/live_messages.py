import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
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
UPCOMING_MATCH_LIMIT = 10
RUNNING_MATCH_LIMIT = 25
RESULTS_MATCH_LIMIT = 10
RUNNING_MATCH_STALE_HOURS = 24
NON_UPCOMING_STATUSES = ("running", "finished", "canceled", "postponed")
GAME_DISPLAY_NAMES = {
    "lol": "LoL",
    "cs2": "CS2",
}


@dataclass(frozen=True)
class LiveMessageScope:
    guild: discord.Guild
    game: str
    scope: str


@dataclass(frozen=True)
class LiveMessageTarget:
    channel: object
    live_record: Optional[object]
    scope: LiveMessageScope


@dataclass(frozen=True)
class LiveEmbedSpec:
    title_suffix: str
    empty_description: str
    formatter: object
    color: discord.Color


def normalize_enabled_games(
    raw_games: Optional[str],
    *,
    default_games: Optional[Sequence[str]] = None,
) -> list[str]:
    supported = set(get_supported_game_slugs())
    defaults = _get_default_supported_games(default_games, supported)
    if raw_games is None:
        return defaults
    return _parse_supported_games(raw_games, supported)


def format_enabled_games(raw_games: Optional[str]) -> str:
    return ", ".join(
        _game_display_name(game) for game in normalize_enabled_games(raw_games)
    )


async def refresh_all_live_messages() -> None:
    bot = get_bot_instance()
    if not bot:
        return

    guilds = getattr(bot, "guilds", None)
    if guilds is None:
        return

    for guild in guilds:
        await refresh_live_messages_for_guild(guild)


async def refresh_live_messages_for_games(games: Iterable[str]) -> None:
    bot = get_bot_instance()
    if not bot:
        return

    guilds = getattr(bot, "guilds", None)
    if guilds is None:
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
    cfg = await _load_guild_config(guild)
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
                    cfg,
                    LiveMessageScope(guild=guild, game=game, scope=scope),
                )
            except Exception:
                logger.exception(
                    "Failed refreshing %s live message for guild %s (%s)",
                    scope,
                    getattr(guild, "id", None),
                    game,
                )


async def _refresh_guild_scope(
    cfg,
    live_scope: LiveMessageScope,
) -> None:
    live_record = await _load_live_record(live_scope)
    channel = await _resolve_live_channel(
        live_scope.guild,
        cfg,
        live_record,
    )
    if channel is None:
        logger.debug(
            "Skipping %s live message for guild %s (%s): no writable channel",
            live_scope.scope,
            getattr(live_scope.guild, "id", None),
            live_scope.game,
        )
        return

    embed = await _build_live_message_embed(live_scope)
    await _edit_or_create_live_message(
        LiveMessageTarget(
            channel=channel,
            live_record=live_record,
            scope=live_scope,
        ),
        embed,
    )


async def _load_guild_config(guild: discord.Guild):
    async with get_async_session() as session:
        return await get_guild_config_async(
            session,
            getattr(guild, "id", None),
        )


async def _load_live_record(live_scope: LiveMessageScope):
    async with get_async_session() as session:
        return await get_live_message_async(
            session,
            live_scope.guild.id,
            live_scope.scope,
            live_scope.game,
        )


async def _persist_live_message_pointer(
    live_scope: LiveMessageScope, channel_id: int, message_id: int
) -> None:
    async with get_async_session() as session:
        await set_live_message_async(
            session,
            live_scope.guild.id,
            channel_id,
            message_id,
            live_scope.scope,
            live_scope.game,
        )


def _get_default_supported_games(
    default_games: Optional[Sequence[str]],
    supported: set[str],
) -> list[str]:
    defaults = list(default_games or DEFAULT_GAMES or ("lol",))
    filtered = [game for game in defaults if game in supported]
    return filtered or ["lol"]


def _parse_supported_games(
    raw_games: str,
    supported: set[str],
) -> list[str]:
    seen = set()
    normalized = []
    for raw in raw_games.split(","):
        game = raw.strip().lower()
        if not game or game in seen:
            continue
        if game not in supported:
            continue
        seen.add(game)
        normalized.append(game)
    return normalized


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
    target: LiveMessageTarget,
    embed: discord.Embed,
) -> None:
    if _can_edit_tracked_message(target):
        try:
            message = await target.channel.fetch_message(
                target.live_record.message_id
            )
            await message.edit(embed=embed)
            await _persist_live_message_pointer(
                target.scope,
                target.channel.id,
                message.id,
            )
            return
        except discord.NotFound:
            logger.info(
                "Tracked live message %s missing in guild %s; recreating",
                getattr(target.live_record, "message_id", None),
                target.scope.guild.id,
            )
        except Exception:
            logger.exception(
                "Failed editing %s live message for guild %s (%s)",
                target.scope.scope,
                target.scope.guild.id,
                target.scope.game,
            )
            return

    try:
        message = await target.channel.send(embed=embed)
    except Exception:
        logger.exception(
            "Failed creating %s live message for guild %s (%s) in channel %s",
            target.scope.scope,
            target.scope.guild.id,
            target.scope.game,
            getattr(target.channel, "id", None),
        )
        return

    await _persist_live_message_pointer(
        target.scope,
        target.channel.id,
        message.id,
    )


def _can_edit_tracked_message(target: LiveMessageTarget) -> bool:
    return bool(
        target.live_record
        and getattr(target.live_record, "message_id", None)
        and getattr(target.live_record, "channel_id", None)
        == getattr(target.channel, "id", None)
    )


async def _build_live_message_embed(
    live_scope: LiveMessageScope,
) -> discord.Embed:
    if live_scope.scope == "upcoming":
        matches = await _fetch_upcoming_matches(live_scope.game)
        return _build_upcoming_embed(live_scope.game, matches)
    if live_scope.scope == "running":
        matches = await _fetch_running_matches(live_scope.game)
        return _build_running_embed(live_scope.game, matches)
    results = await _fetch_recent_results(live_scope.game)
    return _build_results_embed(live_scope.game, results)


async def _fetch_upcoming_matches(game: str) -> list[Match]:
    now = datetime.now(timezone.utc)
    async with get_async_session() as session:
        stmt = (
            select(Match)
            .options(selectinload(Match.contest))
            .where(Match.game == game)
            .where(Match.scheduled_time >= now)
            .where(Match.status.notin_(NON_UPCOMING_STATUSES))
            .order_by(Match.scheduled_time, Match.id)
            .limit(UPCOMING_MATCH_LIMIT)
        )
        return list((await session.exec(stmt)).all())


async def _fetch_running_matches(game: str) -> list[Match]:
    cutoff = datetime.now(timezone.utc) - timedelta(
        hours=RUNNING_MATCH_STALE_HOURS
    )
    async with get_async_session() as session:
        stmt = (
            select(Match)
            .options(selectinload(Match.contest))
            .outerjoin(Result, Result.match_id == Match.id)
            .where(Match.game == game)
            .where(Match.status == "running")
            .where(Match.scheduled_time >= cutoff)
            .where(Result.id.is_(None))
            .order_by(Match.scheduled_time, Match.id)
            .limit(RUNNING_MATCH_LIMIT)
        )
        return list((await session.exec(stmt)).all())


async def _fetch_recent_results(game: str) -> list[tuple[Match, Result]]:
    async with get_async_session() as session:
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
    return _build_scoped_embed(
        game,
        matches,
        LiveEmbedSpec(
            title_suffix="Upcoming Matches",
            empty_description="No upcoming matches are scheduled.",
            formatter=_format_upcoming_line,
            color=discord.Color.blue(),
        ),
    )


def _build_running_embed(game: str, matches: Sequence[Match]) -> discord.Embed:
    return _build_scoped_embed(
        game,
        matches,
        LiveEmbedSpec(
            title_suffix="Live Matches",
            empty_description="No matches are currently live.",
            formatter=_format_running_line,
            color=discord.Color.orange(),
        ),
    )


def _build_results_embed(
    game: str, results: Sequence[tuple[Match, Result]]
) -> discord.Embed:
    return _build_scoped_embed(
        game,
        results,
        LiveEmbedSpec(
            title_suffix="Recent Results",
            empty_description="No recent results to show yet.",
            formatter=_format_result_entry,
            color=discord.Color.gold(),
        ),
    )


def _build_scoped_embed(
    game: str,
    entries: Sequence,
    spec: LiveEmbedSpec,
) -> discord.Embed:
    title = f"{_game_display_name(game)} {spec.title_suffix}"
    if not entries:
        description = spec.empty_description
    else:
        description = "\n\n".join(spec.formatter(entry) for entry in entries)
    return _build_live_embed(title, description, spec.color)


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


def _format_result_entry(entry: tuple[Match, Result]) -> str:
    match, result = entry
    return _format_result_line(match, result)


def _contest_name(match: Match) -> str:
    contest = getattr(match, "contest", None)
    return getattr(contest, "name", "Unknown contest")


def _best_of_suffix(match: Match) -> str:
    if getattr(match, "best_of", None):
        return f" • Bo{match.best_of}"
    return ""


def _game_display_name(game: str) -> str:
    return GAME_DISPLAY_NAMES.get(game, game.upper())
