"""Guild configuration commands: view / set channels / manage games."""

import logging
from collections.abc import Iterable
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from src.auth import is_admin
from src.crud import get_guild_config_async, upsert_guild_config_async
from src.db import get_async_session
from src.live_messages import format_enabled_games
from src.parsers.factory import get_supported_game_slugs

logger = logging.getLogger(__name__)


config_group = app_commands.Group(
    name="config", description="Manage guild configuration"
)

CHANNEL_KIND_CHOICES = [
    app_commands.Choice(name="Announcement", value="announcement"),
    app_commands.Choice(name="Live Updates", value="live_updates"),
]


def _format_channel(channel_id: Optional[int]) -> str:
    if not channel_id:
        return "(not set)"
    return f"<#{channel_id}>"


def _supported_games_text() -> str:
    return ", ".join(game.upper() for game in get_supported_game_slugs())


def _normalize_games_list(raw_games: Iterable[str]) -> list[str]:
    supported = set(get_supported_game_slugs())
    normalized = []
    seen = set()
    for raw in raw_games:
        game = raw.strip().lower()
        if not game or game in seen:
            continue
        if game not in supported:
            raise ValueError(game)
        seen.add(game)
        normalized.append(game)
    return normalized


def _filter_supported_games(raw_games: Iterable[str]) -> list[str]:
    supported = set(get_supported_game_slugs())
    normalized = []
    seen = set()
    for raw in raw_games:
        game = raw.strip().lower()
        if not game or game in seen:
            continue
        if game not in supported:
            continue
        seen.add(game)
        normalized.append(game)
    return normalized


def _serialize_games(games: Iterable[str]) -> Optional[str]:
    normalized = _normalize_games_list(games)
    if not normalized:
        return None
    return ",".join(normalized)


@config_group.command(
    name="view", description="View this guild's configuration"
)
async def view(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message(
            "This command must be run in a server.", ephemeral=True
        )
        return

    async with get_async_session() as session:
        cfg = await get_guild_config_async(session, interaction.guild.id)
        enabled = format_enabled_games(getattr(cfg, "enabled_games", None))
        ann = _format_channel(getattr(cfg, "announcement_channel_id", None))
        live = _format_channel(getattr(cfg, "live_updates_channel_id", None))
        message = (
            f"Announcement channel: {ann}\n"
            f"Live updates channel: {live}\n"
            f"Enabled games: {enabled}\n"
            f"Supported games: {_supported_games_text()}"
        )
        await interaction.response.send_message(message, ephemeral=True)


async def _has_config_permission(interaction: discord.Interaction) -> bool:
    """Return True if invoking user may change guild configuration."""
    try:
        if (
            getattr(interaction.user, "guild_permissions", None)
            and interaction.user.guild_permissions.manage_guild
        ):
            return True
        if interaction.guild and interaction.user == interaction.guild.owner:
            return True
    except Exception:
        logger.exception(
            "Local guild permission check failed for user %s in guild %s",
            getattr(interaction.user, "id", None),
            getattr(interaction.guild, "id", None),
        )

    try:
        allowed = await is_admin().predicate(interaction)
        if allowed:
            return True
    except Exception:
        logger.exception(
            "Admin fallback permission check failed for user %s in guild %s",
            getattr(interaction.user, "id", None),
            getattr(interaction.guild, "id", None),
        )

    await interaction.response.send_message(
        "You do not have permission to run this command.", ephemeral=True
    )
    return False


async def _update_guild_channel(guild_id: int, field: str, value: int) -> None:
    async with get_async_session() as session:
        await upsert_guild_config_async(session, guild_id, **{field: value})


@config_group.command(
    name="set_channel", description="Set announcement or live updates channel"
)
@app_commands.choices(kind=CHANNEL_KIND_CHOICES)
@app_commands.describe(
    kind="Which channel setting to update",
    channel="Text channel to use",
)
async def set_channel(
    interaction: discord.Interaction,
    kind: app_commands.Choice[str],
    channel: discord.TextChannel,
):
    if not await _has_config_permission(interaction):
        return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send(
            "This command must be run in a server.", ephemeral=True
        )
        return

    if kind.value == "announcement":
        field = "announcement_channel_id"
        label = "announcement"
    elif kind.value == "live_updates":
        field = "live_updates_channel_id"
        label = "live updates"
    else:
        await interaction.followup.send(
            "Unknown channel kind selected.",
            ephemeral=True,
        )
        return

    try:
        await _update_guild_channel(guild_id, field, channel.id)
        await interaction.followup.send(
            f"Updated {label} channel to {channel.mention}.",
            ephemeral=True,
        )
    except Exception:
        logger.exception("Failed updating guild config for %s", guild_id)
        await interaction.followup.send(
            "Failed to update configuration.", ephemeral=True
        )


@config_group.command(
    name="set_games",
    description="Replace enabled games with a comma-separated list",
)
async def set_games(interaction: discord.Interaction, games: str):
    if not await _has_config_permission(interaction):
        return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send(
            "This command must be run in a server.", ephemeral=True
        )
        return

    try:
        normalized = _serialize_games(games.split(","))
    except ValueError:
        await interaction.followup.send(
            (
                "Invalid games list. Use supported slugs: "
                f"{_supported_games_text()}"
            ),
            ephemeral=True,
        )
        return

    if normalized is None:
        await interaction.followup.send(
            "Choose at least one supported game.",
            ephemeral=True,
        )
        return

    try:
        async with get_async_session() as session:
            await upsert_guild_config_async(
                session, guild_id, enabled_games=normalized
            )
        await interaction.followup.send(
            f"Enabled games set to: {format_enabled_games(normalized)}",
            ephemeral=True,
        )
    except Exception:
        logger.exception("Failed updating enabled_games for %s", guild_id)
        await interaction.followup.send(
            "Failed to update enabled games.", ephemeral=True
        )


async def _load_enabled_games(guild_id: int) -> Optional[list[str]]:
    async with get_async_session() as session:
        cfg = await get_guild_config_async(session, guild_id)
        raw_games = getattr(cfg, "enabled_games", None)
    if raw_games is None:
        return None
    return _filter_supported_games((raw_games or "").split(","))


async def _update_enabled_games(
    guild_id: int,
    games: Iterable[str],
) -> str:
    normalized = _serialize_games(games)
    async with get_async_session() as session:
        await upsert_guild_config_async(
            session, guild_id, enabled_games=normalized
        )
    return normalized or ""


def _normalize_persisted_games(
    games: Optional[Iterable[str]],
) -> list[str]:
    return _filter_supported_games(games or [])


@config_group.command(
    name="add_game",
    description="Enable one supported game for this guild",
)
@app_commands.describe(game="Game to enable")
async def add_game(interaction: discord.Interaction, game: str):
    if not await _has_config_permission(interaction):
        return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send(
            "This command must be run in a server.", ephemeral=True
        )
        return

    normalized_game = game.strip().lower()
    if normalized_game not in get_supported_game_slugs():
        await interaction.followup.send(
            f"Unsupported game. Choose from: {_supported_games_text()}",
            ephemeral=True,
        )
        return

    enabled_games = _normalize_persisted_games(
        await _load_enabled_games(guild_id)
    )
    if normalized_game in enabled_games:
        await interaction.followup.send(
            f"{format_enabled_games(game)} is already enabled.",
            ephemeral=True,
        )
        return

    stored = await _update_enabled_games(
        guild_id,
        [*enabled_games, normalized_game],
    )
    await interaction.followup.send(
        f"Enabled games: {format_enabled_games(stored)}",
        ephemeral=True,
    )


@config_group.command(
    name="remove_game",
    description="Disable one supported game for this guild",
)
@app_commands.describe(game="Game to disable")
async def remove_game(interaction: discord.Interaction, game: str):
    if not await _has_config_permission(interaction):
        return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send(
            "This command must be run in a server.", ephemeral=True
        )
        return

    normalized_game = game.strip().lower()
    if normalized_game not in get_supported_game_slugs():
        await interaction.followup.send(
            f"Unsupported game. Choose from: {_supported_games_text()}",
            ephemeral=True,
        )
        return

    enabled_games = _normalize_persisted_games(
        await _load_enabled_games(guild_id)
    )
    if normalized_game not in enabled_games:
        await interaction.followup.send(
            f"{format_enabled_games(game)} is not enabled.",
            ephemeral=True,
        )
        return

    remaining_games = [
        enabled_game
        for enabled_game in enabled_games
        if enabled_game != normalized_game
    ]
    stored = await _update_enabled_games(guild_id, remaining_games)
    message = (
        f"Enabled games: {format_enabled_games(stored)}"
        if stored
        else "No games are currently enabled for this guild."
    )
    await interaction.followup.send(message, ephemeral=True)


@set_games.autocomplete("games")
async def _games_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    _ = interaction
    supported = list(get_supported_game_slugs())
    entries = [item.strip() for item in current.split(",")]
    prefix = ",".join(entry for entry in entries[:-1] if entry)
    current_token = entries[-1].strip().lower() if entries else ""
    already_selected = {
        entry.strip().lower() for entry in entries[:-1] if entry
    }

    choices = []
    for game in supported:
        if game in already_selected:
            continue
        if current_token and not game.startswith(current_token):
            continue
        value = f"{prefix},{game}" if prefix else game
        choices.append(
            app_commands.Choice(
                name=format_enabled_games(game),
                value=value,
            )
        )
    return choices[:25]


async def _single_game_autocomplete(
    current: str,
) -> list[app_commands.Choice[str]]:
    token = current.strip().lower()
    choices = []
    for game in get_supported_game_slugs():
        if token and not game.startswith(token):
            continue
        choices.append(
            app_commands.Choice(
                name=format_enabled_games(game),
                value=game,
            )
        )
    return choices[:25]


@add_game.autocomplete("game")
async def _add_game_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    _ = interaction
    return await _single_game_autocomplete(current)


@remove_game.autocomplete("game")
async def _remove_game_autocomplete(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    _ = interaction
    return await _single_game_autocomplete(current)


async def setup(bot: commands.Bot):
    bot.tree.add_command(config_group)
