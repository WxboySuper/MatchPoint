"""Guild configuration commands: view / set channels / set games."""

import logging
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
    """Return True if invoking user may change guild configuration.

    Allows guild owners, members with manage_guild, or global admins via
    is_admin(). Sends an ephemeral response when the check fails.
    """
    # Check local guild permissions/owner
    try:
        if (
            getattr(interaction.user, "guild_permissions", None)
            and interaction.user.guild_permissions.manage_guild
        ):
            return True
        if interaction.guild and interaction.user == interaction.guild.owner:
            return True
    except Exception:
        # Be conservative and fall through to admin check
        logger.exception(
            "Local guild permission check failed for user %s in guild %s",
            getattr(interaction.user, "id", None),
            getattr(interaction.guild, "id", None),
        )
        pass

    # Fallback to global admin list
    try:
        allowed = await is_admin().predicate(interaction)
        if allowed:
            return True
    except Exception:
        # treat any error as not permitted
        logger.exception(
            "Admin fallback permission check failed for user %s in guild %s",
            getattr(interaction.user, "id", None),
            getattr(interaction.guild, "id", None),
        )
        pass

    await interaction.response.send_message(
        "You do not have permission to run this command.", ephemeral=True
    )
    return False


async def _update_guild_channel(guild_id: int, field: str, value: int) -> None:
    """Update a single channel field for the guild config.

    This wraps the DB session and upsert call used by set_channel.
    """
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
    # Permission check
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
    description="Enable comma-separated game slugs (e.g. 'lol,cs2')",
)
async def set_games(interaction: discord.Interaction, games: str):
    # Permission check (same as other commands)
    if not await _has_config_permission(interaction):
        return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send(
            "This command must be run in a server.", ephemeral=True
        )
        return

    normalized = _normalize_games_value(games)
    if normalized is None:
        await interaction.followup.send(
            "Invalid games list. Use supported slugs: "
            f"{_supported_games_text()}",
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


def _normalize_games_value(raw_games: str) -> Optional[str]:
    supported = set(get_supported_game_slugs())
    normalized = []
    seen = set()
    for raw in raw_games.split(","):
        game = raw.strip().lower()
        if not game:
            continue
        if game not in supported:
            return None
        if game in seen:
            continue
        seen.add(game)
        normalized.append(game)

    if not normalized:
        return None
    return ",".join(normalized)


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


async def setup(bot: commands.Bot):
    bot.tree.add_command(config_group)
