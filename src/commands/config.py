"""Guild configuration commands: view / set channels / set games

Provides a minimal `/config` command group so guild admins can view and
update persisted `GuildConfig` fields such as announcement/live channel IDs
and enabled games (comma-separated slugs). Permission checks allow guild
owners or members with `manage_guild`, falling back to the environment
`ADMIN_IDS` list via `is_admin()`.
"""

import logging

import discord
from discord import app_commands
from discord.ext import commands

from src.auth import is_admin
from src.crud import get_guild_config_async, upsert_guild_config_async
from src.db import get_async_session

logger = logging.getLogger(__name__)


config_group = app_commands.Group(
    name="config", description="Manage guild configuration"
)


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
        if not cfg:
            await interaction.response.send_message(
                "No configuration found for this guild.", ephemeral=True
            )
            return

        enabled = cfg.enabled_games or "(none)"
        ann = cfg.announcement_channel_id or "(none)"
        live = cfg.live_updates_channel_id or "(none)"
        message = (
            f"Announcement channel: {ann}\n"
            f"Live updates channel: {live}\n"
            f"Enabled games: {enabled}"
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
@app_commands.describe(
    kind="Which channel to set: announcement or live",
    channel="Text channel to use",
)
async def set_channel(
    interaction: discord.Interaction, kind: str, channel: discord.TextChannel
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

    if kind.lower() in ("announcement", "announce"):
        field = "announcement_channel_id"
    elif kind.lower() in ("live", "live_updates"):
        field = "live_updates_channel_id"
    else:
        await interaction.followup.send(
            "Unknown channel kind. Use 'announcement' or 'live'.",
            ephemeral=True,
        )
        return

    try:
        await _update_guild_channel(guild_id, field, channel.id)
        await interaction.followup.send(
            "Configuration updated.", ephemeral=True
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

    normalized = ",".join(
        [g.strip().lower() for g in games.split(",") if g.strip()]
    )
    try:
        async with get_async_session() as session:
            await upsert_guild_config_async(
                session, guild_id, enabled_games=normalized
            )
        await interaction.followup.send(
            f"Enabled games set to: {normalized}", ephemeral=True
        )
    except Exception:
        logger.exception("Failed updating enabled_games for %s", guild_id)
        await interaction.followup.send(
            "Failed to update enabled games.", ephemeral=True
        )


async def setup(bot: commands.Bot):
    bot.tree.add_command(config_group)
