"""Guild configuration commands: view / set channels / set games

Provides a minimal `/config` command group so guild admins can view and
update persisted `GuildConfig` fields such as announcement/live channel IDs
and enabled games (comma-separated slugs). Permission checks allow guild
owners or members with `manage_guild`, falling back to the environment
`ADMIN_IDS` list via `is_admin()`.
"""

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from src.auth import is_admin
from src.db import get_session
from src.crud import get_guild_config, upsert_guild_config

logger = logging.getLogger(__name__)


config_group = app_commands.Group(name="config", description="Manage guild configuration")


@config_group.command(name="view", description="View this guild's configuration")
async def view(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("This command must be run in a server.", ephemeral=True)
        return

    with get_session() as session:
        cfg = get_guild_config(session, interaction.guild.id)
        if not cfg:
            await interaction.response.send_message("No configuration found for this guild.", ephemeral=True)
            return

        enabled = cfg.enabled_games or "(none)"
        ann = cfg.announcement_channel_id or "(none)"
        live = cfg.live_updates_channel_id or "(none)"
        await interaction.response.send_message(
            f"Announcement channel: {ann}\nLive updates channel: {live}\nEnabled games: {enabled}",
            ephemeral=True,
        )


@config_group.command(name="set_channel", description="Set announcement or live updates channel")
@app_commands.describe(kind="Which channel to set: announcement or live", channel="Text channel to use")
async def set_channel(interaction: discord.Interaction, kind: str, channel: discord.TextChannel):
    # Permission check
    if not (interaction.user.guild_permissions.manage_guild or interaction.user == interaction.guild.owner):
        try:
            if not is_admin().predicate(interaction):
                await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
                return
        except Exception:
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send("This command must be run in a server.", ephemeral=True)
        return

    fields = {}
    if kind.lower() in ("announcement", "announce"):
        fields["announcement_channel_id"] = channel.id
    elif kind.lower() in ("live", "live_updates"):
        fields["live_updates_channel_id"] = channel.id
    else:
        await interaction.followup.send("Unknown channel kind. Use 'announcement' or 'live'.", ephemeral=True)
        return

    try:
        with get_session() as session:
            upsert_guild_config(session, guild_id, **fields)
        await interaction.followup.send("Configuration updated.", ephemeral=True)
    except Exception:
        logger.exception("Failed updating guild config for %s", guild_id)
        await interaction.followup.send("Failed to update configuration.", ephemeral=True)


@config_group.command(name="set_games", description="Enable comma-separated game slugs for this guild (e.g. 'lol,cs2')")
async def set_games(interaction: discord.Interaction, games: str):
    # Permission check (same as other commands)
    if not (interaction.user.guild_permissions.manage_guild or interaction.user == interaction.guild.owner):
        try:
            if not is_admin().predicate(interaction):
                await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
                return
        except Exception:
            await interaction.response.send_message("You do not have permission to run this command.", ephemeral=True)
            return

    await interaction.response.defer(ephemeral=True)
    guild_id = interaction.guild.id if interaction.guild else None
    if guild_id is None:
        await interaction.followup.send("This command must be run in a server.", ephemeral=True)
        return

    normalized = ",".join([g.strip().lower() for g in games.split(",") if g.strip()])
    try:
        with get_session() as session:
            upsert_guild_config(session, guild_id, enabled_games=normalized)
        await interaction.followup.send(f"Enabled games set to: {normalized}", ephemeral=True)
    except Exception:
        logger.exception("Failed updating enabled_games for %s", guild_id)
        await interaction.followup.send("Failed to update enabled games.", ephemeral=True)


async def setup(bot: commands.Bot):
    bot.tree.add_command(config_group)
