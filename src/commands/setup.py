import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from src.auth import is_admin
from src.crud import upsert_guild_config_async
from src.db import get_async_session

logger = logging.getLogger(__name__)


@app_commands.command(
    name="setup", description="Initial server setup for MatchPoint bot"
)
async def cmd_setup(
    interaction: discord.Interaction,
    announcement_channel: Optional[discord.TextChannel] = None,
    live_updates_channel: Optional[discord.TextChannel] = None,
    enable_auto_channel_creation: bool = True,
):
    """Configure announcement channels for the guild.

    This command is intended for server owners or admins.
    """
    # Permission check: allow guild owner or configured admin list
    if not interaction.guild:
        await interaction.response.send_message(
            "This command must be run in a server.", ephemeral=True
        )
        return

    can_manage = bool(
        getattr(interaction.user, "guild_permissions", None)
        and interaction.user.guild_permissions.manage_guild
    )
    if not (can_manage or interaction.user == interaction.guild.owner):
        # Try environment admin fallback
        try:
            if not await is_admin().predicate(interaction):
                await interaction.response.send_message(
                    "You do not have permission to run setup.", ephemeral=True
                )
                return
        except Exception:
            await interaction.response.send_message(
                "You do not have permission to run setup.", ephemeral=True
            )
            return

    await interaction.response.defer(ephemeral=True)
    _ = enable_auto_channel_creation

    guild_id = interaction.guild.id

    # Persist configuration
    try:
        async with get_async_session() as session:
            await upsert_guild_config_async(
                session,
                guild_id,
                announcement_channel_id=getattr(
                    announcement_channel, "id", None
                ),
                live_updates_channel_id=getattr(
                    live_updates_channel, "id", None
                ),
                setup_completed=True,
            )
        await interaction.followup.send(
            "Setup complete. Configuration saved.", ephemeral=True
        )
    except Exception:
        logger.exception("Failed saving guild configuration for %s", guild_id)
        await interaction.followup.send(
            "Failed to save configuration.", ephemeral=True
        )


async def setup_cog(bot: commands.Bot):
    bot.tree.add_command(cmd_setup)


async def setup(bot: commands.Bot):
    await setup_cog(bot)
