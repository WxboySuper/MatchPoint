"""
Discord command for syncing matches from PandaScore API.
"""

import asyncio
import io
import logging

import discord
from discord import app_commands
from discord.ext import commands

from src.auth import is_admin
from src.notification_batcher import update_upcoming_live_messages
from src.pandascore_sync import perform_pandascore_sync

logger = logging.getLogger(__name__)

__all__ = ["SyncMatches", "setup"]


class SyncMatches(commands.Cog):
    """A cog for syncing data from PandaScore."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="sync-matches",
        description=(
            "Syncs matches (upcoming, running, and recent past) "
            "from PandaScore API."
        ),
    )
    @is_admin()
    async def sync_matches(self, interaction: discord.Interaction) -> None:
        """
        Performs a full sync of matches from PandaScore
        and returns the logs as a file for debugging.
        """
        await interaction.response.defer(ephemeral=True, thinking=True)

        # Set up a temporary logger to capture the sync process output
        log_stream = io.StringIO()
        root_logger = logging.getLogger()
        original_level = root_logger.level
        root_logger.setLevel(logging.DEBUG)  # Capture everything

        handler = logging.StreamHandler(log_stream)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

        try:
            summary = await perform_pandascore_sync()
            if summary is not None:
                await update_upcoming_live_messages()

            # Retrieve the logs
            log_contents = log_stream.getvalue()

            if summary is None:
                message = (
                    "Sync could not be completed. Check API key configuration "
                    "and rate limits. See attached logs for more details."
                )
            else:
                message = (
                    "PandaScore sync complete!\n"
                    f"- Upserted {summary['contests']} contests.\n"
                    f"- Upserted {summary['matches']} matches.\n"
                    f"- Upserted {summary['teams']} teams."
                )

            if log_contents:
                # Create a file object from the log contents
                log_file = discord.File(
                    io.BytesIO(log_contents.encode()),
                    filename="sync_logs.txt",
                )
                await interaction.followup.send(
                    message, file=log_file, ephemeral=True
                )
            else:
                await interaction.followup.send(
                    message + "\n_No log output was generated._",
                    ephemeral=True,
                )
        finally:
            # Clean up the logger
            root_logger.removeHandler(handler)
            root_logger.setLevel(original_level)
            log_stream.close()

    @app_commands.command(
        name="refresh-live-messages",
        description="Force a refresh of the canonical live messages.",
    )
    @is_admin()
    async def refresh_live_messages(
        self, interaction: discord.Interaction
    ) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            await update_upcoming_live_messages()
        except (asyncio.TimeoutError, discord.HTTPException):
            logger.exception(
                "Failed refreshing live messages manually for guild %s",
                getattr(getattr(interaction, "guild", None), "id", None),
            )
            await interaction.followup.send(
                "Live message refresh failed. Check logs for details.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            "Live messages refreshed.",
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(SyncMatches(bot))
