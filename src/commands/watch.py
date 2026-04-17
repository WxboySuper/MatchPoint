import logging
import discord
from discord import app_commands
from discord.ext import commands

from src.db import get_async_session
from src.crud.watchlist import (
    add_watch_async,
    list_watches_for_user_async,
    remove_watch_async,
)

logger = logging.getLogger(__name__)

watch_group = app_commands.Group(
    name="watch", description="Manage your watchlist"
)


@watch_group.command(name="add", description="Add a match to your watchlist")
@app_commands.describe(match_id="PandaScore match ID to watch")
async def add(interaction: discord.Interaction, match_id: int):
    if not interaction.user:
        await interaction.response.send_message(
            "This command must be run in a server or DM.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    try:
        async with get_async_session() as session:
            rec = await add_watch_async(
                session, str(interaction.user.id), match_id
            )
        await interaction.followup.send(
            f"Added match {match_id} to your watchlist (id={rec.id}).",
            ephemeral=True,
        )
    except Exception:
        logger.exception(
            "Failed to add watchlist entry for user %s",
            getattr(interaction.user, "id", None),
        )
        await interaction.followup.send(
            "Failed to add watchlist entry.", ephemeral=True
        )


@watch_group.command(
    name="remove", description="Remove a watchlist entry by id"
)
@app_commands.describe(watch_id="Watchlist entry id")
async def remove(interaction: discord.Interaction, watch_id: int):
    await interaction.response.defer(ephemeral=True)
    try:
        async with get_async_session() as session:
            ok = await remove_watch_async(session, watch_id)
        if ok:
            await interaction.followup.send(
                f"Removed watchlist entry {watch_id}.", ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"No watchlist entry {watch_id} found.", ephemeral=True
            )
    except Exception:
        logger.exception("Failed to remove watchlist entry %s", watch_id)
        await interaction.followup.send(
            "Failed to remove watchlist entry.", ephemeral=True
        )


@watch_group.command(name="list", description="List your watchlist entries")
async def list_watches(interaction: discord.Interaction):
    if not interaction.user:
        await interaction.response.send_message(
            "This command must be run in a server or DM.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    try:
        async with get_async_session() as session:
            rows = await list_watches_for_user_async(
                session, str(interaction.user.id)
            )
        # Filter out entries already marked as watched
        rows = [r for r in rows if not getattr(r, "is_watched", False)]
        if not rows:
            await interaction.followup.send(
                "Your watchlist is empty.", ephemeral=True
            )
            return
        lines = [
            (
                f"id={r.id} match={r.match_id} "
                f"added={getattr(r, 'created_at', None)}"
            )
            for r in rows
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)
    except Exception:
        logger.exception(
            "Failed to list watchlist entries for user %s",
            getattr(interaction.user, "id", None),
        )
        await interaction.followup.send(
            "Failed to list watchlist entries.", ephemeral=True
        )


async def setup(bot: commands.Bot):
    bot.tree.add_command(watch_group)
