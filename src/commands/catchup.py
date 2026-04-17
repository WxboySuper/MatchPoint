import discord
from discord import app_commands, ui
from discord.ext import commands
from typing import List

from src.db import get_async_session
from src.crud.watchlist import (
    list_watches_for_user_async,
    mark_as_watched_async,
)
from sqlmodel import select
from src.models import Match
import logging

logger = logging.getLogger(__name__)


class _MarkWatchedButton(ui.Button):
    def __init__(self, watch_id: int):
        super().__init__(
            label=f"Mark {watch_id} as Watched",
            style=discord.ButtonStyle.primary,
        )
        self.watch_id = watch_id

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            async with get_async_session() as session:
                rec = await mark_as_watched_async(session, self.watch_id)
            if rec is None:
                msg = (
                    f"Watch entry {self.watch_id} was not found "
                    "or already removed."
                )
                await interaction.followup.send(msg, ephemeral=True)
                return
            self.disabled = True
            await interaction.message.edit(view=self.view)
            await interaction.followup.send(
                f"Marked watch entry {self.watch_id} as watched.",
                ephemeral=True,
            )
        except Exception:
            logger.exception("Failed to mark as watched for %s", self.watch_id)
            await interaction.followup.send(
                "Failed to mark as watched.", ephemeral=True
            )


class CatchupView(ui.View):
    def __init__(self, watch_ids: List[int]):
        super().__init__(timeout=300)
        for wid in watch_ids:
            self.add_item(_MarkWatchedButton(wid))


async def _gather_finished_watches(session, user_id: str):
    rows = await list_watches_for_user_async(session, user_id)
    # Filter out already marked and entries without a pandascore match id
    pending = [
        w
        for w in rows
        if not getattr(w, "is_watched", False) and getattr(w, "match_id", None)
    ]
    if not pending:
        return []

    ids = [w.match_id for w in pending]
    # Batch load matches to avoid N+1 queries
    stmt = select(Match).where(Match.pandascore_id.in_(ids))
    res = await session.exec(stmt)
    matches_map = {m.pandascore_id: m for m in res.all()}

    def _is_match_finished(match):
        return bool(
            getattr(match, "result", None)
            or getattr(match, "status", None) == "finished"
        )

    finished = []
    for w in pending:
        match = matches_map.get(w.match_id)
        if not match:
            continue
        if _is_match_finished(match):
            score = (
                getattr(match.result, "score", None)
                if getattr(match, "result", None)
                else None
            )
            finished.append((w, match, score))
    return finished


catchup_group = app_commands.Group(
    name="catchup", description="Catchup on finished watched matches"
)


@catchup_group.command(name="now", description="List finished watched matches")
async def now(interaction: discord.Interaction):
    if not interaction.user:
        await interaction.response.send_message(
            "This command must be run in a server or DM.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    try:
        async with get_async_session() as session:
            finished = await _gather_finished_watches(
                session, str(interaction.user.id)
            )
        if not finished:
            await interaction.followup.send(
                "No finished watched matches found.", ephemeral=True
            )
            return
        lines = []
        ids = []
        for w, match, score in finished:
            score_text = f"||{score}||" if score else "(score unknown)"
            lines.append(
                f"id={w.id} — {match.team1} vs {match.team2} {score_text}"
            )
            ids.append(w.id)
        view = CatchupView(ids)
        await interaction.followup.send(
            "\n".join(lines), view=view, ephemeral=True
        )
    except Exception:
        await interaction.followup.send(
            "Failed to gather catchup items.", ephemeral=True
        )


async def setup(bot: commands.Bot):
    bot.tree.add_command(catchup_group)
