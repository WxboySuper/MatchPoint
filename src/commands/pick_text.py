# src/commands/pick_text.py
"""
Text-based prefix commands for picking matches.
Enables non-interactive picking via messages like:
  !pick list          - Show available matches
  !pick <match_id> <team_name>  - Submit a pick
  !picks              - Show your current picks
"""

import logging
from datetime import datetime, timezone, timedelta

from discord.ext import commands
from sqlalchemy.orm import selectinload
from sqlmodel import select

from src.db import get_session
from src.models import Match, Pick
from src import crud

logger = logging.getLogger("esports-bot.commands.pick_text")

# Number of days in advance that matches are available for picking.
PICK_WINDOW_DAYS = 3


class PickTextCommands(commands.Cog):
    """Text-based pick commands for agent/bot accessibility."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command(name="pick")
    async def pick_command(self, ctx: commands.Context, *args):
        """
        Submit or view picks via text.

        Usage:
          !pick list              - Show matches available for picking
          !pick <match_id> <team> - Submit a pick for a match
        """
        if not args:
            await ctx.send(
                "**Usage:**\n"
                "`!pick list` - Show available matches\n"
                "`!pick <match_id> <team_name>` - Submit a pick\n"
                "`!picks` - View your current picks"
            )
            return

        subcommand = args[0].lower()

        if subcommand == "list":
            await self._list_matches(ctx)
        else:
            # Assume it's a match_id and team pick
            if len(args) < 2:
                await ctx.send(
                    "❌ Please provide both match ID and team name.\n"
                    "Example: `!pick 42 T1`"
                )
                return
            try:
                match_id = int(args[0])
            except ValueError:
                await ctx.send(f"❌ Invalid match ID: `{args[0]}`. Must be a number.")
                return
            team_name = " ".join(args[1:])
            await self._submit_pick(ctx, match_id, team_name)

    async def _list_matches(self, ctx: commands.Context):
        """List all matches available for picking."""
        now_utc = datetime.now(timezone.utc)
        pick_cutoff = now_utc + timedelta(days=PICK_WINDOW_DAYS)

        with get_session() as session:
            stmt = (
                select(Match)
                .options(selectinload(Match.contest))
                .where(Match.scheduled_time > now_utc)
                .where(Match.scheduled_time <= pick_cutoff)
                .where(Match.team1 != "TBD")
                .where(Match.team2 != "TBD")
                .order_by(Match.scheduled_time)
                .limit(15)
            )
            matches = session.exec(stmt).all()

            if not matches:
                await ctx.send("📭 No matches available for picking right now.")
                return

            # Get user's existing picks
            db_user = crud.get_user_by_discord_id(session, str(ctx.author.id))
            user_picks = {}
            if db_user:
                picks = crud.list_picks_for_user(session, db_user.id)
                user_picks = {p.match_id: p.chosen_team for p in picks}

            lines = ["**📋 Available Matches:**\n"]
            for m in matches:
                ts = int(m.scheduled_time.timestamp())
                contest_name = m.contest.name if m.contest else "Unknown"
                picked = user_picks.get(m.id)
                pick_indicator = f" ✅ *{picked}*" if picked else ""
                lines.append(
                    f"**ID {m.id}**: {m.team1} vs {m.team2} "
                    f"(Bo{m.best_of or '?'}) - {contest_name}\n"
                    f"  ⏰ <t:{ts}:R>{pick_indicator}"
                )

            await ctx.send("\n".join(lines))

    async def _submit_pick(
        self, ctx: commands.Context, match_id: int, team_name: str
    ):
        """Submit or update a pick for a match."""
        now_utc = datetime.now(timezone.utc)

        with get_session() as session:
            # Get the match
            match = session.get(Match, match_id)
            if not match:
                await ctx.send(f"❌ Match with ID `{match_id}` not found.")
                return

            # Check if match has started
            if now_utc >= match.scheduled_time:
                await ctx.send("❌ This match has already started. Pick locked!")
                return

            # Validate team name (case-insensitive match)
            team_lower = team_name.lower()
            if match.team1.lower() == team_lower:
                chosen_team = match.team1
            elif match.team2.lower() == team_lower:
                chosen_team = match.team2
            else:
                await ctx.send(
                    f"❌ Team `{team_name}` not in this match.\n"
                    f"Choose: **{match.team1}** or **{match.team2}**"
                )
                return

            # Ensure user exists
            db_user = crud.get_user_by_discord_id(session, str(ctx.author.id))
            if not db_user:
                db_user = crud.create_user(
                    session, str(ctx.author.id), ctx.author.name
                )

            # Check for existing pick
            existing_stmt = (
                select(Pick)
                .where(Pick.user_id == db_user.id)
                .where(Pick.match_id == match_id)
            )
            existing_pick = session.exec(existing_stmt).first()

            if existing_pick:
                old_team = existing_pick.chosen_team
                existing_pick.chosen_team = chosen_team
                session.add(existing_pick)
                session.commit()
                await ctx.send(
                    f"🔄 Pick updated: **{old_team}** → **{chosen_team}** "
                    f"(Match {match_id}: {match.team1} vs {match.team2})"
                )
            else:
                crud.create_pick(
                    session,
                    crud.PickCreateParams(
                        user_id=db_user.id,
                        contest_id=match.contest_id,
                        match_id=match_id,
                        chosen_team=chosen_team,
                    ),
                )
                await ctx.send(
                    f"✅ Pick submitted: **{chosen_team}** "
                    f"(Match {match_id}: {match.team1} vs {match.team2})"
                )

    @commands.command(name="picks")
    async def picks_command(self, ctx: commands.Context):
        """View your current picks."""
        now_utc = datetime.now(timezone.utc)

        with get_session() as session:
            db_user = crud.get_user_by_discord_id(session, str(ctx.author.id))
            if not db_user:
                await ctx.send("📭 You haven't made any picks yet.")
                return

            # Get picks for upcoming matches
            stmt = (
                select(Pick)
                .join(Match)
                .options(selectinload(Pick.match))
                .where(Pick.user_id == db_user.id)
                .where(Match.scheduled_time > now_utc)
                .order_by(Match.scheduled_time)
                .limit(15)
            )
            picks = session.exec(stmt).all()

            if not picks:
                await ctx.send("📭 No active picks for upcoming matches.")
                return

            lines = [f"**🎯 Your Picks ({ctx.author.name}):**\n"]
            for p in picks:
                m = p.match
                ts = int(m.scheduled_time.timestamp())
                lines.append(
                    f"**ID {m.id}**: {m.team1} vs {m.team2} → **{p.chosen_team}**\n"
                    f"  ⏰ <t:{ts}:R>"
                )

            await ctx.send("\n".join(lines))


async def setup(bot: commands.Bot):
    await bot.add_cog(PickTextCommands(bot))
