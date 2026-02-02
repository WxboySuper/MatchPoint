# src/commands/pick.py

import logging
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from src.db import get_session
from src.models import Match, Pick
from src import crud

logger = logging.getLogger("esports-bot.commands.pick")

# Number of days in advance (from now) that matches are available for picking.
PICK_WINDOW_DAYS = 3


class PickView(discord.ui.View):
    """
    A view that displays matches one by one and allows users to pick teams.
    Supports navigation and auto-advancement.
    """

    def __init__(
        self, matches: list[Match], user_picks: dict[int, str], user_id: int
    ):
        super().__init__(timeout=600)  # Timeout after 10 minutes
        self.matches = matches
        self.user_picks = user_picks
        self.user_id = user_id
        self.current_index = 0
        self.auto_next = True

        # --- Initialize Buttons ---

        # Row 0: Team Buttons
        self.btn_team1 = discord.ui.Button(
            style=discord.ButtonStyle.secondary, row=0
        )
        self.btn_team1.callback = self.on_team1
        self.add_item(self.btn_team1)

        self.btn_team2 = discord.ui.Button(
            style=discord.ButtonStyle.secondary, row=0
        )
        self.btn_team2.callback = self.on_team2
        self.add_item(self.btn_team2)

        # Row 1: Navigation Buttons
        self.btn_prev = discord.ui.Button(
            label="< Prev", style=discord.ButtonStyle.secondary, row=1
        )
        self.btn_prev.callback = self.on_prev
        self.add_item(self.btn_prev)

        self.btn_next = discord.ui.Button(
            label="Next >", style=discord.ButtonStyle.secondary, row=1
        )
        self.btn_next.callback = self.on_next
        self.add_item(self.btn_next)

        # Row 2: Settings
        self.btn_auto = discord.ui.Button(
            label="Auto-Next: ON", style=discord.ButtonStyle.success, row=2
        )
        self.btn_auto.callback = self.on_auto
        self.add_item(self.btn_auto)

        self.update_components()

    @property
    def current_match(self) -> Match:
        return self.matches[self.current_index]

    def update_components(self):
        """
        Update button labels, styles, and states based on current match/pick.
        """
        match = self.current_match
        current_pick = self.user_picks.get(match.id)

        # Update Team Buttons
        is_locked = datetime.now(timezone.utc) >= match.scheduled_time

        self.btn_team1.label = (
            f"{match.team1} 🔒" if is_locked else match.team1
        )
        self.btn_team2.label = (
            f"{match.team2} 🔒" if is_locked else match.team2
        )

        self.btn_team1.disabled = is_locked
        self.btn_team2.disabled = is_locked

        self.btn_team1.style = discord.ButtonStyle.secondary
        self.btn_team2.style = discord.ButtonStyle.secondary

        if current_pick == match.team1:
            self.btn_team1.style = discord.ButtonStyle.success
        elif current_pick == match.team2:
            self.btn_team2.style = discord.ButtonStyle.success

        # Update Navigation Buttons
        self.btn_prev.disabled = self.current_index == 0
        self.btn_next.disabled = self.current_index == len(self.matches) - 1

        # Update Auto-Next Button
        self.btn_auto.label = f"Auto-Next: {'ON' if self.auto_next else 'OFF'}"
        if self.auto_next:
            self.btn_auto.style = discord.ButtonStyle.success
        else:
            self.btn_auto.style = discord.ButtonStyle.secondary

    def get_embed(self) -> discord.Embed:
        """Generate the embed for the current match."""
        match = self.current_match
        current_pick = self.user_picks.get(match.id)

        contest_name = (
            match.contest.name if match.contest else "Unknown Tournament"
        )
        timestamp = int(match.scheduled_time.timestamp())
        time_str = f"<t:{timestamp}:F> (<t:{timestamp}:R>)"

        embed = discord.Embed(
            title=f"Match {self.current_index + 1} of {len(self.matches)}",
            description=f"**{contest_name}**",
            color=discord.Color.blue(),
        )

        embed.add_field(
            name="Teams", value=f"{match.team1} vs {match.team2}", inline=False
        )
        embed.add_field(
            name="Format",
            value=f"Best of {match.best_of}" if match.best_of else "Best of ?",
            inline=True,
        )
        embed.add_field(name="Time", value=time_str, inline=False)

        pick_status = f"✅ **{current_pick}**" if current_pick else "❌ None"
        if datetime.now(timezone.utc) >= match.scheduled_time:
            pick_status += " (Locked)"
        embed.add_field(name="Your Pick", value=pick_status, inline=False)

        return embed

    async def refresh_view(self, interaction: discord.Interaction):
        """Update the message with the new embed and view state."""
        self.update_components()
        embed = self.get_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    async def handle_pick(self, interaction: discord.Interaction, team: str):
        """Process a team pick."""
        match = self.current_match

        # Check if match has already started
        if datetime.now(timezone.utc) >= match.scheduled_time:
            self.update_components()
            embed = self.get_embed()
            await interaction.response.edit_message(embed=embed, view=self)
            await interaction.followup.send(
                "Cannot pick: Match has already started!", ephemeral=True
            )
            return

        # Persist the pick
        with get_session() as session:
            # Ensure user exists
            db_user = crud.get_user_by_discord_id(session, str(self.user_id))
            if not db_user:
                db_user = crud.create_user(
                    session, str(self.user_id), interaction.user.name
                )

            # Check for existing pick
            existing_pick_stmt = (
                select(Pick)
                .where(Pick.user_id == db_user.id)
                .where(Pick.match_id == match.id)
            )
            existing_pick = session.exec(existing_pick_stmt).first()

            if existing_pick:
                existing_pick.chosen_team = team
                session.add(existing_pick)
                session.commit()
            else:
                try:
                    crud.create_pick(
                        session,
                        crud.PickCreateParams(
                            user_id=db_user.id,
                            contest_id=match.contest_id,
                            match_id=match.id,
                            chosen_team=team,
                        ),
                    )
                except IntegrityError:
                    # Race condition: pick was created by concurrent request
                    session.rollback()
                    existing_pick = session.exec(existing_pick_stmt).first()
                    if existing_pick:
                        existing_pick.chosen_team = team
                        session.add(existing_pick)
                        session.commit()

        # Update local state
        self.user_picks[match.id] = team

        # Auto-Next Logic
        if self.auto_next and self.current_index < len(self.matches) - 1:
            self.current_index += 1

        await self.refresh_view(interaction)

    async def on_team1(self, interaction: discord.Interaction):
        await self.handle_pick(interaction, self.current_match.team1)

    async def on_team2(self, interaction: discord.Interaction):
        await self.handle_pick(interaction, self.current_match.team2)

    async def on_prev(self, interaction: discord.Interaction):
        self.current_index -= 1
        await self.refresh_view(interaction)

    async def on_next(self, interaction: discord.Interaction):
        self.current_index += 1
        await self.refresh_view(interaction)

    async def on_auto(self, interaction: discord.Interaction):
        self.auto_next = not self.auto_next
        await self.refresh_view(interaction)


async def _handle_no_matches(interaction: discord.Interaction, session):
    """
    Handle the case where no matches are available to pick.
    Searches for the next upcoming match and sends a helpful embed.
    """
    now_utc = datetime.now(timezone.utc)
    next_match_stmt = (
        select(Match)
        .where(Match.scheduled_time > now_utc)
        .order_by(Match.scheduled_time)
        .limit(1)
    )
    next_match = session.exec(next_match_stmt).first()

    embed = discord.Embed(
        title="No Active Matches",
        description=(
            "There are no matches currently open for picking "
            f"(next {PICK_WINDOW_DAYS} days).\n"
            "Check back later or view the global standings!"
        ),
        color=discord.Color.orange(),
    )

    if next_match:
        ts = int(next_match.scheduled_time.timestamp())
        embed.add_field(
            name="Next Upcoming Match",
            value=f"<t:{ts}:F>\n(<t:{ts}:R>)",
            inline=False,
        )

    # Suggest viewing leaderboard
    embed.add_field(
        name="While You Wait",
        value="Use `/leaderboard` to check the current standings.",
        inline=False,
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


@app_commands.command(
    name="pick", description="Submit or update a pick for an upcoming match."
)
async def pick(interaction: discord.Interaction):
    """The main command to initiate picking a match."""
    logger.info(
        "'%s' (%s) initiated a pick.",
        interaction.user.name,
        interaction.user.id,
    )
    with get_session() as session:
        # Fetch active matches that are within the pick window
        now_utc = datetime.now(timezone.utc)
        pick_cutoff = now_utc + timedelta(days=PICK_WINDOW_DAYS)
        active_matches_stmt = (
            select(Match)
            .options(selectinload(Match.contest))  # Eager load contest
            .where(Match.scheduled_time > now_utc)
            .where(Match.scheduled_time <= pick_cutoff)
            .where(Match.team1 != "TBD")
            .where(Match.team2 != "TBD")
            .order_by(Match.scheduled_time)
        )
        active_matches = session.exec(active_matches_stmt).all()

        if not active_matches:
            await _handle_no_matches(interaction, session)
            return

        # Get user and their existing picks for active matches
        db_user = crud.get_user_by_discord_id(
            session,
            str(interaction.user.id),
        )
        user_picks = {}
        if db_user:
            active_match_ids = [m.id for m in active_matches]
            picks = crud.get_user_picks_for_matches(
                session, db_user.id, active_match_ids
            )
            user_picks = {pick.match_id: pick.chosen_team for pick in picks}

        view = PickView(
            matches=active_matches,
            user_picks=user_picks,
            user_id=interaction.user.id,
        )

        await interaction.response.send_message(
            embed=view.get_embed(),
            view=view,
            ephemeral=True,
        )


async def setup(bot):
    bot.tree.add_command(pick)
