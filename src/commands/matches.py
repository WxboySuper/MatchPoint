# src/commands/matches.py

import logging
import io
import csv
from datetime import datetime, timezone, timedelta

import discord
from discord import app_commands
from sqlmodel import Session

from src.db import get_session
from src.models import Contest, Match
from src import crud
from src.auth import is_admin
from src.parsers.game_slug import game_display_name

logger = logging.getLogger("esports-bot.commands.matches")

matches_group = app_commands.Group(
    name="matches", description="Commands for viewing and managing matches."
)

# Display helpers for matches embed
STATUS_MAP = {
    "finished": "✅ Finished",
    "running": "🔴 Live",
    "live": "🔴 Live",
    "in_progress": "🔴 Live",
    "not_started": "⏳ Upcoming",
    "scheduled": "⏳ Upcoming",
    "canceled": "❌ Canceled",
    "postponed": "🕒 Postponed",
}
LIVE_STATUSES = ("running", "live", "in_progress")


def _format_match_value(m: Match) -> str:
    status_label = STATUS_MAP.get(
        m.status, m.status.capitalize() if m.status else "Upcoming"
    )
    time_str = m.scheduled_time.strftime("%H:%M UTC")
    value = (
        f"**Game:** {game_display_name(getattr(m, 'game', None))}\n"
        f"**Status:** {status_label}\n"
        f"**Time:** {time_str}\n"
        f"**Contest:** {m.contest.name if m.contest else 'Unknown'}"
    )
    if m.best_of:
        value += f"\n**Format:** Best of {m.best_of}"
    if m.result:
        score_str = f" ({m.result.score})" if m.result.score else ""
        value += f"\n**Result:** **{m.result.winner}** won{score_str}"
    elif m.status in LIVE_STATUSES and m.last_announced_score:
        value += f"\n**Current Score:** {m.last_announced_score}"
    return value


def _paginate_matches(ms: list[Match], info: tuple[int, int] | None):
    if info is None:
        p, sz = 1, 10
    else:
        p, sz = info
    total = len(ms)
    total_pages = (total + sz - 1) // sz
    start = (p - 1) * sz
    end = start + sz
    return ms[start:end], p, total_pages, sz, total


def _make_aware(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=timezone.utc)


def _status_for(c: Contest, now_dt: datetime) -> str:
    start = _make_aware(c.start_date)
    end = _make_aware(c.end_date)
    return "Active" if start <= now_dt <= end else "Upcoming"


def _label_for(c: Contest, now_dt: datetime) -> str:
    start = _make_aware(c.start_date)
    status = _status_for(c, now_dt)
    label = f"{c.name} — {status} " f"({start.strftime('%Y-%m-%d %H:%M UTC')})"
    if len(label) <= 100:
        return label
    suffix = f"... (ID: {c.id})"
    max_name_length = 100 - len(suffix)
    return f"{(c.name or '')[:max_name_length]}{suffix}"


def _matches_name(current_norm: str, c: Contest) -> bool:
    """Return True if contest `c` matches the normalized `current_norm`.

    Encapsulates the conditional logic used for filtering by name so
    callers don't include complex expressions inline.
    """
    if not current_norm:
        return True
    name = (c.name or "").lower()
    return current_norm in name


def _build_entries(
    contests: list[Contest],
    current_norm: str,
    now: datetime,
) -> list[tuple[str, datetime, Contest]]:
    entries: list[tuple[str, datetime, Contest]] = []
    for c in contests:
        start = _make_aware(c.start_date)
        end = _make_aware(c.end_date)
        if end <= now:
            continue
        if not _matches_name(current_norm, c):
            continue
        status = _status_for(c, now)
        entries.append((status, start, c))
    entries.sort(key=lambda t: (0 if t[0] == "Active" else 1, t[1]))
    return entries


def _get_autocomplete_choices(
    session: Session,
    current: str,
    limit: int = 25,
) -> list[app_commands.Choice[int]]:
    """Build autocomplete `Choice` objects for contests using helpers.

    Separated so the public async handler is a tiny wrapper and its
    complexity is minimal.
    """
    all_contests = crud.list_contests(session)
    now = datetime.now(timezone.utc)
    current_norm = (current or "").strip().lower()

    entries = _build_entries(all_contests, current_norm, now)

    choices: list[app_commands.Choice[int]] = []
    for _, _, contest in entries:
        choices.append(
            app_commands.Choice(
                name=_label_for(contest, now),
                value=contest.id,
            )
        )
        if len(choices) >= limit:
            break

    return choices


async def contest_autocompletion(
    _: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[int]]:
    """Autocomplete for contests, showing active and upcoming contests.

    This handler is a small wrapper that delegates work to
    `_get_autocomplete_choices` so its complexity stays minimal.
    """

    with get_session() as session:
        return _get_autocomplete_choices(session, current, limit=25)


async def create_matches_embed(
    title: str,
    matches: list[Match],
    interaction: discord.Interaction,
    page_info: tuple[int, int] | None = None,
) -> discord.Embed:
    """Creates a standard embed for displaying a list of matches.

    `page_info` is an optional (page, page_size) tuple. When omitted the
    defaults are page=1 and page_size=10.
    """
    embed = discord.Embed(
        title=title,
        color=discord.Color.purple(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=(
            interaction.user.avatar.url if interaction.user.avatar else None
        ),
    )

    if not matches:
        embed.description = "No matches found."
        return embed

    display_matches, page, total_pages, _, total_matches = _paginate_matches(
        matches, page_info
    )
    embed.set_footer(
        text=f"Page {page} of {total_pages} | Total Matches: {total_matches}"
    )

    for m in display_matches:
        embed.add_field(
            name=f"{m.team1} vs {m.team2}",
            value=_format_match_value(m),
            inline=False,
        )
    return embed


class PaginatedMatchesView(discord.ui.View):
    """A view with buttons to navigate between pages of matches."""

    def __init__(
        self,
        title: str,
        matches: list[Match],
        interaction: discord.Interaction,
        page_size: int = 10,
    ):
        super().__init__(timeout=180)
        self.title = title
        self.matches = matches
        self.interaction = interaction
        self.page_size = page_size
        self.current_page = 1
        self.total_pages = (len(matches) + page_size - 1) // page_size
        self._update_buttons()

    def _update_buttons(self):
        self.prev_button.disabled = self.current_page <= 1
        self.next_button.disabled = self.current_page >= self.total_pages

    async def update_message(self, interaction: discord.Interaction):
        """Updates the embed with matches for the current page."""
        embed = await create_matches_embed(
            self.title,
            self.matches,
            self.interaction,
            page_info=(self.current_page, self.page_size),
        )
        self._update_buttons()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.gray)
    async def prev_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current_page -= 1
        await self.update_message(interaction)

    @discord.ui.button(label="Next ▶️", style=discord.ButtonStyle.gray)
    async def next_button(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current_page += 1
        await self.update_message(interaction)


class DayNavigationView(discord.ui.View):
    """A view with buttons to navigate between days."""

    def __init__(
        self,
        current_date: datetime.date,
        interaction: discord.Interaction,
    ):
        super().__init__(timeout=180)
        self.current_date = current_date
        self.interaction = interaction
        self.current_page = 1

    async def update_embed(self, interaction: discord.Interaction):
        """Updates the embed with matches for the current date."""
        with get_session() as session:
            matches = crud.get_matches_by_date(session, self.current_date)
            title = f"Matches for {self.current_date.strftime('%Y-%m-%d')}"

            page_size = 10
            total_pages = (len(matches) + page_size - 1) // page_size
            self.current_page = (
                max(1, min(self.current_page, total_pages))
                if total_pages > 0
                else 1
            )

            embed = await create_matches_embed(
                title,
                matches,
                self.interaction,
                page_info=(self.current_page, page_size),
            )

            # Update button states
            self.prev_page.disabled = self.current_page <= 1
            self.next_page.disabled = self.current_page >= total_pages

            if interaction.response.is_done():
                await interaction.edit_original_response(
                    embed=embed, view=self
                )
            else:
                await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(
        label="📅 Prev Day", style=discord.ButtonStyle.secondary
    )
    async def previous_day(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current_date -= timedelta(days=1)
        self.current_page = 1
        await self.update_embed(interaction)

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.primary)
    async def prev_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current_page -= 1
        await self.update_embed(interaction)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.primary)
    async def next_page(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current_page += 1
        await self.update_embed(interaction)

    @discord.ui.button(
        label="Next Day 📅", style=discord.ButtonStyle.secondary
    )
    async def next_day(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        self.current_date += timedelta(days=1)
        self.current_page = 1
        await self.update_embed(interaction)


class TournamentSelect(discord.ui.Select):
    """A dropdown to select a tournament."""

    def __init__(self, contests: list[Contest]):
        options = [
            discord.SelectOption(label=c.name, value=str(c.id))
            for c in contests
        ]
        super().__init__(placeholder="Choose a tournament...", options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        contest_id = int(self.values[0])
        with get_session() as session:
            contest = crud.get_contest_by_id(session, contest_id)
            if not contest:
                await interaction.edit_original_response(
                    content=f"Contest with ID {contest_id} not found.",
                    view=None,
                )
                return
            matches = crud.list_matches_for_contest(session, contest_id)
            title = f"Matches for {contest.name}"

            if len(matches) > 10:
                view = PaginatedMatchesView(title, matches, interaction)
                embed = await create_matches_embed(
                    title, matches, interaction, page_info=(1, 10)
                )
                await interaction.edit_original_response(
                    embed=embed, view=view
                )
            else:
                embed = await create_matches_embed(title, matches, interaction)
                await interaction.edit_original_response(
                    embed=embed, view=None
                )


# --- Commands ---


@matches_group.command(
    name="view-by-day",
    description="View matches scheduled for a specific day.",
)
async def view_by_day(interaction: discord.Interaction):
    """Shows matches for the current day with navigation."""
    logger.info("'%s' requested matches by day.", interaction.user.name)
    current_date = datetime.now(timezone.utc).date()

    # Initial response
    await interaction.response.defer(ephemeral=True)

    with get_session() as session:
        matches = crud.get_matches_by_date(session, current_date)
        title = f"Matches for {current_date.strftime('%Y-%m-%d')}"

        embed = await create_matches_embed(
            title, matches, interaction, page_info=(1, 10)
        )
        view = DayNavigationView(current_date, interaction)

        # Update button states for the first page
        page_size = 10
        total_pages = (len(matches) + page_size - 1) // page_size
        view.prev_page.disabled = True
        view.next_page.disabled = total_pages <= 1

        await interaction.followup.send(
            embed=embed,
            view=view,
            ephemeral=True,
        )


@matches_group.command(
    name="view-by-tournament",
    description="View all matches for a specific tournament.",
)
async def view_by_tournament(interaction: discord.Interaction):
    """Shows a dropdown to select a tournament and view its matches."""
    logger.info("'%s' requested matches by tournament.", interaction.user.name)
    with get_session() as session:
        contests = crud.list_contests(session)
        if not contests:
            await interaction.response.send_message(
                "No tournaments found.",
                ephemeral=True,
            )
            return

        view = discord.ui.View(timeout=180)
        view.add_item(TournamentSelect(contests=contests[:25]))
        await interaction.response.send_message(
            "Please select a tournament:", view=view, ephemeral=True
        )


@matches_group.command(
    name="upload", description="Upload a match schedule via CSV (Admin only)."
)
@app_commands.autocomplete(contest_id=contest_autocompletion)
@app_commands.describe(
    contest_id="The ID of the contest to add matches to.",
    attachment="The CSV file with the match schedule.",
)
@is_admin()
async def upload(
    interaction: discord.Interaction,
    contest_id: int,
    attachment: discord.Attachment,
):
    """Handles CSV upload for match schedules."""

    logger.info(
        "Admin '%s' is uploading matches for contest %s.",
        interaction.user.name,
        contest_id,
    )
    await interaction.response.defer(ephemeral=True)

    with get_session() as session:
        contest = crud.get_contest_by_id(session, contest_id)
        if not contest:
            await interaction.followup.send(
                f"Contest with ID {contest_id} not found.", ephemeral=True
            )
            return

        try:
            csv_data = await attachment.read()
            csv_file = io.StringIO(csv_data.decode("utf-8"))
            reader = csv.DictReader(csv_file)

            matches_to_create = []
            errors = []
            for i, row in enumerate(reader, 1):
                try:
                    team1 = row["team1"]
                    team2 = row["team2"]
                    scheduled_time = datetime.fromisoformat(
                        row["scheduled_time"]
                    )
                    # Extract leaguepedia_id or generate a fallback
                    leaguepedia_id = row.get("leaguepedia_id")
                    if not leaguepedia_id:
                        # Deterministic fallback for manual uploads
                        leaguepedia_id = (
                            f"manual-{contest_id}-{team1}-{team2}-"
                            f"{scheduled_time.isoformat()}"
                        )

                    matches_to_create.append(
                        {
                            "contest_id": contest_id,
                            "leaguepedia_id": leaguepedia_id,
                            "team1": team1,
                            "team2": team2,
                            "scheduled_time": scheduled_time,
                        }
                    )
                except (KeyError, ValueError) as e:
                    errors.append(f"Row {i}: Invalid data or format. {e}")

            if errors:
                await interaction.followup.send(
                    "Errors found in CSV:\n" + "\n".join(errors),
                    ephemeral=True,
                )
                return

            crud.bulk_create_matches(session, matches_to_create)
            await interaction.followup.send(
                f"Successfully uploaded {len(matches_to_create)} matches for "
                f"'{contest.name}'.",
                ephemeral=True,
            )

        except Exception as e:
            logger.exception("Error during match upload.")
            await interaction.followup.send(
                f"An unexpected error occurred: {e}", ephemeral=True
            )


async def setup(bot):
    bot.tree.add_command(matches_group)
