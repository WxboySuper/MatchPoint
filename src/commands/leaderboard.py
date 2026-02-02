# src/commands/leaderboard.py

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Union

import discord
from discord import app_commands
from sqlalchemy import func, case
from sqlmodel import Session, select

from src.db import get_session
from src.models import Contest, Pick, User
from src import crud
from src.bot_instance import get_bot_instance

logger = logging.getLogger("esports-bot.commands.leaderboard")


MIN_PICKS_FOR_ACCURACY_LEADERBOARD = 5


# --- Helper Functions ---


LeaderboardData = Union[
    # User, Accuracy, Total Correct, Total Picks
    List[tuple[User, float, int, int]],
    # User, Total Correct
    List[tuple[User, int]],
]


def _get_pick_correct_attr():
    """
    Get the Pick model attribute used to indicate whether a pick was
    correct.

    Checks common attribute names ("correct", "is_correct",
    "was_correct") and returns the first matching attribute on the
    Pick model. Raises AttributeError if no suitable attribute is
    found.

    Returns:
        attribute: The Pick model attribute (descriptor) that
            represents correctness.
    """
    for name in ("correct", "is_correct", "was_correct"):
        if hasattr(Pick, name):
            return getattr(Pick, name)
    raise AttributeError("Pick model has no attribute indicating correctness")


def _build_accuracy_query():
    """
    Build a query that returns
    (User, accuracy_percent, total_correct).

    Accuracy is returned as a percentage (0-100). Only users with at
    least MIN_PICKS_FOR_ACCURACY_LEADERBOARD picks are included.
    """
    correct_attr = _get_pick_correct_attr()
    total_picks = func.count(getattr(Pick, "id"))
    total_correct = func.sum(case((correct_attr.is_(True), 1), else_=0))
    # accuracy as percentage
    accuracy = (total_correct * 100.0) / total_picks

    query = (
        select(
            User,
            accuracy.label("accuracy"),
            total_correct.label("total_correct"),
            total_picks.label("total_picks"),
        )
        .join(Pick, getattr(Pick, "user_id") == User.id)
        .group_by(User.id)
        .having(total_picks >= MIN_PICKS_FOR_ACCURACY_LEADERBOARD)
        .order_by(accuracy.desc())
    )
    return query


def _build_count_query(days: int = None, contest_id: int = None):
    """
    Construct a SQL query selecting each User with their total correct
    picks, optionally limited to a recent time window or a specific
    contest.

    Parameters:
        days (int | None): If provided, include only picks created
            within the last `days` days.
        contest_id (int | None): If provided, include only picks
            belonging to the specified contest.

    Returns:
        sqlalchemy.sql.selectable.Select: A select query that yields
            (User, total_correct) where `total_correct` is the count
            of correct picks per user, ordered by `total_correct`
            descending.
    """
    correct_attr = _get_pick_correct_attr()
    total_correct = func.sum(case((correct_attr.is_(True), 1), else_=0))

    query = select(User, total_correct.label("total_correct")).join(
        Pick, getattr(Pick, "user_id") == User.id
    )

    # Apply optional filters
    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        if hasattr(Pick, "timestamp"):
            query = query.where(getattr(Pick, "timestamp") >= cutoff)
    if contest_id is not None and hasattr(Pick, "contest_id"):
        query = query.where(getattr(Pick, "contest_id") == contest_id)

    # Only include picks that have been resolved as correct when a
    # status field is available. This mirrors the previous behaviour
    # that filtered on Pick.status == "correct", rather than relying
    # solely on the is_correct flag.
    if hasattr(Pick, "status"):
        query = query.where(getattr(Pick, "status") == "correct")

    query = query.group_by(User.id).order_by(total_correct.desc())
    return query


def _passes_guild(user, guild_id: int) -> bool:
    """
    Determine whether a user passes the specified guild filter.

    Checks if the user (by discord_id) is a member of the specified guild.
    Requires the bot instance to be available and the guild to be cached.

    Parameters:
        user: A User model instance with a `discord_id` attribute.
        guild_id (int | None): Guild identifier to filter by; when
            `None`, no filtering is applied.

    Returns:
        bool: `True` if `guild_id` is `None` or the user is found in the
            guild's member cache; `False` otherwise.
    """
    if guild_id is None:
        return True

    bot_instance = get_bot_instance()
    if not bot_instance:
        return False

    guild = bot_instance.get_guild(guild_id)
    if not guild:
        return False

    try:
        # User.discord_id is stored as a string
        return guild.get_member(int(user.discord_id)) is not None
    except (ValueError, TypeError):
        return False


def _normalize_row(row):
    """
    Normalize a database result row into a tuple.

    Attempts to convert the given row to a tuple. If the input is
    already a tuple it is returned unchanged. If conversion fails, the
    original value is wrapped in a single-element tuple.

    Parameters:
        row: The database result row or iterable to normalize.

    Returns:
        tuple: A tuple representation of the input, or a
            single-element tuple containing the original value when
            conversion is not possible.
    """
    if isinstance(row, tuple):
        return row
    try:
        return tuple(row)
    except Exception:
        return (row,)


def _to_float(val, default=0.0):
    """
    Convert a value to a float, returning a fallback if conversion fails.

    Parameters:
        val: The value to convert to float.
        default (float): Value returned if conversion fails (defaults to 0.0).

    Returns:
        float: The converted float, or `default` if conversion raises
            an exception.
    """
    try:
        return float(val)
    except Exception:
        return default


def _to_int(val, default=0):
    """
    Convert a value to an integer, returning a fallback if conversion fails.

    Parameters:
        val (Any): The value to convert to int.
        default (int): Integer to return if conversion raises an
            exception (defaults to 0).

    Returns:
        int: The converted integer, or `default` if conversion fails.
    """
    try:
        return int(val)
    except Exception:
        return default


def _normalize_legacy_accuracy(
    accuracy: float, total_correct: int, total_picks: int
) -> float:
    """
    Detect and normalize legacy fractional accuracy (0-1) to
    percentage (0-100).

    We detect legacy fractions by checking if the accuracy matches the
    fraction (correct/picks) but is significantly different from the
    percentage ((correct*100)/picks).
    """
    if not (0.0 < accuracy <= 1.0 and total_picks > 0):
        return accuracy

    fractional = total_correct / total_picks
    percentage = (total_correct * 100.0) / total_picks

    # If it matches the fraction exactly but not the percentage,
    # it's legacy.
    if abs(accuracy - fractional) < 0.0001 < abs(accuracy - percentage):
        return accuracy * 100.0
    return accuracy


def _parse_accuracy_row(t: tuple):
    """
    Normalize an accuracy-based database row into
    (User, accuracy, total_correct, total_picks).

    Converts raw accuracy to a float percentage (0-100). If the value
    appears to be a legacy fraction (0-1) rather than a percentage, it
    is multiplied by 100. Values are clamped to [0, 100] and integers
    are coerced.

    Parameters:
        t (tuple): Database row expected in the form
            (User, accuracy, total_correct, total_picks).

    Returns:
        tuple: (User, accuracy_float, total_correct, total_picks) where
            `accuracy_float` is a percentage between 0 and 100.
    """
    raw_accuracy = t[1] if len(t) > 1 else None
    accuracy = _to_float(raw_accuracy, default=0.0)

    total_correct = _to_int(t[2]) if len(t) > 2 else 0
    total_picks = _to_int(t[3]) if len(t) > 3 else 0

    accuracy = _normalize_legacy_accuracy(accuracy, total_correct, total_picks)

    # Clamp and sanitize
    accuracy = max(0.0, min(100.0, accuracy))
    total_correct = max(0, total_correct)

    return (t[0], accuracy, total_correct, total_picks)


def _parse_count_row(t: tuple):
    """
    Parse a count-based leaderboard row into a
    (User, total_correct) tuple.

    Parameters:
        t (tuple): Row or tuple where index 0 is a User and index 1
            is the total correct picks (may be None).

    Returns:
        tuple: (User, total_correct) where total_correct is an int
            greater than or equal to 0.
    """
    total = _to_int(t[1]) if len(t) > 1 and t[1] is not None else 0
    if total < 0:
        total = 0
    return (t[0], total)


def _parse_row(tup, is_accuracy_based: bool):
    """
    Normalize a database result row into a leaderboard tuple or
    return None on parse failure.

    Parameters:
        tup (tuple | Any): The raw database row; expected to have a
            User as the first element.
        is_accuracy_based (bool): If true, parse as an accuracy-based
            row; otherwise parse as a count-based row.

    Returns:
        tuple | None: For accuracy-based rows, returns
            (User, accuracy: float, total_correct: int, total_picks: int).
            For count-based rows, returns (User, total_correct: int).
            Returns `None` if the input is empty or the first element
            (User) is missing.
    """
    if not tup:
        return None
    user = tup[0]
    if user is None:
        return None

    return (
        _parse_accuracy_row(tup)
        if (is_accuracy_based)
        else _parse_count_row(tup)
    )


async def _apply_guild_filter(results, guild_id: int, is_accuracy_based: bool):
    """
    Normalize database result rows into leaderboard entries and
    filter them by guild membership.

    Parameters:
        results: An iterable of database result rows produced by the
            leaderboard queries.
        guild_id (int | None): Guild/server identifier used to
            include only users belonging to that guild. A falsy value
            disables guild filtering.
        is_accuracy_based (bool): If True, parse rows as
            accuracy-based entries; otherwise parse as count-based
            entries.

    Returns:
        list: A list of parsed leaderboard entries. For
            accuracy-based data each entry is
            (User, accuracy: float, total_correct: int, total_picks: int). For
            count-based data each entry is (User, total_correct: int).
            Rows that cannot be parsed or whose users fail the guild
            check are omitted.
    """
    processed = []
    for row in results:
        tup = _normalize_row(row)
        parsed = _parse_row(tup, is_accuracy_based)
        if parsed is None:
            continue

        user = parsed[0]
        if not _passes_guild(user, guild_id):
            continue

        processed.append(parsed)

    return processed


def _try_apply_guild_filter_in_sql(query, guild_id: int | None):
    """
    Attempt to filter by guild members using a SQL IN clause.

    If the guild is small enough (<= 900 members), modify the query to
    filter users directly in the database. Returns the (potentially modified)
    query and a boolean indicating if the SQL filter was applied.
    """
    if not guild_id:
        return query, False

    bot = get_bot_instance()
    if not bot:
        return query, False

    guild = bot.get_guild(guild_id)
    if not guild:
        return query, False

    # Limit for SQL IN clause (safe margin below 999)
    sql_in_limit = 900
    member_ids = [str(m.id) for m in guild.members]
    count = len(member_ids)

    if count > sql_in_limit:
        logger.debug(
            "Skipping SQL IN filter for guild %s with %s members (limit %s)",
            guild_id,
            count,
            sql_in_limit,
        )
        return query, False

    logger.debug(
        "Using SQL IN filter for guild %s with %s members", guild_id, count
    )
    return query.where(User.discord_id.in_(member_ids)), True


async def get_leaderboard_data(
    session: Session,
    days: int = None,
    guild_id: int = None,
    contest_id: int = None,
) -> LeaderboardData:
    """
    Retrieve leaderboard entries, selecting an accuracy-based
    leaderboard when neither `days` nor `contest_id` are provided,
    otherwise selecting a count-based leaderboard.

    Parameters:
        days (int | None): Optional time window in days to limit
            picks for a count-based leaderboard. If omitted and
            `contest_id` is also omitted, an accuracy-based
            leaderboard is used.
        guild_id (int | None): Optional guild/server ID to filter
            users by their associated guild; if omitted no guild
            filtering is applied.
        contest_id (int | None): Optional contest identifier to
            restrict results to a specific contest; if provided, a
            count-based leaderboard is used.

    Returns:
        LeaderboardData: For an accuracy-based leaderboard, a list of
            tuples (User, accuracy_percent, total_correct, total_picks). For a
            count-based leaderboard, a list of tuples
            (User, total_correct).
    """
    is_accuracy_based = not days and not contest_id

    # Build appropriate query
    if is_accuracy_based:
        query = _build_accuracy_query()
    else:
        query = _build_count_query(days=days, contest_id=contest_id)

    query, use_sql_filter = _try_apply_guild_filter_in_sql(query, guild_id)

    results = session.exec(query).all()
    # If we filtered in SQL, pass None as guild_id to skip Python filtering
    filter_guild_id = None if use_sql_filter else guild_id
    return await _apply_guild_filter(
        results, filter_guild_id, is_accuracy_based
    )


async def create_leaderboard_embed(
    title: str,
    leaderboard_data: LeaderboardData,
    interaction: discord.Interaction,
) -> discord.Embed:
    """
    Create a standardized Discord embed representing a leaderboard.

    The embed's author is set from the interaction user (display name
    and avatar). If `leaderboard_data` is empty the embed description
    will state that the leaderboard is empty; otherwise the
    description contains up to the top 20 formatted entries. The
    function detects whether `leaderboard_data` is accuracy-based or
    count-based and formats each entry accordingly.

    Parameters:
        title (str): Title to display at the top of the embed.
        leaderboard_data (LeaderboardData): Parsed leaderboard rows
            (accuracy-based or count-based) to include in the embed.
        interaction (discord.Interaction): Interaction whose user
            provides the embed author information.

    Returns:
        discord.Embed: A populated embed ready to be sent or edited
            into a message.
    """
    embed = discord.Embed(
        title=title,
        color=discord.Color.dark_gold(),
        timestamp=datetime.now(timezone.utc),
    )
    icon_url = interaction.user.avatar.url if interaction.user.avatar else None
    embed.set_author(name=interaction.user.display_name, icon_url=icon_url)

    if not leaderboard_data:
        embed.description = "The leaderboard is empty."
        return embed

    # Build description from leaderboard entries
    lines = []
    is_accuracy_based = _is_accuracy_based_data(leaderboard_data)

    for i, entry in enumerate(leaderboard_data[:20], 1):
        if is_accuracy_based:
            lines.append(_format_accuracy_entry(entry, i))
        else:
            lines.append(_format_count_entry(entry, i))

    embed.description = "\n".join(lines)
    return embed


def _is_accuracy_based_data(leaderboard_data: LeaderboardData) -> bool:
    """
    Determine whether the leaderboard data is accuracy-based
    (entries as 4-tuples).

    Returns:
        `True` if the first entry is a tuple of length 4 representing
            (User, accuracy, total, picks), `False` otherwise.
    """
    if not leaderboard_data:
        return False
    first = leaderboard_data[0]
    return isinstance(first, tuple) and len(first) == 4


def _format_accuracy_entry(entry, index: int) -> str:
    """
    Format an accuracy-based leaderboard entry into a single display
    string.

    Parameters:
        entry (tuple): A leaderboard row in the form
            (User, accuracy_percent, total_correct, total_picks).
            `accuracy_percent` is a numeric value in percent (0–100).
        index (int): 1-based rank position to display.

    Returns:
        str: A single-line string like
            "**{index}.** {username} - `{accuracy:.2f}%` accuracy",
            where `username` falls back to "User ID: {discord_id}"
            when unavailable.
    """
    user = entry[0]
    username = user.username or f"User ID: {user.discord_id}"
    accuracy = entry[1]
    return f"**{index}.** {username} - `{accuracy:.2f}%` accuracy"


def _format_count_entry(entry, index: int) -> str:
    """
    Format a count-based leaderboard entry line for display.

    Parameters:
        entry (tuple): A (User, total_correct) tuple where `User`
            provides `.username` and `.discord_id`.
        index (int): 1-based rank position to display.

    Returns:
        str: A single-line markdown string like
            "**1.** username - `5` correct picks".
    """
    user = entry[0]
    username = user.username or f"User ID: {user.discord_id}"
    total = entry[1]
    plural = "s" if total != 1 else ""
    return f"**{index}.** {username} - `{total}` correct pick{plural}"


# --- Views ---


class LeaderboardView(discord.ui.View):
    """A view with buttons to switch between leaderboard types."""

    def __init__(self, interaction: discord.Interaction):
        super().__init__(timeout=180)
        self.interaction = interaction

        # Disable "Server" button if not in a guild (DM context)
        if interaction.guild is None:
            for item in self.children:
                if (
                    isinstance(item, discord.ui.Button)
                    and item.label == "Server 🏘️"
                ):
                    item.disabled = True

    async def update_leaderboard(
        self, interaction: discord.Interaction, period: str, days: int = None
    ):
        """
        Refresh the leaderboard embed and update the view's button
        styles to reflect the selected period.

        Updates the original interaction message by setting the active
        button (matching `period`) to primary/disabled, resetting
        other buttons to secondary/enabled, fetching leaderboard data
        with optional time or guild filters, and replacing the embed
        with the newly generated leaderboard.

        Parameters:
            interaction (discord.Interaction): The interaction that
                triggered the update; used to edit the original
                response.
            period (str): The period label to display and mark active
                (e.g., "Global", "Server", "Daily", "Weekly").
            days (int, optional): Number of days to limit the
                leaderboard to (used for time-windowed leaderboards);
                when omitted, no time filter is applied.
        """
        if period == "Server 🏘️" and interaction.guild is None:
            await interaction.response.send_message(
                "The server leaderboard is only available within a server.",
                ephemeral=True,
            )
            return

        await interaction.response.defer()
        with get_session() as session:
            # Only read guild id if interaction occurred in a guild
            guild_id = (
                interaction.guild.id
                if (period == "Server 🏘️" and interaction.guild is not None)
                else None
            )

            # Update button styles
            for item in self.children:
                if isinstance(item, discord.ui.Button):
                    if item.label == period:
                        item.style = discord.ButtonStyle.primary
                        item.disabled = True
                    else:
                        item.style = discord.ButtonStyle.secondary
                        item.disabled = False

            data = await get_leaderboard_data(
                session,
                days=days,
                guild_id=guild_id,
            )
            title = f"{period} Leaderboard"
            embed = await create_leaderboard_embed(
                title,
                data,
                self.interaction,
            )
            await interaction.edit_original_response(embed=embed, view=self)

    @discord.ui.button(label="Global 🌎", style=discord.ButtonStyle.primary)
    async def global_leaderboard(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self.update_leaderboard(interaction, "Global 🌎")

    @discord.ui.button(label="Server 🏘️", style=discord.ButtonStyle.secondary)
    async def server_leaderboard(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self.update_leaderboard(interaction, "Server 🏘️")

    @discord.ui.button(label="Weekly 📅", style=discord.ButtonStyle.secondary)
    async def weekly_leaderboard(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self.update_leaderboard(interaction, "Weekly 📅", days=7)

    @discord.ui.button(label="Daily ☀️", style=discord.ButtonStyle.secondary)
    async def daily_leaderboard(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self.update_leaderboard(interaction, "Daily ☀️", days=1)


class ContestSelectForLeaderboard(discord.ui.Select):
    """A dropdown to select a contest for the leaderboard."""

    def __init__(self, contests: list[Contest]):
        options = [
            discord.SelectOption(label=c.name, value=str(c.id))
            for c in contests
        ]
        super().__init__(placeholder="Choose a contest...", options=options)

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
            data = await get_leaderboard_data(session, contest_id=contest_id)
            title = f"Leaderboard for {contest.name}"
            embed = await create_leaderboard_embed(
                title,
                data,
                interaction,
            )
            await interaction.edit_original_response(embed=embed, view=None)


# --- Commands ---


@app_commands.command(
    name="leaderboard",
    description="Displays the main leaderboards.",
)
async def leaderboard(interaction: discord.Interaction):
    """Shows the main leaderboard with view options."""
    logger.info("'%s' requested the main leaderboard.", interaction.user.name)
    with get_session() as session:
        # Default to global view
        data = await get_leaderboard_data(session)
        embed = await create_leaderboard_embed(
            "Global 🌎 Leaderboard",
            data,
            interaction,
        )
        view = LeaderboardView(interaction)

        await interaction.response.send_message(
            embed=embed,
            view=view,
            ephemeral=True,
        )


@app_commands.command(
    name="leaderboard-contest",
    description="Displays the leaderboard for a specific contest.",
)
async def leaderboard_contest(interaction: discord.Interaction):
    """Shows a dropdown to select a contest leaderboard."""
    logger.info("'%s' requested a contest leaderboard.", interaction.user.name)
    with get_session() as session:
        contests = crud.list_contests(session)
        if not contests:
            await interaction.response.send_message(
                "No contests found.",
                ephemeral=True,
            )
            return

        view = discord.ui.View(timeout=180)
        view.add_item(ContestSelectForLeaderboard(contests=contests[:25]))
        await interaction.response.send_message(
            "Please select a contest:", view=view, ephemeral=True
        )


async def setup(bot_instance):
    bot_instance.tree.add_command(leaderboard)
    bot_instance.tree.add_command(leaderboard_contest)
