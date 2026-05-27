# src/commands/result.py

import logging
from typing import List

import discord
from discord import app_commands

from src.db import get_session
from src import crud
from src.auth import is_admin
from src.models import Match

logger = logging.getLogger("esports-bot.commands.result")


def _format_match_choice_name(match_obj: Match, has_result: bool) -> str:
    """Formats a match choice name for Discord autocomplete, with
    truncation."""
    prefix = "[HAS RESULT] " if has_result else ""
    name = (
        f"{prefix}{match_obj.team1} vs {match_obj.team2} "
        f"(ID: {match_obj.id})"
    )
    if len(name) <= 100:
        return name
    suffix = f"... (ID: {match_obj.id})"
    max_len = 100 - len(suffix)
    return f"{name[:max_len]}{suffix}"


async def winner_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    """Autocompletes the winner argument with the teams from the specified
    match."""
    match_id_str = interaction.namespace.match_id
    if not match_id_str:
        return []

    try:
        match_id = int(match_id_str)
    except ValueError:
        return []

    with get_session() as session:
        match = crud.get_match_by_id(session, match_id)

        if not match:
            return []

        choices = [
            app_commands.Choice(name=match.team1, value=match.team1),
            app_commands.Choice(name=match.team2, value=match.team2),
        ]

        # Filter choices based on what the user has already typed
        return [
            choice
            for choice in choices
            if current.lower() in choice.name.lower()
        ]


async def match_autocompletion(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[int]]:
    """Autocomplete for matches, prioritizing those without results."""
    current_lc = current.lower()
    choices: list[app_commands.Choice[int]] = []

    with get_session() as session:
        # list_all_matches now eagerly loads results
        all_matches = crud.list_all_matches(session)

    # Sort: matches without results first (False < True)
    sorted_matches = sorted(all_matches, key=lambda m: m.result is not None)

    for match in sorted_matches:
        has_result = match.result is not None
        choice_name = _format_match_choice_name(match, has_result)

        if current_lc in choice_name.lower():
            choices.append(
                app_commands.Choice(name=choice_name, value=match.id)
            )

        if len(choices) >= 25:
            break

    return choices


@app_commands.command(
    name="enter-result",
    description="Enter the result of a match (Admin only).",
)
@app_commands.describe(
    match_id="The ID of the match to enter a result for.",
    winner="The winning team.",
)
@app_commands.autocomplete(
    match_id=match_autocompletion, winner=winner_autocomplete
)
@is_admin()
async def enter_result(
    interaction: discord.Interaction,
    match_id: int,
    winner: str,
):
    """Admin command to enter a match result and score all related picks."""

    logger.info(
        "Admin '%s' is entering a result for match %s.",
        interaction.user.name,
        match_id,
    )
    await interaction.response.defer(ephemeral=True)

    with get_session() as session:
        # --- Validation ---
        match = crud.get_match_by_id(session, match_id)
        if not match:
            await interaction.followup.send(
                f"Match with ID {match_id} not found.", ephemeral=True
            )
            return

        if winner not in [match.team1, match.team2]:
            await interaction.followup.send(
                f"Invalid winner. Please choose either '{match.team1}' or "
                f"'{match.team2}'.",
                ephemeral=True,
            )
            return

        if crud.get_result_for_match(session, match_id):
            await interaction.followup.send(
                f"A result for match {match_id} has already been entered.",
                ephemeral=True,
            )
            return

        # --- Process Result and Score Picks ---
        try:
            # 1. Create the result
            crud.create_result(session, match_id=match_id, winner=winner)

            # 2. Bulk update all picks for the match to prevent N+1 queries
            from sqlalchemy import update
            from src.models import Pick

            # Bulk update correct picks
            stmt_correct = (
                update(Pick)
                .where(Pick.match_id == match_id, Pick.chosen_team == winner)
                .values(is_correct=True, status="correct", score=10)
            )

            # Bulk update incorrect picks
            stmt_incorrect = (
                update(Pick)
                .where(Pick.match_id == match_id, Pick.chosen_team != winner)
                .values(is_correct=False, status="incorrect", score=0)
            )

            res_correct = session.exec(stmt_correct)
            res_incorrect = session.exec(stmt_incorrect)

            updated_picks_count = res_correct.rowcount + res_incorrect.rowcount

            session.commit()

            await interaction.followup.send(
                (
                    "Result for match "
                    f"**{match.team1} vs {match.team2}** has been entered as "
                    f"**{winner}**.\nProcessed and scored "
                    f"{updated_picks_count} user picks."
                ),
                ephemeral=True,
            )

        except Exception as e:
            logger.exception("Error entering result for match %s", match_id)
            session.rollback()
            await interaction.followup.send(
                f"An unexpected error occurred: {e}", ephemeral=True
            )


async def setup(bot):
    bot.tree.add_command(enter_result)
