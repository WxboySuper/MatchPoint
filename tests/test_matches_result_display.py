import pytest
import discord
from unittest.mock import MagicMock
from datetime import datetime, timezone
from src.commands.matches import create_matches_embed
from src.models import Match, Contest, Result


@pytest.mark.asyncio
async def test_create_matches_embed_with_result():
    # Arrange
    interaction = MagicMock(spec=discord.Interaction)
    interaction.user.display_name = "TestUser"
    interaction.user.avatar = None

    # Use MagicMock with `spec` to avoid SQLModel validation/construct
    contest = MagicMock(spec=Contest)
    contest.name = "Test Tournament"
    contest.tier = None

    match_no_result = MagicMock(spec=Match)
    match_no_result.team1 = "Team A"
    match_no_result.team2 = "Team B"
    match_no_result.scheduled_time = datetime(
        2025, 12, 26, 12, 0, tzinfo=timezone.utc
    )
    match_no_result.contest = contest
    match_no_result.result = None
    match_no_result.status = "not_started"
    match_no_result.best_of = 3
    match_no_result.game = "lol"
    match_no_result.last_announced_score = None

    match_with_result = MagicMock(spec=Match)
    match_with_result.team1 = "Team C"
    match_with_result.team2 = "Team D"
    match_with_result.scheduled_time = datetime(
        2025, 12, 26, 15, 0, tzinfo=timezone.utc
    )
    match_with_result.contest = contest
    match_with_result.status = "finished"
    match_with_result.best_of = 3
    match_with_result.game = "cs2"
    match_with_result.last_announced_score = None
    result = MagicMock(spec=Result)
    result.winner = "Team C"
    result.score = "2-0"
    match_with_result.result = result

    matches = [match_no_result, match_with_result]

    # Act
    embed = await create_matches_embed("Test Matches", matches, interaction)

    # Assert
    assert len(embed.fields) == 2

    # Field 0: No result
    assert embed.fields[0].name == "Team A vs Team B — ⏳ Upcoming"
    assert "Final:" not in embed.fields[0].value
    assert "Test Tournament • LoL • Bo3" in embed.fields[0].value
    assert "<t:" in embed.fields[0].value

    # Field 1: With result
    assert embed.fields[1].name == "Team C vs Team D — ✅ Finished"
    assert "Final: **Team C** won (2-0)" in embed.fields[1].value
    assert "Test Tournament • CS2 • Bo3" in embed.fields[1].value


@pytest.mark.asyncio
async def test_create_matches_embed_with_tier_metadata():
    interaction = MagicMock(spec=discord.Interaction)
    interaction.user.display_name = "TestUser"
    interaction.user.avatar = None

    contest = MagicMock(spec=Contest)
    contest.name = "First Stand"
    contest.tier = "S"

    match = MagicMock(spec=Match)
    match.team1 = "T1"
    match.team2 = "GEN"
    match.scheduled_time = datetime(2025, 12, 26, 18, 0, tzinfo=timezone.utc)
    match.contest = contest
    match.result = None
    match.status = "not_started"
    match.best_of = 5
    match.game = "lol"
    match.last_announced_score = None

    embed = await create_matches_embed("Tier Test", [match], interaction)

    assert len(embed.fields) == 1
    assert "First Stand • S-tier • LoL • Bo5" in embed.fields[0].value


@pytest.mark.asyncio
async def test_create_matches_embed_no_matches():
    # Arrange
    interaction = MagicMock(spec=discord.Interaction)
    interaction.user.display_name = "TestUser"
    interaction.user.avatar = None
    matches = []

    # Act
    embed = await create_matches_embed("Empty", matches, interaction)

    # Assert
    assert embed.description == "No matches found."
    assert len(embed.fields) == 0
