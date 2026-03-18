import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone, timedelta
import discord
from src.commands.pick import PickView
from src.models import Match, Contest


@pytest.fixture
def mock_match():
    return Match(
        id=1,
        team1="T1",
        team2="T2",
        scheduled_time=datetime.now(timezone.utc) + timedelta(days=1),
        contest=Contest(
            name="Worlds",
            start_date=datetime.now(timezone.utc),
            end_date=datetime.now(timezone.utc),
        ),
        best_of=1,
        contest_id=1,
    )


@pytest.fixture
def mock_matches(mock_match):
    m2 = Match(
        id=2,
        team1="G2",
        team2="FNC",
        scheduled_time=datetime.now(timezone.utc) + timedelta(days=2),
        contest=Contest(
            name="LEC",
            start_date=datetime.now(timezone.utc),
            end_date=datetime.now(timezone.utc),
        ),
        best_of=3,
        contest_id=1,
    )
    return [mock_match, m2]


@pytest.mark.asyncio
async def test_pick_view_initialization(mock_matches):
    user_picks = {1: "T1"}
    view = PickView(matches=mock_matches, user_picks=user_picks, user_id=123)

    # Check initial state
    assert view.current_index == 0
    assert view.current_match == mock_matches[0]

    # Check button states
    # Row 0: Team Buttons
    # user picked T1, so T1 button should be success, T2 secondary
    assert view.btn_team1.label == "T1"
    assert view.btn_team1.style == discord.ButtonStyle.success
    assert view.btn_team2.style == discord.ButtonStyle.secondary

    # Row 1: Nav
    assert view.btn_prev.disabled is True
    assert view.btn_next.disabled is False


@pytest.mark.asyncio
async def test_pick_view_navigation(mock_matches):
    view = PickView(matches=mock_matches, user_picks={}, user_id=123)
    mock_interaction = AsyncMock(spec=discord.Interaction)
    mock_interaction.response = AsyncMock()

    # Click Next
    await view.on_next(mock_interaction)
    assert view.current_index == 1
    mock_interaction.response.edit_message.assert_called_once()

    # Click Prev
    mock_interaction.response.reset_mock()
    await view.on_prev(mock_interaction)
    assert view.current_index == 0


@pytest.mark.asyncio
@patch("src.commands.pick.get_session")
@patch("src.commands.pick.crud")
async def test_pick_view_pick_logic(mock_crud, mock_get_session, mock_matches):
    view = PickView(matches=mock_matches, user_picks={}, user_id=123)
    view.auto_next = True
    mock_interaction = AsyncMock(spec=discord.Interaction)
    mock_interaction.response = AsyncMock()
    mock_interaction.user.name = "TestUser"

    mock_session = MagicMock()
    mock_get_session.return_value.__enter__.return_value = mock_session
    mock_crud.get_user_by_discord_id.return_value = MagicMock(id=1)
    mock_crud.PickCreateParams = MagicMock()

    # No existing pick, creates new one
    mock_session.exec.return_value.first.return_value = None

    await view.on_team1(mock_interaction)

    # Verify DB calls
    mock_crud.upsert_pick.assert_called_once()

    # Verify state update
    assert view.user_picks[1] == "T1"

    # Verify auto-next
    assert view.current_index == 1
    mock_interaction.response.edit_message.assert_called_once()


@pytest.mark.asyncio
async def test_pick_view_locked_match(mock_matches):
    # Match in the past
    mock_matches[0].scheduled_time = datetime.now(timezone.utc) - timedelta(
        hours=1
    )

    view = PickView(matches=mock_matches, user_picks={}, user_id=123)

    # Check button states for locked match
    assert view.btn_team1.disabled is True
    assert view.btn_team2.disabled is True
    assert "🔒" in view.btn_team1.label
    assert "🔒" in view.btn_team2.label

    # Check embed content
    embed = view.get_embed()
    # Find the "Your Pick" field
    pick_field = next((f for f in embed.fields if f.name == "Your Pick"), None)
    assert pick_field is not None
    assert "(Locked)" in pick_field.value


@pytest.mark.asyncio
async def test_pick_view_embed_uses_compact_match_metadata(mock_match):
    mock_match.game = "lol"
    mock_match.contest.tier = "S"

    view = PickView(matches=[mock_match], user_picks={}, user_id=123)
    embed = view.get_embed()

    assert "**T1 vs T2 — ⏳ Upcoming**" in embed.description
    assert "Worlds • S-tier • LoL • Bo1" in embed.description
    assert "<t:" in embed.description
