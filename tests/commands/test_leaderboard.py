import pytest
import discord
from unittest.mock import AsyncMock, MagicMock, patch

from src.commands.leaderboard import LeaderboardView, leaderboard


@pytest.fixture
def mock_interaction():
    """Fixture for a mock discord.Interaction."""
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.response = AsyncMock()
    interaction.followup = AsyncMock()
    interaction.edit_original_response = AsyncMock()
    interaction.user = MagicMock(spec=discord.Member)
    interaction.guild = MagicMock(spec=discord.Guild)
    interaction.guild.id = 67890
    interaction.user.id = 12345
    interaction.user.name = "TestUser"
    interaction.user.avatar = None
    return interaction


@pytest.mark.asyncio
@patch("src.commands.leaderboard.get_session")
@patch("src.commands.leaderboard.get_leaderboard_data")
@patch("src.commands.leaderboard.create_leaderboard_embed")
async def test_leaderboard_command_initial_call(
    mock_create_embed, mock_get_data, mock_get_session, mock_interaction
):
    """Test the initial call to the /leaderboard command."""
    # Arrange
    mock_get_data.return_value = [("user1", 100)]
    mock_embed = discord.Embed(title="Global 🌎 Leaderboard")
    mock_create_embed.return_value = mock_embed

    # Act
    await leaderboard.callback(mock_interaction)

    # Assert
    mock_get_data.assert_called_once_with(
        mock_get_session.return_value.__enter__.return_value
    )
    mock_create_embed.assert_called_once_with(
        "Global 🌎 Leaderboard", [("user1", 100)], mock_interaction
    )
    mock_interaction.response.send_message.assert_called_once()

    args, kwargs = mock_interaction.response.send_message.call_args
    assert kwargs["embed"] == mock_embed
    assert isinstance(kwargs["view"], LeaderboardView)
    assert kwargs["ephemeral"] is True


@pytest.mark.asyncio
@patch("src.commands.leaderboard.get_session")
@patch("src.commands.leaderboard.get_leaderboard_data")
@patch("src.commands.leaderboard.create_leaderboard_embed")
async def test_leaderboard_view_button_click(
    mock_create_embed, mock_get_data, mock_get_session, mock_interaction
):
    """Test clicking a button in the LeaderboardView."""
    # Arrange
    view = LeaderboardView(mock_interaction)

    # Simulate clicking the "Server" button
    button_to_click = None
    for item in view.children:
        if isinstance(item, discord.ui.Button) and item.label == "Server 🏘️":
            button_to_click = item
            break
    assert button_to_click is not None
    assert button_to_click.label == "Server 🏘️"

    # Mock the return values for the update
    mock_get_data.return_value = [("user2", 200)]
    mock_embed = discord.Embed(title="Server 🏘️ Leaderboard")
    mock_create_embed.return_value = mock_embed

    # Act
    await view.update_leaderboard(mock_interaction, "Server 🏘️")

    # Assert
    # Check that button styles are updated correctly
    for item in view.children:
        if isinstance(item, discord.ui.Button):
            if item.label == "Server 🏘️":
                assert item.style == discord.ButtonStyle.primary
                assert item.disabled is True
            else:
                assert item.style == discord.ButtonStyle.secondary
                assert item.disabled is False

    # Check that the leaderboard was updated
    mock_get_data.assert_called_with(
        mock_get_session.return_value.__enter__.return_value,
        days=None,
        guild_id=mock_interaction.guild.id,
    )
    mock_create_embed.assert_called_with(
        "Server 🏘️ Leaderboard", [("user2", 200)], mock_interaction
    )
    mock_interaction.edit_original_response.assert_called_once()

    args, kwargs = mock_interaction.edit_original_response.call_args
    assert kwargs["embed"] == mock_embed
    assert kwargs["view"] == view


@pytest.mark.asyncio
async def test_leaderboard_view_dm_context(mock_interaction):
    """Test that the Server button is disabled in DM context (no guild)."""
    # Arrange
    mock_interaction.guild = None

    # Act
    view = LeaderboardView(mock_interaction)

    # Assert
    server_button = None
    for item in view.children:
        if isinstance(item, discord.ui.Button) and item.label == "Server 🏘️":
            server_button = item
            break
    assert server_button is not None
    assert server_button.disabled is True

    # Check other buttons are enabled
    global_button = None
    for item in view.children:
        if isinstance(item, discord.ui.Button) and item.label == "Global 🌎":
            global_button = item
            break
    assert global_button is not None
    assert global_button.disabled is False
