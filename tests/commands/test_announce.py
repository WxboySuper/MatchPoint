import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import discord
from discord import ui

from src.commands.announce import (
    Announce,
    AnnounceView,
    AnnouncementModal,
    CATEGORY_NAME,
    CHANNEL_NAME,
)


@pytest.fixture
def mock_bot():
    """Fixture for a mock bot."""
    return MagicMock()


@pytest.fixture
def mock_interaction():
    """Fixture for a mock interaction."""
    interaction = AsyncMock(spec=discord.Interaction)
    interaction.user = MagicMock(spec=discord.User)
    interaction.user.id = 12345  # A default admin ID for testing
    interaction.response = AsyncMock(spec=discord.InteractionResponse)
    interaction.followup = AsyncMock(spec=discord.Webhook)
    interaction.guild = AsyncMock(spec=discord.Guild)
    return interaction


@pytest.mark.asyncio
async def test_announce_command_sends_view(mock_bot, mock_interaction):
    """Test that the /announce command responds with the AnnounceView."""
    cog = Announce(mock_bot)
    await cog.announce.callback(cog, mock_interaction)

    mock_interaction.response.send_message.assert_called_once()
    args, kwargs = mock_interaction.response.send_message.call_args
    assert "Please select an announcement type:" in args
    assert isinstance(kwargs["view"], AnnounceView)
    assert kwargs["ephemeral"] is True


@pytest.mark.asyncio
async def test_announcement_type_select_callback(mock_bot, mock_interaction):
    """Test the select dropdown callback sends the modal."""
    view = AnnounceView(mock_bot)
    select = view.children[0]
    view.stop = MagicMock()

    # Patch the 'values' property to simulate a selection
    with patch(
        "src.commands.announce.AnnouncementTypeSelect.values",
        new_callable=PropertyMock,
    ) as mock_values:
        mock_values.return_value = ["bug"]
        await select.callback(mock_interaction)

    mock_interaction.response.send_modal.assert_called_once()
    modal = mock_interaction.response.send_modal.call_args[0][0]
    assert isinstance(modal, AnnouncementModal)
    assert modal.announcement_type == "bug"


@pytest.mark.asyncio
async def test_modal_on_submit(mock_bot, mock_interaction, monkeypatch):
    """Test the modal submission logic."""
    # Arrange
    mock_get = MagicMock(return_value=None)
    monkeypatch.setattr("src.commands.announce.get", mock_get)

    modal = AnnouncementModal(mock_bot, "update")
    modal.title_input = MagicMock(spec=ui.TextInput, value="Test Title")
    modal.message_input = MagicMock(spec=ui.TextInput, value="Test Message")

    mock_session = AsyncMock()
    with patch(
        "src.commands.announce.get_async_session"
    ) as mock_session_factory, patch(
        "src.commands.announce.set_live_message_async",
        new_callable=AsyncMock,
    ) as mock_set_live_message:
        mock_session_factory.return_value.__aenter__.return_value = (
            mock_session
        )

        # Act
        await modal.on_submit(mock_interaction)

        # Assert
        mock_interaction.guild.create_category.assert_called_once_with(
            CATEGORY_NAME
        )
        category = mock_interaction.guild.create_category.return_value
        category.create_text_channel.assert_called_once()
        args, _ = category.create_text_channel.call_args
        assert args[0] == CHANNEL_NAME

        channel = category.create_text_channel.return_value
        channel.send.assert_called_once()
        embed = channel.send.call_args[1]["embed"]
        assert embed.title == "Test Title"
        assert embed.description == "Test Message"
        assert "Update Notification" in embed.footer.text
        mock_set_live_message.assert_awaited_once()

        mock_interaction.followup.send.assert_called_once_with(
            f"Announcement successfully sent to #{channel.name}!",
            ephemeral=True,
        )
