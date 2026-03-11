import discord
from discord import app_commands, ui, Color, SelectOption
from discord.ext import commands
from discord.utils import get

from src.auth import is_admin

CATEGORY_NAME = "esports-pickem"
CHANNEL_NAME = "pickem-announcements"

ANNOUNCEMENT_TYPES = {
    "bug": {"label": "Bug Notification", "emoji": "🐛", "color": Color.red()},
    "update": {
        "label": "Update Notification",
        "emoji": "✨",
        "color": Color.green(),
    },
    "reminder": {
        "label": "Match Reminder",
        "emoji": "🗓️",
        "color": Color.blue(),
    },
    "result": {
        "label": "Result Notification",
        "emoji": "🏆",
        "color": Color.purple(),
    },
    "status": {"label": "Bot Status", "emoji": "🤖", "color": Color.gold()},
    "general": {
        "label": "General Announcement",
        "emoji": "📣",
        "color": Color.default(),
    },
}


class AnnouncementModal(ui.Modal, title="Create Announcement"):
    title_input = ui.TextInput(
        label="Title",
        placeholder="Enter the title of the announcement",
        max_length=100,
    )
    message_input = ui.TextInput(
        label="Message",
        placeholder="Enter the announcement message",
        style=discord.TextStyle.long,
        max_length=2000,
    )

    def __init__(self, bot: commands.Bot, announcement_type: str):
        super().__init__()
        self.bot = bot
        self.announcement_type = announcement_type

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if not guild:
            await interaction.followup.send(
                "This command can only be used in a server.", ephemeral=True
            )
            return

        # Find or create the category
        category = get(guild.categories, name=CATEGORY_NAME)
        if category is None:
            category = await guild.create_category(CATEGORY_NAME)

        # Define channel permissions
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                read_messages=True,
                read_message_history=True,
                add_reactions=True,
                send_messages=False,
            )
        }

        # Find or create the channel
        channel = get(category.text_channels, name=CHANNEL_NAME)
        if channel is None:
            channel = await category.create_text_channel(
                CHANNEL_NAME, overwrites=overwrites
            )

        # Get announcement details
        announcement_details = ANNOUNCEMENT_TYPES[self.announcement_type]
        embed_color = announcement_details["color"]
        embed_title = self.title_input.value
        embed_message = self.message_input.value

        # Create the embed
        embed = discord.Embed(
            title=embed_title,
            description=embed_message,
            color=embed_color,
        )
        embed.set_author(
            name=f"Announcement from {interaction.user.display_name}",
            icon_url=(
                interaction.user.avatar.url
                if interaction.user.avatar
                else None
            ),
        )
        embed.set_footer(text=announcement_details["label"])

        # Send the announcement immediately (preserve existing behavior/tests)
        # Also, if the guild has a persisted live message configured, update that pointer
        await channel.send(embed=embed)
        try:
            from src.db import get_session
            from src.crud import set_live_message

            with get_session() as session:
                set_live_message(session, getattr(guild, "id", 0), getattr(channel, "id", 0), 0)
        except Exception:
            # Non-fatal: we do not require live message pointer during announce
            pass

        # Send confirmation to the admin
        await interaction.followup.send(
            f"Announcement successfully sent to #{channel.name}!",
            ephemeral=True,
        )


class AnnouncementTypeSelect(ui.Select):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        options = [
            SelectOption(
                label=details["label"],
                value=type_key,
                emoji=details["emoji"],
            )
            for type_key, details in ANNOUNCEMENT_TYPES.items()
        ]
        super().__init__(
            placeholder="Select an announcement type...",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        modal = AnnouncementModal(self.bot, self.values[0])
        await interaction.response.send_modal(modal)
        self.view.stop()


class AnnounceView(ui.View):
    def __init__(self, bot: commands.Bot):
        super().__init__(timeout=180)
        self.add_item(AnnouncementTypeSelect(bot))


class Announce(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="announce", description="Create and send an announcement."
    )
    @is_admin()
    async def announce(self, interaction: discord.Interaction):
        """Creates a modal for sending an announcement."""
        view = AnnounceView(self.bot)
        await interaction.response.send_message(
            "Please select an announcement type:", view=view, ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Announce(bot))
