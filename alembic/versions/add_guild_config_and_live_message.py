"""Add GuildConfig and LiveUpdateMessage tables

Revision ID: add_guild_config_and_live_message
Revises: 578d2e46a72d
Create Date: 2026-03-10
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_guild_config_and_live_message"
down_revision = "578d2e46a72d"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "guildconfig" in inspector.get_table_names():
        # Already applied
        return

    op.create_table(
        "guildconfig",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("guild_id", sa.BigInteger(), nullable=False),
        sa.Column("announcement_channel_id", sa.BigInteger(), nullable=True),
        sa.Column("live_updates_channel_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "setup_completed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("enabled_games", sa.String(), nullable=True),
    )
    op.create_index(
        op.f("ix_guildconfig_guild_id"),
        "guildconfig",
        ["guild_id"],
        unique=True,
    )

    if "liveupdatemessage" not in inspector.get_table_names():
        op.create_table(
            "liveupdatemessage",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("guild_id", sa.BigInteger(), nullable=False),
            sa.Column("channel_id", sa.BigInteger(), nullable=False),
            sa.Column("message_id", sa.BigInteger(), nullable=False),
            sa.Column(
                "scope_type",
                sa.String(),
                nullable=False,
                server_default=sa.text("'guild_live'"),
            ),
            sa.Column("scope_key", sa.String(), nullable=True),
            sa.Column("last_rendered_at", sa.String(), nullable=True),
        )
        op.create_index(
            "ix_liveupdatemessage_guild_id",
            "liveupdatemessage",
            ["guild_id"],
            unique=False,
        )
        op.create_index(
            "ix_liveupdatemessage_channel_id",
            "liveupdatemessage",
            ["channel_id"],
            unique=False,
        )
        op.create_index(
            "ix_liveupdatemessage_message_id",
            "liveupdatemessage",
            ["message_id"],
            unique=False,
        )


def downgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if "liveupdatemessage" in inspector.get_table_names():
        live_indexes = {
            index["name"]
            for index in inspector.get_indexes("liveupdatemessage")
        }
        for index_name in (
            "ix_liveupdatemessage_guild_id",
            "ix_liveupdatemessage_channel_id",
            "ix_liveupdatemessage_message_id",
        ):
            if index_name in live_indexes:
                op.drop_index(index_name, table_name="liveupdatemessage")
        op.drop_table("liveupdatemessage")
    if "guildconfig" in inspector.get_table_names():
        guild_indexes = {
            index["name"] for index in inspector.get_indexes("guildconfig")
        }
        guild_index_name = op.f("ix_guildconfig_guild_id")
        if guild_index_name in guild_indexes:
            op.drop_index(guild_index_name, table_name="guildconfig")
        op.drop_table("guildconfig")
