"""Add reminder_channel_id to guildconfig

Revision ID: add_guildconfig_reminder_channel
Revises: d9f7a6b5c4e3
Create Date: 2026-04-16
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_guildconfig_reminder_channel"
down_revision = "d9f7a6b5c4e3"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "guildconfig" not in inspector.get_table_names():
        # Table missing (unexpected) — skip
        return

    # Add column if it doesn't exist
    cols = [c["name"] for c in inspector.get_columns("guildconfig")]
    if "reminder_channel_id" in cols:
        return

    op.add_column(
        "guildconfig",
        sa.Column("reminder_channel_id", sa.BigInteger(), nullable=True),
    )
    op.create_index(
        "ix_guildconfig_reminder_channel_id",
        "guildconfig",
        ["reminder_channel_id"],
        unique=False,
    )


def downgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "guildconfig" in inspector.get_table_names():
        cols = [c["name"] for c in inspector.get_columns("guildconfig")]
        if "reminder_channel_id" in cols:
            indexes = {i["name"] for i in inspector.get_indexes("guildconfig")}
            if "ix_guildconfig_reminder_channel_id" in indexes:
                op.drop_index(
                    "ix_guildconfig_reminder_channel_id",
                    table_name="guildconfig",
                )
            op.drop_column("guildconfig", "reminder_channel_id")
