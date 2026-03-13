"""add game column to match

Revision ID: 6f9f9a8b7c6d
Revises: e0e204d3db15, add_guild_config_and_live_message
Create Date: 2026-03-13
"""

from alembic import op
import sqlalchemy as sa
import sqlmodel

# revision identifiers, used by Alembic.
revision = "6f9f9a8b7c6d"
down_revision = ("e0e204d3db15", "add_guild_config_and_live_message")
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = {column["name"] for column in inspector.get_columns("match")}

    if "game" not in columns:
        op.add_column(
            "match",
            sa.Column(
                "game",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=True,
                server_default=sa.text("'lol'"),
            ),
        )

    indexes = {index["name"] for index in inspector.get_indexes("match")}
    if "ix_match_game" not in indexes:
        op.create_index("ix_match_game", "match", ["game"], unique=False)


def downgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    indexes = {index["name"] for index in inspector.get_indexes("match")}
    if "ix_match_game" in indexes:
        op.drop_index("ix_match_game", table_name="match")

    columns = {column["name"] for column in inspector.get_columns("match")}
    if "game" in columns:
        op.drop_column("match", "game")
