"""Add user_watchlist table

Revision ID: d9f7a6b5c4e3
Revises: f4e5d6c7b8a9
Create Date: 2026-04-16
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func

revision = "d9f7a6b5c4e3"
down_revision = "f4e5d6c7b8a9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_watchlist",
        sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("match_id", sa.Integer(), nullable=False),
        sa.Column("team_id", sa.Integer(), nullable=True),
        sa.Column("is_watched", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now()),
    )
    op.create_index(op.f("ix_user_watchlist_user_id"), "user_watchlist", ["user_id"], unique=False)
    op.create_index(op.f("ix_user_watchlist_match_id"), "user_watchlist", ["match_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_user_watchlist_match_id"), table_name="user_watchlist")
    op.drop_index(op.f("ix_user_watchlist_user_id"), table_name="user_watchlist")
    op.drop_table("user_watchlist")
