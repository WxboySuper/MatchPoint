"""add rate_limit_state table

Revision ID: rate_limit_state_001
Revises: 578d2e46a72d
Create Date: 2026-04-01
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "rate_limit_state_001"
down_revision = "578d2e46a72d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ratelimitstate",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("resource", sa.String, nullable=False, unique=True, index=True),
        sa.Column("remaining", sa.Integer, nullable=False, server_default="1000"),
        sa.Column("limit", sa.Integer, nullable=False, server_default="1000"),
        sa.Column("reset_at", sa.String(64), nullable=True),
        sa.Column("updated_at", sa.String(64), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("ratelimitstate")
