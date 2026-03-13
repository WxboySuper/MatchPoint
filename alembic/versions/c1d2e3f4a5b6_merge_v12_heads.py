"""Merge v1.2 Alembic heads.

Revision ID: c1d2e3f4a5b6
Revises: 549c0b6c69a6, 6f9f9a8b7c6d
Create Date: 2026-03-13
"""

from collections.abc import Sequence

revision: str = "c1d2e3f4a5b6"
down_revision: str | Sequence[str] | None = (
    "549c0b6c69a6",
    "6f9f9a8b7c6d",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Merge the v1.2 migration branches."""


def downgrade() -> None:
    """Split the v1.2 migration branches."""
