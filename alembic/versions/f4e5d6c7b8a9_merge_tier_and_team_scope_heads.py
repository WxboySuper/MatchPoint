"""Merge contest tier and team scope heads.

Revision ID: f4e5d6c7b8a9
Revises: 7a4d1f2c9b8e, 8f7c6b5a4d3e
Create Date: 2026-03-15
"""

from collections.abc import Sequence


revision = "f4e5d6c7b8a9"
down_revision: str | Sequence[str] | None = (
    "7a4d1f2c9b8e",
    "8f7c6b5a4d3e",
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
