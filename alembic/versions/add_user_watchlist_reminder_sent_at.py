"""Add reminder_sent_at to user_watchlist

Revision ID: add_user_watchlist_reminder_sent_at
Revises: d9f7a6b5c4e3
Create Date: 2026-04-17
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "add_user_watchlist_reminder_sent_at"
down_revision = "d9f7a6b5c4e3"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "user_watchlist" not in inspector.get_table_names():
        # Table missing; nothing to do
        return

    cols = [c["name"] for c in inspector.get_columns("user_watchlist")]
    if "reminder_sent_at" in cols:
        return

    op.add_column(
        "user_watchlist",
        sa.Column("reminder_sent_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )


def downgrade():
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    if "user_watchlist" in inspector.get_table_names():
        cols = [c["name"] for c in inspector.get_columns("user_watchlist")]
        if "reminder_sent_at" in cols:
            op.drop_column("user_watchlist", "reminder_sent_at")
