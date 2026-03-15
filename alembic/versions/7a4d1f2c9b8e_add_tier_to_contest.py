"""add_tier_to_contest

Revision ID: 7a4d1f2c9b8e
Revises: c1d2e3f4a5b6
Create Date: 2026-03-14 21:30:00.000000

"""

from alembic import op
import sqlalchemy as sa
import sqlmodel

revision = "7a4d1f2c9b8e"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("contest", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "tier", sqlmodel.sql.sqltypes.AutoString(), nullable=True
            )
        )
        batch_op.create_index(
            batch_op.f("ix_contest_tier"),
            ["tier"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("contest", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_contest_tier"))
        batch_op.drop_column("tier")
