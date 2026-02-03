"""add_unique_constraint_to_pick

Revision ID: 549c0b6c69a6
Revises: e0e204d3db15
Create Date: 2026-02-02 12:28:26.959711

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "549c0b6c69a6"
down_revision = "e0e204d3db15"
branch_labels = None
depends_on = None


def upgrade():
    # Step 1: Remove duplicate picks before applying constraint
    # Keep only the most recent pick for each (user_id, match_id) pair
    conn = op.get_bind()
    
    # Find and delete duplicate picks, keeping only the one with the highest ID (most recent)
    conn.execute(sa.text("""
        DELETE FROM pick
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM pick
            GROUP BY user_id, match_id
        )
    """))
    
    # Step 2: Add the unique constraint to pick table
    with op.batch_alter_table("pick", schema=None) as batch_op:
        batch_op.create_unique_constraint(
            "uq_pick_user_match", ["user_id", "match_id"]
        )


def downgrade():
    # Removing the unique constraint
    with op.batch_alter_table("pick", schema=None) as batch_op:
        batch_op.drop_constraint("uq_pick_user_match", type_="unique")
