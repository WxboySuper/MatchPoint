"""Scope team uniqueness by game.

Revision ID: 8f7c6b5a4d3e
Revises: c1d2e3f4a5b6
Create Date: 2026-03-14
"""

from alembic import op
import sqlalchemy as sa
import sqlmodel

TEAM_TABLE = "team"
GAME_COLUMN = "game"
GAME_DEFAULT = "lol"

# revision identifiers, used by Alembic.
revision = "8f7c6b5a4d3e"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade():
    _add_team_game_column()
    _backfill_team_game()
    _rebuild_team_table_for_game_scope()


def downgrade():
    conn = op.get_bind()
    _assert_no_duplicate_rows(conn, "name")
    _assert_no_duplicate_rows(conn, "pandascore_id")
    _restore_global_team_uniqueness()


def _add_team_game_column() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns(TEAM_TABLE)}
    if GAME_COLUMN in columns:
        return
    op.add_column(
        TEAM_TABLE,
        sa.Column(
            GAME_COLUMN,
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=True,
            server_default=sa.text("'lol'"),
        ),
    )


def _backfill_team_game() -> None:
    conn = op.get_bind()
    team1_col, team2_col = _match_team_id_columns()
    if team1_col and team2_col:
        conn.execute(
            sa.text(
                f"""
                UPDATE team
                   SET game = COALESCE(
                       (
                           SELECT match.game
                             FROM match
                            WHERE match.{team1_col} = team.pandascore_id
                              AND match.game IS NOT NULL
                            LIMIT 1
                       ),
                       (
                           SELECT match.game
                             FROM match
                            WHERE match.{team2_col} = team.pandascore_id
                              AND match.game IS NOT NULL
                            LIMIT 1
                       ),
                       game
                   )
                 WHERE pandascore_id IS NOT NULL
                   AND (game IS NULL OR TRIM(game) = '')
                """
            )
        )
    conn.execute(
        sa.text("""
            UPDATE team
               SET game = :default_game
             WHERE game IS NULL OR TRIM(game) = ''
            """),
        {"default_game": GAME_DEFAULT},
    )


def _match_team_id_columns() -> tuple[str | None, str | None]:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("match")}
    return (
        _preferred_column(columns, "team1_id", "pandascore_team1_id"),
        _preferred_column(columns, "team2_id", "pandascore_team2_id"),
    )


def _preferred_column(
    columns: set[str], preferred: str, legacy: str
) -> str | None:
    if preferred in columns:
        return preferred
    if legacy in columns:
        return legacy
    return None


def _rebuild_team_table_for_game_scope() -> None:
    indexes = _index_names(TEAM_TABLE)
    with op.batch_alter_table(TEAM_TABLE, recreate="always") as batch_op:
        _drop_index_if_present(batch_op, indexes, "ix_team_name")
        _drop_index_if_present(batch_op, indexes, "ix_team_pandascore_id")
        _drop_index_if_present(batch_op, indexes, "ix_team_game")
        batch_op.alter_column(
            GAME_COLUMN,
            existing_type=sqlmodel.sql.sqltypes.AutoString(),
            nullable=False,
            server_default=sa.text("'lol'"),
        )
        batch_op.create_index("ix_team_game", [GAME_COLUMN], unique=False)
        batch_op.create_index("ix_team_name", ["name"], unique=False)
        batch_op.create_index(
            "ix_team_pandascore_id", ["pandascore_id"], unique=False
        )
        batch_op.create_unique_constraint(
            "uq_team_name_game", ["name", GAME_COLUMN]
        )
        batch_op.create_unique_constraint(
            "uq_team_pandascore_id_game",
            ["pandascore_id", GAME_COLUMN],
        )


def _restore_global_team_uniqueness() -> None:
    indexes = _index_names(TEAM_TABLE)
    constraints = _unique_constraint_names(TEAM_TABLE)
    with op.batch_alter_table(TEAM_TABLE, recreate="always") as batch_op:
        _drop_constraint_if_present(
            batch_op, constraints, "uq_team_name_game"
        )
        _drop_constraint_if_present(
            batch_op,
            constraints,
            "uq_team_pandascore_id_game",
        )
        _drop_index_if_present(batch_op, indexes, "ix_team_game")
        _drop_index_if_present(batch_op, indexes, "ix_team_name")
        _drop_index_if_present(batch_op, indexes, "ix_team_pandascore_id")
        batch_op.create_index("ix_team_name", ["name"], unique=True)
        batch_op.create_index(
            "ix_team_pandascore_id", ["pandascore_id"], unique=True
        )
        batch_op.drop_column(GAME_COLUMN)


def _index_names(table: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {index["name"] for index in inspector.get_indexes(table)}


def _unique_constraint_names(table: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {
        constraint["name"]
        for constraint in inspector.get_unique_constraints(table)
        if constraint["name"]
    }


def _drop_constraint_if_present(
    batch_op, existing: set[str], name: str
) -> None:
    if name in existing:
        batch_op.drop_constraint(name, type_="unique")


def _drop_index_if_present(batch_op, existing: set[str], name: str) -> None:
    if name in existing:
        batch_op.drop_index(name)


def _assert_no_duplicate_rows(conn, column: str) -> None:
    value_filter = "IS NOT NULL" if column == "pandascore_id" else "!= ''"
    stmt = sa.text(f"""
        SELECT COUNT(1)
          FROM (
                SELECT {column}
                  FROM {TEAM_TABLE}
                 WHERE {column} {value_filter}
                 GROUP BY {column}
                HAVING COUNT(1) > 1
               ) AS duplicates
        """)
    duplicate_count = int(conn.execute(stmt).scalar() or 0)
    if duplicate_count == 0:
        return
    raise RuntimeError(
        "Cannot downgrade team game scoping while duplicate "
        f"{column} values exist across games ({duplicate_count} duplicates)."
    )
