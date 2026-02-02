import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Session, create_engine, select
from src import crud
from src.models import Pick


@pytest.fixture()
def session(tmp_path):
    """Provide a fresh SQLite database session for each test."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        try:
            yield s
        finally:
            engine.dispose()


def test_upsert_pick_handles_race_condition_logic(session: Session, monkeypatch):
    """
    Test that upsert_pick correctly recovers from an IntegrityError
    (simulating a race condition where the initial check returns None
    but the insert fails because a record exists).
    """
    # Setup data
    user = crud.create_user(session, discord_id="u1", username="u1")
    contest = crud.create_contest(
        session,
        {
            "name": "Test Contest",
            "start_date": datetime.now(),
            "end_date": datetime.now(),
            "leaguepedia_id": "c1",
        },
    )
    match = crud.create_match(
        session,
        crud.MatchCreateParams(
            contest_id=contest.id,
            team1="A",
            team2="B",
            scheduled_time=datetime.now(),
            leaguepedia_id="m1",
        ),
    )

    # 1. Pre-seed the DB with a pick (Team A)
    existing_pick = crud.create_pick(
        session,
        crud.PickCreateParams(
            user_id=user.id,
            contest_id=contest.id,
            match_id=match.id,
            chosen_team="A",
        )
    )

    # 2. Mock the initial existence check to return None
    # This simulates "we didn't see it" (Race condition Step 1)
    original_exec = session.exec

    def side_effect(statement):
        # If looking for this specific pick, pretend it's not there ONLY ONCE
        # The logic calls exec twice: once at start, once in catch block.
        # We need the first one to fail (return empty), second to succeed.
        s_str = str(statement)
        if "pick" in s_str.lower() and "user_id" in s_str.lower():
            # This is a bit brittle but sufficient for a unit test of logic flow
            # We'll use a counter to return None only the first time
            if not hasattr(side_effect, "called"):
                side_effect.called = True
                mock_res = MagicMock()
                mock_res.first.return_value = None
                return mock_res
        return original_exec(statement)

    monkeypatch.setattr(session, "exec", side_effect)

    # 3. Call upsert_pick trying to change to Team B
    # - Check returns None (mocked)
    # - create_pick called -> Raises IntegrityError (Real DB has unique constraint)
    # - Catch IntegrityError
    # - Check again (Real DB) -> Finds Pick
    # - Updates to Team B
    params = crud.PickCreateParams(
        user_id=user.id,
        contest_id=contest.id,
        match_id=match.id,
        chosen_team="B",
    )

    # We need to ensure create_pick actually raises the right error string
    # for SQLite ("UNIQUE constraint failed")
    updated_pick = crud.upsert_pick(session, params)

    assert updated_pick.id == existing_pick.id
    assert updated_pick.chosen_team == "B"
    
    # Verify the database state
    session.refresh(existing_pick)
    assert existing_pick.chosen_team == "B"

