import pytest
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Session, create_engine
from src import crud


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


def test_pick_unique_constraint(session: Session):
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

    # First pick
    pick1 = crud.create_pick(
        session,
        crud.PickCreateParams(
            user_id=user.id,
            contest_id=contest.id,
            match_id=match.id,
            chosen_team="A",
        ),
    )
    assert pick1.id is not None

    # Duplicate pick (same user, same match)
    with pytest.raises(IntegrityError):
        crud.create_pick(
            session,
            crud.PickCreateParams(
                user_id=user.id,
                contest_id=contest.id,
                match_id=match.id,
                chosen_team="B",  # Even if team is different, should fail
            ),
        )


def test_upsert_pick_handles_race_condition(session: Session):
    # Setup data
    user = crud.create_user(session, discord_id="u2", username="u2")
    contest = crud.create_contest(
        session,
        {
            "name": "Test Contest 2",
            "start_date": datetime.now(),
            "end_date": datetime.now(),
            "leaguepedia_id": "c2",
        },
    )
    match = crud.create_match(
        session,
        crud.MatchCreateParams(
            contest_id=contest.id,
            team1="A",
            team2="B",
            scheduled_time=datetime.now(),
            leaguepedia_id="m2",
        ),
    )

    # 1. Create initial pick using upsert
    params1 = crud.PickCreateParams(
        user_id=user.id,
        contest_id=contest.id,
        match_id=match.id,
        chosen_team="A",
    )
    pick1 = crud.upsert_pick(session, params1)
    assert pick1.id is not None
    assert pick1.chosen_team == "A"

    # 2. Update pick using upsert (standard update path)
    params2 = crud.PickCreateParams(
        user_id=user.id,
        contest_id=contest.id,
        match_id=match.id,
        chosen_team="B",
    )
    pick2 = crud.upsert_pick(session, params2)
    assert pick2.id == pick1.id
    assert pick2.chosen_team == "B"

    # 3. Verify consistency
    pick3 = crud.upsert_pick(session, params1)  # Change back to A
    assert pick3.id == pick1.id
    assert pick3.chosen_team == "A"
