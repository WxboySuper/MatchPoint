import pytest
from datetime import datetime, timezone

from sqlmodel import SQLModel, Session, create_engine

from src import crud
from src.crud.watchlist import add_watch, list_watches_for_user, remove_watch, mark_as_watched


@pytest.fixture()
def local_session(tmp_path):
    db_path = tmp_path / "test_watchlist.db"
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        try:
            yield s
        finally:
            engine.dispose()


def test_watchlist_crud_happy_path(local_session: Session):
    # Create minimal data: user and a match via existing CRUD helpers
    user = crud.create_user(local_session, discord_id="u123", username="u123")
    contest = crud.create_contest(
        local_session,
        {
            "name": "TestContest",
            "start_date": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "end_date": datetime(2025, 1, 2, tzinfo=timezone.utc),
            "leaguepedia_id": "tc",
        },
    )
    match = crud.create_match(
        local_session,
        crud.MatchCreateParams(
            contest_id=contest.id,
            team1="A",
            team2="B",
            scheduled_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
            leaguepedia_id="m1",
        ),
    )

    # Add watch
    w = add_watch(local_session, str(user.discord_id), match.id)
    assert w.id is not None

    # List
    listings = list_watches_for_user(local_session, str(user.discord_id))
    assert len(listings) == 1
    assert listings[0].match_id == match.id

    # Mark as watched
    updated = mark_as_watched(local_session, w.id)
    assert updated is not None and updated.is_watched is True

    # Remove
    ok = remove_watch(local_session, w.id)
    assert ok is True
    assert list_watches_for_user(local_session, str(user.discord_id)) == []
