import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timedelta, timezone

from src.models import Match, Contest, Result
from src.pandascore_polling import poll_live_match_job


def _mock_poll_session(with_commit: bool = False):
    session = MagicMock()
    session.exec = AsyncMock()
    result = MagicMock()
    result.first.return_value = None
    session.exec.return_value = result
    if with_commit:
        session.commit = AsyncMock()
        session.flush = AsyncMock()
    return session


@pytest.mark.asyncio
async def test_poll_live_match_job_mid_series_update():
    """
    Tests that a mid-series update is correctly announced when the score
    changes.
    """
    # Arrange
    mock_match = Match(
        id=1,
        pandascore_id=123,
        game="cs2",
        team1="Team A",
        team2="Team B",
        team1_id=100,
        team2_id=200,
        best_of=5,
        scheduled_time=datetime.now(timezone.utc) - timedelta(hours=1),
        last_announced_score="0-0",
        contest=Contest(pandascore_league_id=1),
    )
    mock_match_data = {
        "id": 123,
        "status": "running",
        "results": [
            {"team_id": 100, "score": 1},
            {"team_id": 200, "score": 0},
        ],
        "winner_id": None,
    }
    mock_session = _mock_poll_session(with_commit=True)

    with patch(
        "src.pandascore_polling.get_async_session"
    ) as mock_get_async_session, patch(
        "src.pandascore_polling.crud.get_match_with_result_by_id",
        new_callable=AsyncMock,
        return_value=mock_match,
    ) as mock_get_match, patch(
        "src.pandascore_polling_core.pandascore_client.fetch_match_by_id",
        new_callable=AsyncMock,
        return_value=mock_match_data,
    ) as mock_get_match_data, patch(
        "src.notifications.send_mid_series_update", new_callable=AsyncMock
    ) as mock_send_update, patch(
        "src.notifications.send_result_notification",
        new_callable=AsyncMock,
    ) as mock_send_result, patch(
        "src.scheduler_instance.scheduler.remove_job"
    ) as mock_remove_job:

        mock_get_async_session.return_value.__aenter__.return_value = (
            mock_session
        )

        # Act
        await poll_live_match_job(match_db_id=1)

        # Assert
        mock_get_match.assert_awaited_once_with(mock_session, 1)
        mock_get_match_data.assert_awaited_once_with(123, game="cs2")
        mock_send_update.assert_awaited_once()
        mock_send_result.assert_not_called()
        mock_remove_job.assert_not_called()
        assert mock_match.last_announced_score == "1-0"
        mock_session.add.assert_called_once_with(mock_match)
        mock_session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_poll_live_match_job_final_result():
    mock_match = Match(
        id=2,
        pandascore_id=456,
        game="cs2",
        team1="Team A",
        team2="Team B",
        team1_id=100,
        team2_id=200,
        best_of=3,
        scheduled_time=datetime.now(timezone.utc) - timedelta(hours=1),
        last_announced_score="1-0",
        contest=Contest(pandascore_league_id=1),
    )
    mock_match_data = {
        "id": 456,
        "status": "finished",
        "results": [
            {"team_id": 100, "score": 2},
            {"team_id": 200, "score": 0},
        ],
        "winner_id": 100,
    }
    mock_session = _mock_poll_session(with_commit=True)
    mock_session.get = AsyncMock(return_value=mock_match)

    with patch(
        "src.pandascore_polling.get_async_session"
    ) as mock_get_async_session, patch(
        "src.db.get_async_session"
    ) as mock_db_get_async_session, patch(
        "src.pandascore_polling.crud.get_match_with_result_by_id",
        new_callable=AsyncMock,
        return_value=mock_match,
    ) as mock_get_match, patch(
        "src.pandascore_polling_core.pandascore_client.fetch_match_by_id",
        new_callable=AsyncMock,
        return_value=mock_match_data,
    ) as mock_get_match_data, patch(
        "src.notifications.send_mid_series_update", new_callable=AsyncMock
    ) as mock_send_update, patch(
        "src.notifications.send_result_notification",
        new_callable=AsyncMock,
    ) as mock_send_result, patch(
        "src.scheduler_instance.scheduler.remove_job"
    ) as mock_remove_job, patch(
        "src.match_result_utils.save_result_and_update_picks",
        new_callable=AsyncMock,
        return_value=Result(id=1, winner="Team A", score="2-0"),
    ):

        mock_get_async_session.return_value.__aenter__.return_value = (
            mock_session
        )
        mock_db_get_async_session.return_value.__aenter__.return_value = (
            mock_session
        )

        await poll_live_match_job(match_db_id=2)

        mock_get_match.assert_awaited_once_with(mock_session, 2)
        mock_get_match_data.assert_awaited_once_with(456, game="cs2")
        mock_send_update.assert_not_called()
        mock_send_result.assert_awaited_once()
        mock_remove_job.assert_called_once()
        mock_session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_poll_live_match_job_no_score_change():
    """
    Tests that no announcement is made if the score has not changed.
    """
    # Arrange
    mock_match = Match(
        id=3,
        pandascore_id=789,
        game="cs2",
        team1="Team A",
        team2="Team B",
        team1_id=100,
        team2_id=200,
        best_of=5,
        scheduled_time=datetime.now(timezone.utc) - timedelta(hours=1),
        last_announced_score="1-0",
        contest=Contest(pandascore_league_id=1),
    )
    mock_match_data = {
        "id": 789,
        "status": "running",
        "results": [
            {"team_id": 100, "score": 1},
            {"team_id": 200, "score": 0},
        ],
        "winner_id": None,
    }
    mock_session = _mock_poll_session()

    with patch(
        "src.pandascore_polling.get_async_session"
    ) as mock_get_async_session, patch(
        "src.pandascore_polling.crud.get_match_with_result_by_id",
        new_callable=AsyncMock,
        return_value=mock_match,
    ) as mock_get_match, patch(
        "src.pandascore_polling_core.pandascore_client.fetch_match_by_id",
        new_callable=AsyncMock,
        return_value=mock_match_data,
    ) as mock_get_match_data, patch(
        "src.notifications.send_mid_series_update", new_callable=AsyncMock
    ) as mock_send_update, patch(
        "src.notifications.send_result_notification",
        new_callable=AsyncMock,
    ) as mock_send_result, patch(
        "src.scheduler_instance.scheduler.remove_job"
    ) as mock_remove_job:

        mock_get_async_session.return_value.__aenter__.return_value = (
            mock_session
        )

        # Act
        await poll_live_match_job(match_db_id=3)

        # Assert
        mock_get_match.assert_awaited_once_with(mock_session, 3)
        mock_get_match_data.assert_awaited_once_with(789, game="cs2")
        mock_send_update.assert_not_called()
        mock_send_result.assert_not_called()
        mock_remove_job.assert_not_called()
        mock_session.add.assert_not_called()


@pytest.mark.asyncio
async def test_poll_live_match_job_already_has_result():
    """
    Tests that the job unschedules itself if the match already has a result.
    """
    # Arrange
    mock_match = Match(
        id=4,
        pandascore_id=101,
        team1="Team A",
        team2="Team B",
        best_of=3,
        scheduled_time=datetime.now(timezone.utc) - timedelta(hours=1),
        result=Result(winner="Team A", score="2-0"),
        contest=Contest(pandascore_league_id=1),
    )
    mock_session = MagicMock()
    mock_session.exec = AsyncMock()
    # Return a result so it unschedules
    mock_session.exec.return_value.first.return_value = Result(
        id=1, winner="Team A", score="2-0"
    )

    with patch(
        "src.pandascore_polling.get_async_session"
    ) as mock_get_async_session, patch(
        "src.pandascore_polling.crud.get_match_with_result_by_id",
        new_callable=AsyncMock,
        return_value=mock_match,
    ) as mock_get_match, patch(
        "src.pandascore_polling_core.pandascore_client.fetch_match_by_id",
        new_callable=AsyncMock,
    ) as mock_get_match_data, patch(
        "src.scheduler_instance.scheduler.remove_job"
    ) as mock_remove_job:

        mock_get_async_session.return_value.__aenter__.return_value = (
            mock_session
        )

        # Act
        await poll_live_match_job(match_db_id=4)

        # Assert
        mock_get_match.assert_awaited_once_with(mock_session, 4)
        mock_get_match_data.assert_not_called()
        mock_remove_job.assert_called_once()
