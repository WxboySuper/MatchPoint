"""
Unit tests for PandaScore client and sync logic.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestPandaScoreClient:
    """Tests for PandaScoreClient class."""

    @pytest.fixture
    def mock_response(self):
        """Create a mock aiohttp response."""
        response = MagicMock()
        response.status = 200
        response.headers = {"X-Rate-Limit-Remaining": "999"}
        return response

    @pytest.fixture
    def sample_match_data(self):
        """Sample PandaScore match response."""
        return {
            "id": 123456,
            "name": "Team A vs Team B",
            "status": "not_started",
            "scheduled_at": "2024-03-15T10:00:00Z",
            "number_of_games": 3,
            "league": {
                "id": 1,
                "name": "LCS",
            },
            "serie": {
                "id": 10,
                "name": "Spring 2024",
                "full_name": "Spring Split 2024",
            },
            "opponents": [
                {
                    "opponent": {
                        "id": 100,
                        "name": "Team A",
                        "acronym": "TA",
                        "image_url": "https://example.com/team_a.png",
                    }
                },
                {
                    "opponent": {
                        "id": 200,
                        "name": "Team B",
                        "acronym": "TB",
                        "image_url": "https://example.com/team_b.png",
                    }
                },
            ],
            "results": [],
            "winner_id": None,
        }

    @pytest.fixture
    def sample_finished_match(self, sample_match_data):
        """Sample finished match with winner."""
        match = sample_match_data.copy()
        match["status"] = "finished"
        match["winner_id"] = 100
        match["results"] = [
            {"team_id": 100, "score": 2},
            {"team_id": 200, "score": 1},
        ]
        return match


class TestPandaScoreParser:
    """Tests for PandaScoreParser class."""

    @pytest.fixture
    def parser(self):
        from src.parsers.base import PandaScoreParser

        return PandaScoreParser()

    def test_parse_date_valid(self, parser):
        """Test parsing a valid ISO 8601 date."""
        result = parser.parse_date("2024-03-15T10:00:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 15
        assert result.hour == 10
        assert result.tzinfo is not None

    def test_parse_date_none(self, parser):
        """Test parsing None returns None."""
        result = parser.parse_date(None)
        assert result is None

    def test_parse_date_invalid(self, parser):
        """Test parsing invalid date returns None."""
        result = parser.parse_date("not-a-date")
        assert result is None

    def test_extract_team_data_valid(self, parser):
        """Test extracting team data from opponent object."""
        opponent = {
            "opponent": {
                "id": 100,
                "name": "Team A",
                "acronym": "TA",
                "image_url": "https://example.com/team.png",
            }
        }

        result = parser.extract_team_data(opponent)
        assert result is not None
        assert result["pandascore_id"] == 100
        assert result["name"] == "Team A"
        assert result["acronym"] == "TA"

    def test_extract_team_data_missing_opponent(self, parser):
        """Test extracting team data with missing opponent key."""
        result = parser.extract_team_data({})
        assert result is None

    def test_extract_contest_data(self, parser):
        """Test extracting contest data from match."""
        match_data = {
            "league": {"id": 1, "name": "LCS", "image_url": "http://img"},
            "serie": {
                "id": 10,
                "name": "Spring",
                "full_name": "Spring Split 2024",
            },
            "scheduled_at": "2024-03-15T10:00:00Z",
        }

        result = parser.extract_contest_data(match_data)
        assert result["pandascore_league_id"] == 1
        assert result["pandascore_serie_id"] == 10
        assert "LCS" in result["name"]
        assert "Spring" in result["name"]
        assert result["image_url"] == "http://img"

    def test_extract_match_data_valid(self, parser):
        """Test extracting match data from PandaScore match object."""
        match_data = {
            "id": 123456,
            "scheduled_at": "2024-03-15T10:00:00Z",
            "number_of_games": 3,
            "status": "not_started",
            "opponents": [
                {"opponent": {"id": 100, "name": "Team A", "acronym": "TA"}},
                {"opponent": {"id": 200, "name": "Team B", "acronym": "TB"}},
            ],
        }

        result = parser.extract_match_data(match_data, contest_id=1)
        assert result is not None
        assert result["pandascore_id"] == 123456
        assert result["team1"] == "Team A"
        assert result["team2"] == "Team B"
        assert result["team1_id"] == 100
        assert result["team2_id"] == 200
        assert result["best_of"] == 3

    def test_extract_match_data_missing_opponents(self, parser):
        """Test extracting match data with fewer than 2 opponents."""
        match_data = {
            "id": 123456,
            "scheduled_at": "2024-03-15T10:00:00Z",
            "opponents": [{"opponent": {"id": 100, "name": "Team A"}}],
        }

        result = parser.extract_match_data(match_data, contest_id=1)
        assert result is not None
        assert result["pandascore_id"] == 123456
        assert result["team1"] == "Team A"
        assert result["team2"] == "TBD"


class TestPandaScorePollingHelpers:
    """Tests for polling helper functions."""

    def _make_mock_match(
        self, team1_id=100, team2_id=200, team1="Team A", team2="Team B"
    ):
        m = MagicMock()
        m.team1_id = team1_id
        m.team2_id = team2_id
        m.team1 = team1
        m.team2 = team2
        return m

    def test_extract_scores_from_pandascore(self):
        """Test extracting scores from match data."""
        from src.pandascore_polling_core import _extract_scores_from_pandascore

        match_data = {
            "results": [
                {"team_id": 100, "score": 2},
                {"team_id": 200, "score": 1},
            ]
        }

        match = self._make_mock_match()

        team1_score, team2_score = _extract_scores_from_pandascore(
            match_data, match
        )
        assert team1_score == 2
        assert team2_score == 1

    def test_determine_winner_from_pandascore(self):
        """Test determining winner from match data."""
        from src.pandascore_polling_core import (
            _determine_winner_from_pandascore,
        )

        match_data_winner = {
            "winner_id": 100,
            "status": "finished",
        }

        match = self._make_mock_match()

        winner = _determine_winner_from_pandascore(
            match_data_winner, match, 2, 1
        )
        assert winner == "Team A"

    @pytest.mark.parametrize(
        "match_data,team1_score,team2_score,expected",
        [
            ({"winner_id": 100, "status": "finished"}, 2, 1, "Team A"),
            ({"winner_id": None, "status": "running"}, 1, 1, None),
        ],
    )
    def test_determine_winner_parametrized(
        self, match_data, team1_score, team2_score, expected
    ):
        """Parametrized test for winner detection (finished vs running)."""
        from src.pandascore_polling_core import (
            _determine_winner_from_pandascore,
        )

        match = self._make_mock_match()

        winner = _determine_winner_from_pandascore(
            match_data, match, team1_score, team2_score
        )
        assert winner == expected


class TestPandaScoreSyncIntegration:
    """Integration tests for PandaScore sync (mocked API)."""

    @pytest.mark.asyncio
    async def test_perform_pandascore_sync_empty_response(self):
        """Test sync with no matches returned."""
        from src.pandascore_sync import perform_pandascore_sync

        with patch(
            "src.pandascore_sync.pandascore_client.fetch_matches",
            new_callable=AsyncMock,
            return_value=[],
        ), patch(
            "src.pandascore_sync.pandascore_client.fetch_running_matches",
            new_callable=AsyncMock,
            return_value=[],
        ):
            result = await perform_pandascore_sync()
            assert result is not None
            assert result["matches"] == 0
            assert result["contests"] == 0
            assert result["teams"] == 0

    @pytest.mark.asyncio
    async def test_perform_pandascore_sync_api_error(self):
        """Test sync handles API errors gracefully."""
        from src.pandascore_sync import perform_pandascore_sync

        with patch(
            "src.pandascore_sync.pandascore_client.fetch_matches",
            new_callable=AsyncMock,
            side_effect=Exception("API Error"),
        ):
            result = await perform_pandascore_sync()
            assert result is None


class TestPandaScoreClientRateLimiting:
    """Tests for rate limiting behavior."""

    @pytest.mark.asyncio
    async def test_rate_limit_tracking(self):
        """Test that rate limit tracking is initialized."""
        from src.pandascore_client import PandaScoreClient

        client = PandaScoreClient()
        assert client._request_count == 0
        assert client._window_start is not None
