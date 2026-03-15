"""
Unit tests for PandaScore client and sync logic.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlmodel import SQLModel, Session, create_engine

from src.models import Contest, Match


def _make_mock_match(
    team1_id=100, team2_id=200, team1="Team A", team2="Team B"
):
    match = MagicMock()
    match.team1_id = team1_id
    match.team2_id = team2_id
    match.team1 = team1
    match.team2 = team2
    return match


def _assert_match_data_game(
    parser, match_data: dict, expected_game: str
) -> None:
    result = parser.extract_match_data(match_data, contest_id=1)
    assert result is not None
    assert result["game"] == expected_game


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


class TestLoLParser:
    """Tests for LoLParser class."""

    @pytest.fixture
    def parser(self):
        from src.parsers.lol import LoLParser

        return LoLParser()

    @staticmethod
    def test_parse_date_valid(parser):
        """Test parsing a valid ISO 8601 date."""
        result = parser.parse_date("2024-03-15T10:00:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 15
        assert result.hour == 10
        assert result.tzinfo is not None

    @staticmethod
    def test_parse_date_none(parser):
        """Test parsing None returns None."""
        result = parser.parse_date(None)
        assert result is None

    @staticmethod
    def test_parse_date_invalid(parser):
        """Test parsing invalid date returns None."""
        result = parser.parse_date("not-a-date")
        assert result is None

    @staticmethod
    def test_extract_team_data_valid(parser):
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

    @staticmethod
    def test_extract_team_data_missing_opponent(parser):
        """Test extracting team data with missing opponent key."""
        result = parser.extract_team_data({})
        assert result is None

    @staticmethod
    def test_extract_match_data_valid(parser):
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
        assert result["game"] == "lol"
        assert result["team1"] == "Team A"
        assert result["team2"] == "Team B"
        assert result["team1_id"] == 100
        assert result["team2_id"] == 200
        assert result["best_of"] == 3

    @staticmethod
    def test_extract_match_data_normalizes_lol_payload_slug(
        parser,
    ) -> None:
        match_data = {
            "id": 123456,
            "scheduled_at": "2024-03-15T10:00:00Z",
            "opponents": [],
            "videogame": {"slug": "league-of-legends"},
            "videogame_title": "League of Legends",
        }
        _assert_match_data_game(parser, match_data, "lol")

    @staticmethod
    def test_extract_match_data_uses_payload_game_slug(parser):
        match_data = {
            "id": 123456,
            "scheduled_at": "2024-03-15T10:00:00Z",
            "opponents": [],
            "videogame": {"slug": "lol"},
        }
        _assert_match_data_game(parser, match_data, "lol")

    @staticmethod
    def test_extract_match_data_missing_opponents(parser):
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

    @staticmethod
    def test_extract_scores_from_pandascore():
        """Test extracting scores from match data."""
        from src.pandascore_polling_core import _extract_scores_from_pandascore

        match_data = {
            "results": [
                {"team_id": 100, "score": 2},
                {"team_id": 200, "score": 1},
            ]
        }

        match = _make_mock_match()

        team1_score, team2_score = _extract_scores_from_pandascore(
            match_data, match
        )
        assert team1_score == 2
        assert team2_score == 1

    @staticmethod
    def test_determine_winner_from_pandascore():
        """Test determining winner from match data."""
        from src.pandascore_polling_core import (
            _determine_winner_from_pandascore,
        )

        match_data_winner = {
            "winner_id": 100,
            "status": "finished",
        }

        match = _make_mock_match()

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
    @staticmethod
    def test_determine_winner_parametrized(
        match_data, team1_score, team2_score, expected
    ):
        """Parametrized test for winner detection (finished vs running)."""
        from src.pandascore_polling_core import (
            _determine_winner_from_pandascore,
        )

        match = _make_mock_match()

        winner = _determine_winner_from_pandascore(
            match_data, match, team1_score, team2_score
        )
        assert winner == expected


class TestCS2Parser:
    @pytest.fixture
    def parser(self):
        from src.parsers.cs2 import CS2Parser

        return CS2Parser()

    @staticmethod
    def test_extract_match_data_sets_cs2_game(parser):
        match_data = {
            "id": 987654,
            "scheduled_at": "2024-03-15T10:00:00Z",
            "number_of_games": 3,
            "status": "not_started",
            "opponents": [
                {"opponent": {"id": 100, "name": "Team A"}},
                {"opponent": {"id": 200, "name": "Team B"}},
            ],
        }

        result = parser.extract_match_data(match_data, contest_id=1)
        assert result is not None
        assert result["game"] == "cs2"

    @staticmethod
    def test_extract_match_data_normalizes_counterstrike_payload(parser):
        match_data = {
            "id": 987654,
            "scheduled_at": "2024-03-15T10:00:00Z",
            "status": "not_started",
            "opponents": [],
            "videogame": {"slug": "counterstrike"},
            "videogame_title": "Counter-Strike 2",
        }
        _assert_match_data_game(parser, match_data, "cs2")


class TestPandaScoreSyncIntegration:
    """Integration tests for PandaScore sync (mocked API)."""

    @staticmethod
    @pytest.mark.asyncio
    async def test_perform_pandascore_sync_empty_response():
        """Test sync with no matches returned."""
        from src.pandascore_sync import perform_pandascore_sync

        with patch(
            "src.pandascore_sync.pandascore_client.fetch_matches",
            new_callable=AsyncMock,
            return_value=[],
        ), patch(
            "src.pandascore_sync._reconcile_finished_matches_for_game",
            new_callable=AsyncMock,
        ):
            result = await perform_pandascore_sync()
            assert result is not None
            assert result["matches"] == 0
            assert result["contests"] == 0
            assert result["teams"] == 0

    @staticmethod
    @pytest.mark.asyncio
    async def test_perform_pandascore_sync_api_error():
        """Test sync handles API errors gracefully."""
        from src.pandascore_sync import perform_pandascore_sync

        with patch(
            "src.pandascore_sync.pandascore_client.fetch_matches",
            new_callable=AsyncMock,
            side_effect=Exception("API Error"),
        ), patch(
            "src.pandascore_sync._reconcile_finished_matches_for_game",
            new_callable=AsyncMock,
        ):
            result = await perform_pandascore_sync()
            assert result is None

    @staticmethod
    @pytest.mark.asyncio
    async def test_reconcile_finished_matches_handles_legacy_lol_rows(
        async_session_for_engine,
    ) -> None:
        from src import pandascore_sync

        engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(engine)

        with Session(engine) as session:
            contest = Contest(
                pandascore_league_id=1,
                pandascore_serie_id=2,
                name="LCK Spring",
                start_date=datetime.now(timezone.utc),
                end_date=datetime.now(timezone.utc),
            )
            session.add(contest)
            session.commit()
            session.refresh(contest)

            session.add(
                Match(
                    contest_id=contest.id,
                    pandascore_id=77,
                    team1="T1",
                    team2="GEN",
                    status="live",
                    game="league-of-legends",
                    scheduled_time=datetime.now(timezone.utc),
                )
            )
            session.commit()

        with patch.object(
            pandascore_sync,
            "get_async_session",
            return_value=async_session_for_engine(engine),
        ), patch.object(
            pandascore_sync,
            "_fetch_pandascore_match",
            new_callable=AsyncMock,
            return_value={"status": "finished"},
        ) as mock_fetch, patch.object(
            pandascore_sync,
            "fetch_and_update_match_result",
            new_callable=AsyncMock,
        ) as mock_update:
            await pandascore_sync._reconcile_finished_matches_for_game("lol")

        mock_fetch.assert_awaited_once_with(77, "lol")
        mock_update.assert_awaited_once_with(77, game_slug="lol")

    @staticmethod
    @pytest.mark.asyncio
    async def test_perform_pandascore_sync_fetches_all_configured_games():
        from src.pandascore_sync import perform_pandascore_sync

        async def _fetch_matches(kind, options=None, game="lol"):
            _ = options
            if kind == "running":
                return []
            if kind == "recent_past":
                return []
            return [
                {
                    "id": 100 if game == "lol" else 200,
                    "scheduled_at": "2024-03-15T10:00:00Z",
                    "number_of_games": 3,
                    "status": "not_started",
                    "league": {"id": 1, "name": "League"},
                    "serie": {
                        "id": 10,
                        "name": "Split",
                        "full_name": "Split",
                    },
                    "opponents": [
                        {
                            "opponent": {
                                "id": 1,
                                "name": f"{game} A",
                            }
                        },
                        {
                            "opponent": {
                                "id": 2,
                                "name": f"{game} B",
                            }
                        },
                    ],
                    "videogame": {"slug": game},
                }
            ]

        with patch(
            "src.pandascore_sync.pandascore_client.fetch_matches",
            new_callable=AsyncMock,
            side_effect=_fetch_matches,
        ) as mock_fetch, patch(
            "src.pandascore_sync._run_post_sync_actions",
            new_callable=AsyncMock,
        ), patch(
            "src.pandascore_sync._reconcile_finished_matches_for_game",
            new_callable=AsyncMock,
        ), patch(
            "src.config.DEFAULT_GAMES",
            ["lol", "cs2"],
        ):
            result = await perform_pandascore_sync()

        requested_games = {
            call.kwargs["game"] for call in mock_fetch.await_args_list
        }
        assert requested_games == {"lol", "cs2"}
        assert result is not None

    @staticmethod
    @pytest.mark.asyncio
    async def test_perform_pandascore_sync_includes_guild_enabled_games():
        from src.pandascore_sync import perform_pandascore_sync

        class _ResultWrapper:
            def __init__(self, rows):
                self._rows = rows

            def all(self):
                return self._rows

        class _AsyncSession:
            @staticmethod
            async def exec(stmt):
                _ = stmt
                return _ResultWrapper(["cs2"])

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                _ = (exc_type, exc, tb)
                return False

        async def _fetch_matches(kind, options=None, game="lol"):
            _ = (kind, options)
            return []

        with patch(
            "src.pandascore_sync.get_async_session",
            return_value=_AsyncSession(),
        ), patch(
            "src.pandascore_sync.pandascore_client.fetch_matches",
            new_callable=AsyncMock,
            side_effect=_fetch_matches,
        ) as mock_fetch, patch(
            "src.pandascore_sync._run_post_sync_actions",
            new_callable=AsyncMock,
        ), patch(
            "src.pandascore_sync._reconcile_finished_matches_for_game",
            new_callable=AsyncMock,
        ), patch(
            "src.config.DEFAULT_GAMES",
            ["lol"],
        ):
            await perform_pandascore_sync()

        requested_games = {
            call.kwargs["game"] for call in mock_fetch.await_args_list
        }
        assert requested_games == {"lol", "cs2"}


class TestPandaScoreClientRateLimiting:
    """Tests for rate limiting behavior."""

    @staticmethod
    @pytest.mark.asyncio
    async def test_fetch_matches_maps_cs2_to_csgo_route():
        from src.pandascore_client import PandaScoreClient

        client = PandaScoreClient(api_key="test-key")

        with patch.object(
            client,
            "_fetch_matches",
            new_callable=AsyncMock,
            return_value=[],
        ) as mock_fetch:
            await client.fetch_matches("upcoming", game="cs2")

        endpoint = mock_fetch.await_args.args[0]
        params = mock_fetch.await_args.args[1]
        assert endpoint == "/csgo/matches/upcoming"
        assert "filter[videogame_title]" not in params

    @staticmethod
    @pytest.mark.asyncio
    async def test_fetch_match_by_id_maps_cs2_to_csgo_route():
        from src.pandascore_client import PandaScoreClient

        client = PandaScoreClient(api_key="test-key")

        with patch.object(
            client,
            "_make_request",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_request:
            await client.fetch_match_by_id(42, game="cs2")

        assert mock_request.await_args.args[0] == "/csgo/matches/42"
        assert mock_request.await_args.kwargs.get("params") is None

    @staticmethod
    @pytest.mark.asyncio
    async def test_rate_limit_tracking():
        """Test that rate limit tracking is initialized."""
        from src.pandascore_client import PandaScoreClient

        client = PandaScoreClient()
        assert client._request_count == 0
        assert client._window_start is not None
