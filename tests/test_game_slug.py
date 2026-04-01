from src.parsers.cs2 import CS2Parser
from src.parsers.valorant import ValorantParser
from src.parsers.game_slug import normalize_game_slug, game_display_name, game_query_slugs


def test_normalize_game_slug_handles_dict_title() -> None:
    assert (
        normalize_game_slug(
            "league-of-legends",
            {"name": "League of Legends"},
        )
        == "lol"
    )


def test_cs2_parser_handles_dict_videogame_title() -> None:
    parser = CS2Parser()
    match_data = {
        "id": 1400257,
        "scheduled_at": "2026-03-18T12:00:00Z",
        "opponents": [],
        "videogame": {"slug": "cs2"},
        "videogame_title": {"name": "Counter-Strike 2"},
    }

    result = parser.extract_match_data(match_data, contest_id=1)
    assert result is not None
    assert result["game"] == "cs2"


def test_normalize_valorant_slug() -> None:
    assert normalize_game_slug("valorant") == "valorant"
    assert normalize_game_slug("val") == "valorant"
    assert normalize_game_slug("vct") == "valorant"
    assert normalize_game_slug("valorant", "VCT 2026") == "valorant"


def test_valorant_display_name() -> None:
    assert game_display_name("valorant") == "Valorant"
    assert game_display_name("val") == "Valorant"


def test_valorant_query_slugs() -> None:
    slugs = game_query_slugs("valorant")
    assert "valorant" in slugs
    assert "val" in slugs
    assert "vct" in slugs


def test_valorant_parser_extracts_match_data() -> None:
    parser = ValorantParser()
    match_data = {
        "id": 9999001,
        "scheduled_at": "2026-04-01T18:00:00Z",
        "opponents": [
            {"opponent": {"id": 12345, "name": "Sentinels", "acronym": "SEN", "image_url": "https://example.com/sen.png"}},
            {"opponent": {"id": 12346, "name": "Cloud9", "acronym": "C9", "image_url": "https://example.com/c9.png"}},
        ],
        "videogame": {"slug": "valorant"},
        "league": {"id": 4378, "name": "VCT Americas", "image_url": "https://example.com/vct.png"},
        "serie": {"id": 7890, "full_name": "Stage 1"},
        "number_of_games": 3,
        "status": "not_started",
    }

    result = parser.extract_match_data(match_data, contest_id=1)
    assert result is not None
    assert result["game"] == "valorant"
    assert result["team1"] == "Sentinels"
    assert result["team2"] == "Cloud9"
    assert result["best_of"] == 3


def test_valorant_parser_extracts_contest() -> None:
    parser = ValorantParser()
    match_data = {
        "league": {"id": 4378, "name": "VCT Americas", "image_url": "https://example.com/vct.png"},
        "serie": {"id": 7890, "full_name": "Stage 1"},
        "scheduled_at": "2026-04-01T18:00:00Z",
    }

    contest = parser.extract_contest_data(match_data)
    assert contest["name"] == "VCT Americas Stage 1"
    assert contest["pandascore_league_id"] == 4378


def test_valorant_parser_extracts_winner_and_scores() -> None:
    parser = ValorantParser()
    match_data = {
        "results": [
            {"team_id": 12345, "score": 2},
            {"team_id": 12346, "score": 1},
        ]
    }
    # Mock match object
    class MockMatch:
        team1_id = 12345
        team2_id = 12346
        team1 = "Sentinels"
        team2 = "Cloud9"

    winner_name, team1_score, team2_score = parser.extract_winner_and_scores(
        match_data, MockMatch(), 12345
    )
    assert winner_name == "Sentinels"
    assert team1_score == 2
    assert team2_score == 1
