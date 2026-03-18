from src.parsers.cs2 import CS2Parser
from src.parsers.game_slug import normalize_game_slug


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
