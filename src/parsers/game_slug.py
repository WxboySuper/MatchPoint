from typing import Any, Optional


def _clean_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip().lower()
    return ""


def normalize_game_slug(
    slug: Any,
    title: Any = None,
) -> Optional[str]:
    raw_slug = _clean_text(slug)
    raw_title = _clean_text(title)

    if raw_slug in {"lol", "league-of-legends", "leagueoflegends"}:
        return "lol"

    if raw_slug in {"cs2", "csgo", "counterstrike", "counter-strike"}:
        return "cs2"

    if "league of legends" in raw_title:
        return "lol"

    if "counter-strike 2" in raw_title or raw_title == "cs2":
        return "cs2"

    return raw_slug or None


def game_query_slugs(game: str) -> tuple[str, ...]:
    normalized = normalize_game_slug(game) or _clean_text(game)
    if normalized == "lol":
        return ("lol", "league-of-legends", "leagueoflegends")
    if normalized == "cs2":
        return ("cs2", "csgo", "counterstrike", "counter-strike")
    return (normalized,) if normalized else tuple()


def game_display_name(game: Optional[str]) -> str:
    normalized = normalize_game_slug(game)
    if normalized == "lol":
        return "LoL"
    if normalized == "cs2":
        return "CS2"
    if not game:
        return "Unknown"
    return game.upper()
