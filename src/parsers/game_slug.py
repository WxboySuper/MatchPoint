from typing import Optional


def _normalize_lol_slug(raw_slug: str, raw_title: str) -> Optional[str]:
    if raw_slug in {"lol", "league-of-legends", "leagueoflegends"}:
        return "lol"
    if "league of legends" in raw_title:
        return "lol"
    return None


def _normalize_cs2_slug(raw_slug: str, raw_title: str) -> Optional[str]:
    if raw_slug in {"cs2", "csgo", "counterstrike", "counter-strike"}:
        return "cs2"
    if "counter-strike 2" in raw_title or raw_title == "cs2":
        return "cs2"
    return None


def normalize_game_slug(
    slug: Optional[str],
    title: Optional[str] = None,
) -> Optional[str]:
    raw_slug = (slug or "").strip().lower()
    raw_title = (title or "").strip().lower()

    normalized_lol = _normalize_lol_slug(raw_slug, raw_title)
    if normalized_lol:
        return normalized_lol

    normalized_cs2 = _normalize_cs2_slug(raw_slug, raw_title)
    if normalized_cs2:
        return normalized_cs2

    return raw_slug or None


def game_query_slugs(game: str) -> tuple[str, ...]:
    normalized = normalize_game_slug(game) or game.strip().lower()
    if normalized == "lol":
        return ("lol", "league-of-legends", "leagueoflegends")
    if normalized == "cs2":
        return ("cs2", "csgo", "counterstrike", "counter-strike")
    return (normalized,)


def game_display_name(game: Optional[str]) -> str:
    normalized = normalize_game_slug(game)
    if normalized == "lol":
        return "LoL"
    if normalized == "cs2":
        return "CS2"
    if not game:
        return "Unknown"
    return game.upper()
