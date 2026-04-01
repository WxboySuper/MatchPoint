from typing import Any, Optional


def _clean_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, dict):
        for key in ("name", "title", "full_name", "slug"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                return candidate.strip().lower()
    return ""


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


def _normalize_valorant_slug(raw_slug: str, raw_title: str) -> Optional[str]:
    if raw_slug in {"valorant", "val", "vct"}:
        return "valorant"
    if "valorant" in raw_title:
        return "valorant"
    return None


def normalize_game_slug(
    slug: Any,
    title: Any = None,
) -> Optional[str]:
    raw_slug = _clean_text(slug)
    raw_title = _clean_text(title)

    normalized_lol = _normalize_lol_slug(raw_slug, raw_title)
    if normalized_lol:
        return normalized_lol

    normalized_cs2 = _normalize_cs2_slug(raw_slug, raw_title)
    if normalized_cs2:
        return normalized_cs2

    normalized_valorant = _normalize_valorant_slug(raw_slug, raw_title)
    if normalized_valorant:
        return normalized_valorant

    return raw_slug or None


def game_query_slugs(game: str) -> tuple[str, ...]:
    normalized = normalize_game_slug(game)
    if normalized == "lol":
        return ("lol", "league-of-legends", "leagueoflegends")
    if normalized == "cs2":
        return ("cs2", "csgo", "counterstrike", "counter-strike")
    if normalized == "valorant":
        return ("valorant", "val", "vct")
    return (normalized,) if normalized else ()


def game_display_name(game: Optional[str]) -> str:
    normalized = normalize_game_slug(game)
    if normalized == "lol":
        return "LoL"
    if normalized == "cs2":
        return "CS2"
    if normalized == "valorant":
        return "Valorant"
    if not game:
        return "Unknown"
    return str(game).upper()
