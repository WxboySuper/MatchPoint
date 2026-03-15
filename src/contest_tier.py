from typing import Any, Optional

TIER_DISPLAY = {
    "S": "S-tier",
    "A": "A-tier",
    "B": "B-tier",
    "C": "C-tier",
    "D": "D-tier",
}


def _normalize_tier_prefix(value: str, prefix: str) -> Optional[str]:
    if not value.startswith(prefix):
        return None
    return _normalize_tier_key(value.removeprefix(prefix).lstrip("-:"))


def _normalize_tier_key(value: str) -> Optional[str]:
    if value in TIER_DISPLAY:
        return value
    return None


def normalize_contest_tier(raw_tier: Any) -> Optional[str]:
    if raw_tier is None:
        return None

    value = str(raw_tier).strip().upper()
    if not value:
        return None

    direct = value.replace(" ", "")
    normalized = _normalize_tier_key(direct)
    if normalized:
        return normalized

    if direct.endswith("-TIER"):
        return _normalize_tier_key(direct.removesuffix("-TIER"))

    normalized = _normalize_tier_prefix(direct, "TIER")
    if normalized:
        return normalized

    return None


def extract_contest_tier(match_data: dict[str, Any]) -> Optional[str]:
    league = match_data.get("league") or {}
    serie = match_data.get("serie") or {}
    tournament = match_data.get("tournament") or {}

    candidates = (
        match_data.get("tier"),
        league.get("tier"),
        serie.get("tier"),
        tournament.get("tier"),
        match_data.get("tournament_tier"),
        league.get("tournament_tier"),
        serie.get("tournament_tier"),
        tournament.get("tournament_tier"),
    )
    for candidate in candidates:
        normalized = normalize_contest_tier(candidate)
        if normalized:
            return normalized
    return None


def display_contest_tier(raw_tier: Optional[str]) -> Optional[str]:
    normalized = normalize_contest_tier(raw_tier)
    if normalized is None:
        return None
    return TIER_DISPLAY[normalized]
