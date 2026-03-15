from typing import Any, Optional

TIER_DISPLAY = {
    "S": "S-tier",
    "A": "A-tier",
    "B": "B-tier",
    "C": "C-tier",
    "D": "D-tier",
}


def normalize_contest_tier(raw_tier: Any) -> Optional[str]:
    if raw_tier is None:
        return None

    value = str(raw_tier).strip().upper()
    if not value:
        return None

    direct = value.replace(" ", "")
    if direct in TIER_DISPLAY:
        return direct

    if direct.endswith("-TIER"):
        base = direct.removesuffix("-TIER")
        if base in TIER_DISPLAY:
            return base

    if direct.startswith("TIER"):
        base = direct.removeprefix("TIER").lstrip("-:")
        if base in TIER_DISPLAY:
            return base

    return None


def extract_contest_tier(match_data: dict[str, Any]) -> Optional[str]:
    league = match_data.get("league") or {}
    serie = match_data.get("serie") or {}

    candidates = (
        match_data.get("tier"),
        league.get("tier"),
        serie.get("tier"),
        match_data.get("tournament_tier"),
        league.get("tournament_tier"),
        serie.get("tournament_tier"),
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
