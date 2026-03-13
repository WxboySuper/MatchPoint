"""
League of Legends Parser.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

from src.parsers.base import PandaScoreParser

logger = logging.getLogger(__name__)


def _extract_opponent_info(
    match_data: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    opponents = match_data.get("opponents", [])
    team_infos = [opponent.get("opponent", {}) for opponent in opponents[:2]]
    while len(team_infos) < 2:
        team_infos.append({})
    return team_infos[0], team_infos[1]


def _team_display_name(team_info: Dict[str, Any]) -> str:
    return team_info.get("name") or team_info.get("acronym") or "TBD"


def _match_game_slug(match_data: Dict[str, Any]) -> str:
    return (match_data.get("videogame") or {}).get("slug") or "lol"


class LoLParser(PandaScoreParser):
    """Parser for League of Legends matches."""

    @staticmethod
    def extract_team_data(
        opponent: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Extract team data from opponent object."""
        team_info = opponent.get("opponent")
        if not team_info:
            return None

        return {
            "pandascore_id": team_info.get("id"),
            "name": team_info.get("name"),
            "acronym": team_info.get("acronym"),
            "image_url": team_info.get("image_url"),
        }

    @staticmethod
    def extract_contest_data(match_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract contest (league/series) data from match object."""
        league = match_data.get("league", {})
        serie = match_data.get("serie", {})

        league_name = league.get("name", "Unknown League")
        serie_name = serie.get("full_name") or serie.get("name", "")
        contest_name = f"{league_name} {serie_name}".strip()

        scheduled_at = PandaScoreParser.parse_date(
            match_data.get("scheduled_at")
        )
        now = datetime.now(timezone.utc)

        return {
            "pandascore_league_id": league.get("id"),
            "pandascore_serie_id": serie.get("id"),
            "name": contest_name,
            "start_date": scheduled_at or now,
            "end_date": scheduled_at or now,
            "image_url": league.get("image_url"),
        }

    @staticmethod
    def extract_match_data(
        match_data: Dict[str, Any], contest_id: int
    ) -> Optional[Dict[str, Any]]:
        """Extract match data from PandaScore match object."""
        pandascore_id = match_data.get("id")
        scheduled_at = PandaScoreParser.parse_date(
            match_data.get("scheduled_at")
        )

        if not pandascore_id or not scheduled_at:
            logger.warning("Match missing id or scheduled_at: %s", match_data)
            return None

        team1_info, team2_info = _extract_opponent_info(match_data)

        return {
            "pandascore_id": pandascore_id,
            "contest_id": contest_id,
            "game": _match_game_slug(match_data),
            "team1": _team_display_name(team1_info),
            "team2": _team_display_name(team2_info),
            "team1_id": team1_info.get("id"),
            "team2_id": team2_info.get("id"),
            "best_of": match_data.get("number_of_games"),
            "status": match_data.get("status", "not_started"),
            "scheduled_time": scheduled_at,
        }

    @staticmethod
    def extract_winner_and_scores(
        match_data: Dict[str, Any], match: Any, winner_id: Any
    ) -> Tuple[Optional[str], int, int]:
        """Extract winner name and scores from match result."""
        results = match_data.get("results") or []
        scores = {
            r.get("team_id"): (r.get("score") or 0)
            for r in results
            if r.get("team_id") is not None
        }

        team1_score = scores.get(match.team1_id, 0)
        team2_score = scores.get(match.team2_id, 0)

        id_to_name = {match.team1_id: match.team1, match.team2_id: match.team2}
        winner_name = id_to_name.get(winner_id)

        return winner_name, team1_score, team2_score
