"""
Base PandaScore Parser.

Contains generic parsing logic for PandaScore API payloads.
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class PandaScoreParser:
    """Base parser for PandaScore API responses."""

    @staticmethod
    def parse_date(date_str: Optional[str]) -> Optional[datetime]:
        """Parse PandaScore ISO 8601 date string."""
        if not date_str:
            return None
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            logger.warning("Failed to parse date: %s", date_str)
            return None

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

        opponents = match_data.get("opponents", [])

        # Extract team info, defaulting to TBD if missing
        team1_info = (
            opponents[0].get("opponent", {}) if len(opponents) > 0 else {}
        )
        team2_info = (
            opponents[1].get("opponent", {}) if len(opponents) > 1 else {}
        )

        team1_name = (
            team1_info.get("name") or team1_info.get("acronym") or "TBD"
        )
        team2_name = (
            team2_info.get("name") or team2_info.get("acronym") or "TBD"
        )

        return {
            "pandascore_id": pandascore_id,
            "contest_id": contest_id,
            "team1": team1_name,
            "team2": team2_name,
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
