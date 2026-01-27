"""
Parsing helpers for PandaScore payloads.

This module is now a facade for `src.parsers.base.PandaScoreParser`.
"""

from src.parsers.base import PandaScoreParser

# Default parser instance for backward compatibility functions
_parser = PandaScoreParser()

# Backward compatibility aliases
_parse_pandascore_date = _parser.parse_date
_extract_team_data = _parser.extract_team_data
_extract_contest_data = _parser.extract_contest_data
_extract_match_data = _parser.extract_match_data
_extract_winner_and_scores = _parser.extract_winner_and_scores
