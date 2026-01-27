"""
League of Legends Parser.
"""

from src.parsers.base import PandaScoreParser


class LoLParser(PandaScoreParser):
    """Parser for League of Legends matches."""

    def __init__(self):
        # Game slug removed from base for now as per feedback
        pass
