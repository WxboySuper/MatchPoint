"""Parser factory for game-specific PandaScore parsers."""

from typing import Optional

from src.parsers.lol import LoLParser
from src.parsers.base import PandaScoreParser
from src.parsers.cs2 import CS2Parser


_PARSERS = {
    "lol": LoLParser,
    "cs2": CS2Parser,
}


def get_parser(game_slug: str) -> Optional[PandaScoreParser]:
    cls = _PARSERS.get(game_slug)
    if cls is None:
        return None
    return cls()
