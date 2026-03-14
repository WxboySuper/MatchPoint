import logging
from typing import Any, Iterable, List, Tuple, Optional
from sqlmodel import select
from src.models import Match, Result, Pick, Team

logger = logging.getLogger(__name__)


def _normalize_team_name(s: str) -> str:
    """
    Normalize a text string for case- and whitespace-insensitive
    comparisons.

    Parameters:
        s (str): Input string to normalize; None or falsy values
            are treated as empty.

    Returns:
        normalized (str): The input converted to lowercase with
            leading/trailing whitespace removed (empty string if
            input was falsy).
    """
    return (s or "").strip().lower()


def _get_score_delta_from_game(
    game: dict, m1: str, m2: str
) -> Tuple[int, int]:
    """
    Compute the score delta contributed by a single scoreboard
    game for a match between two teams.

    Parameters:
        game (dict): A scoreboard entry containing at least
            "Team1", "Team2", and "Winner".
        m1 (str): Normalized identifier for match.team1.
        m2 (str): Normalized identifier for match.team2.

    Returns:
        tuple: (delta_team1, delta_team2) where exactly one value
            is 1 when the game is won by one of the match teams
            and 0 otherwise. Returns (0, 0) if the game does not
            involve the two match teams or the winner is
            missing/invalid.
    """
    winner_raw = game.get("Winner")
    if winner_raw is None:
        return 0, 0
    try:
        winner_id = int(str(winner_raw).strip())
    except Exception:
        return 0, 0

    g1 = _normalize_team_name(game.get("Team1"))
    g2 = _normalize_team_name(game.get("Team2"))

    # If the game doesn't involve the two match teams, ignore it
    if {g1, g2} != {m1, m2}:
        return 0, 0

    # Determine which side (g1/g2) maps to match.team1
    # If winner_id is 1, the winner is g1; if 2, the winner is g2
    if winner_id == 1:
        return (1, 0) if g1 == m1 else (0, 1)
    if winner_id == 2:
        return (1, 0) if g2 == m1 else (0, 1)
    return 0, 0


def calculate_team_scores(
    relevant_games: Iterable[dict], match: Any
) -> Tuple[int, int]:
    """
    Compute aggregate series scores for match.team1 and match.team2
    from scoreboard game entries.

    Normalizes team names and counts wins using each game's 'Winner'
    field (expected values 1 or 2). Games that do not involve both
    match teams or that contain an invalid/missing winner are ignored.

    Parameters:
        relevant_games (Iterable[dict]): Sequence of scoreboard
            entries; each entry should provide at least the keys
            'Team1', 'Team2', and 'Winner'.
        match (object): Match object with attributes `team1` and
            `team2` containing team names.

    Returns:
        tuple: (team1_score, team2_score) — integers representing
            the number of games won by match.team1 and match.team2,
            respectively.
    """
    m_t1 = _normalize_team_name(match.team1)
    m_t2 = _normalize_team_name(match.team2)

    team1_score = 0
    team2_score = 0
    for game in relevant_games:
        d1, d2 = _get_score_delta_from_game(game, m_t1, m_t2)
        team1_score += d1
        team2_score += d2

    return team1_score, team2_score


def determine_winner(
    team1_score: int, team2_score: int, match: Any
) -> Optional[str]:
    """
    Determine if there's a winner based on the current scores.
    Returns the winner team name or None if no winner yet.
    """
    # Handle missing best_of to avoid TypeError
    if match.best_of is None:
        return None

    games_to_win = (match.best_of // 2) + 1
    if team1_score >= games_to_win:
        return match.team1
    elif team2_score >= games_to_win:
        return match.team2
    return None


async def save_result_and_update_picks(session, match, winner, score_str):
    """
    Persist a match result and mark all picks for that match as
    correct or incorrect.

    Parameters:
        session: An asynchronous database session used to add the
            result and update picks.
        match: The Match model instance for which the result is being
            recorded.
        winner: The value stored as the winner on the Result;
            compared against each Pick.chosen_team to set is_correct.
        score_str: A human-readable score string to store on the
            Result.

    Returns:
        The created Result instance that was added to the session
            (not committed).
    """
    result = Result(
        match_id=match.id,
        winner=winner,
        score=score_str,
    )
    session.add(result)

    # Update picks
    logger.info("Updating picks for match %s", match.id)
    statement = select(Pick).where(Pick.match_id == match.id)
    result_proxy = await session.exec(statement)
    picks_to_update = result_proxy.all()
    updated_count = 0
    for pick in picks_to_update:
        pick.is_correct = pick.chosen_team == winner
        if pick.is_correct:
            pick.status = "correct"
            pick.score = 10
        else:
            pick.status = "incorrect"
            pick.score = 0
        session.add(pick)
        updated_count += 1
    logger.info("Updated %d picks for match %s.", updated_count, match.id)

    return result


def filter_relevant_games_from_scoreboard(
    scoreboard_data: Iterable[dict] | None, match: Any
) -> List[dict]:
    """
    Return the subset of scoreboard entries that involve exactly the
    two teams in the given match.

    Parameters:
        scoreboard_data (iterable[dict] | None): Iterable of scoreboard
            entries where each entry is expected to have "Team1" and
            "Team2" keys containing team names.
        match (Match): Match whose `team1` and `team2` names are used
            to identify relevant entries.

    Returns:
        list[dict]: List of scoreboard entries from `scoreboard_data`
            whose Team1/Team2 pair matches the match teams (order-
            insensitive). Returns an empty list if scoreboard_data is
            None.
    """
    if scoreboard_data is None:
        return []

    match_teams = {
        _normalize_team_name(match.team1),
        _normalize_team_name(match.team2),
    }
    return [
        g
        for g in scoreboard_data
        if {
            _normalize_team_name(g.get("Team1")),
            _normalize_team_name(g.get("Team2")),
        }
        == match_teams
    ]


async def fetch_teams(session, match: Match):
    """
    Retrieve the `Team` objects corresponding to the match's teams.

    This function prefers ID-based lookup: if `match.team1_id` and/or
    `match.team2_id` are present it will first attempt to retrieve the
    corresponding `Team` rows by `pandascore_id`. If an ID is absent,
    the function falls back to a name-based lookup using `match.team1`
    and `match.team2` respectively. If no `Team` is found for a side,
    the function returns `None` for that position.

    Parameters:
        session: Database session to execute queries.
        match (Match): Match instance whose `team1_id`/`team2_id` and
            `team1`/`team2` fields are used for lookups.

    Returns:
        tuple: `(team1_obj, team2_obj)` where each element is the
            matching `Team` object or `None` if no match was found. The
            function does not raise on missing teams; callers should
            handle `None` values appropriately.
    """
    t1_stmt = _build_team_lookup_stmt(match, match.team1_id, match.team1)
    t2_stmt = _build_team_lookup_stmt(match, match.team2_id, match.team2)

    team1 = (await session.exec(t1_stmt)).first()
    team2 = (await session.exec(t2_stmt)).first()
    return team1, team2


def _build_team_lookup_stmt(match: Match, team_id, team_name):
    stmt = select(Team).where(Team.game == _match_game(match))
    if team_id:
        return stmt.where(Team.pandascore_id == team_id)
    return stmt.where(Team.name == team_name)


def _match_game(match: Match) -> str:
    return getattr(match, "game", None) or "lol"
