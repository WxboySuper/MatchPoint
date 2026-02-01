# src/api/routes.py
"""API route handlers."""

import logging
from datetime import datetime, timezone, timedelta
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import selectinload
from sqlmodel import select

from src.db import get_session
from src.models import Match, Pick, User
from src import crud
from src.api.deps import verify_api_key
from src.api.schemas import (
    MatchResponse,
    PickRequest,
    PickResponse,
    PickSubmitResponse,
    UserPicksResponse,
)

logger = logging.getLogger("esports-bot.api")

router = APIRouter(prefix="/api", tags=["picks"])

# Number of days in advance that matches are available for picking
PICK_WINDOW_DAYS = 3


@router.get("/matches", response_model=List[MatchResponse])
async def list_matches(_: str = Depends(verify_api_key)):
    """
    List all matches available for picking.

    Returns matches scheduled within the next PICK_WINDOW_DAYS days
    that haven't started yet and don't have TBD teams.
    """
    now_utc = datetime.now(timezone.utc)
    pick_cutoff = now_utc + timedelta(days=PICK_WINDOW_DAYS)

    with get_session() as session:
        stmt = (
            select(Match)
            .options(selectinload(Match.contest))
            .where(Match.scheduled_time > now_utc)
            .where(Match.scheduled_time <= pick_cutoff)
            .where(Match.team1 != "TBD")
            .where(Match.team2 != "TBD")
            .order_by(Match.scheduled_time)
            .limit(25)
        )
        matches = session.exec(stmt).all()

        return [
            MatchResponse(
                id=m.id,
                team1=m.team1,
                team2=m.team2,
                team1_id=m.team1_id,
                team2_id=m.team2_id,
                best_of=m.best_of,
                scheduled_time=m.scheduled_time,
                contest_name=m.contest.name if m.contest else None,
                status=m.status,
            )
            for m in matches
        ]


@router.post("/picks", response_model=PickSubmitResponse)
async def submit_pick(
    pick_req: PickRequest,
    _: str = Depends(verify_api_key),
):
    """
    Submit or update a pick for a match.

    If the user has already picked this match, their pick will be updated.
    """
    now_utc = datetime.now(timezone.utc)

    with get_session() as session:
        # Get the match
        match = session.get(Match, pick_req.match_id)
        if not match:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Match {pick_req.match_id} not found",
            )

        # Check if match has started
        if now_utc >= match.scheduled_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Match has already started. Pick locked!",
            )

        # Validate team name (case-insensitive)
        team_lower = pick_req.chosen_team.lower()
        if match.team1.lower() == team_lower:
            chosen_team = match.team1
        elif match.team2.lower() == team_lower:
            chosen_team = match.team2
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Team '{pick_req.chosen_team}' not in this match. "
                       f"Choose: {match.team1} or {match.team2}",
            )

        # Ensure user exists
        db_user = crud.get_user_by_discord_id(session, pick_req.user_id)
        if not db_user:
            db_user = crud.create_user(
                session,
                pick_req.user_id,
                pick_req.username or f"User_{pick_req.user_id[:8]}",
            )

        # Check for existing pick
        existing_stmt = (
            select(Pick)
            .where(Pick.user_id == db_user.id)
            .where(Pick.match_id == pick_req.match_id)
        )
        existing_pick = session.exec(existing_stmt).first()

        if existing_pick:
            old_team = existing_pick.chosen_team
            existing_pick.chosen_team = chosen_team
            existing_pick.timestamp = now_utc
            session.add(existing_pick)
            session.commit()
            session.refresh(existing_pick)
            logger.info(
                "Pick updated: user=%s match=%s %s->%s",
                pick_req.user_id,
                pick_req.match_id,
                old_team,
                chosen_team,
            )
            return PickSubmitResponse(
                success=True,
                message=f"Pick updated: {old_team} → {chosen_team}",
                pick_id=existing_pick.id,
                action="updated",
            )
        else:
            new_pick = crud.create_pick(
                session,
                crud.PickCreateParams(
                    user_id=db_user.id,
                    contest_id=match.contest_id,
                    match_id=pick_req.match_id,
                    chosen_team=chosen_team,
                ),
            )
            logger.info(
                "Pick created: user=%s match=%s team=%s",
                pick_req.user_id,
                pick_req.match_id,
                chosen_team,
            )
            return PickSubmitResponse(
                success=True,
                message=f"Pick submitted: {chosen_team}",
                pick_id=new_pick.id,
                action="created",
            )


@router.get("/picks/{user_id}", response_model=UserPicksResponse)
async def get_user_picks(
    user_id: str,
    include_past: bool = False,
    _: str = Depends(verify_api_key),
):
    """
    Get all picks for a user.

    By default, only returns picks for upcoming matches.
    Set include_past=true to include picks for matches that have ended.
    """
    now_utc = datetime.now(timezone.utc)

    with get_session() as session:
        db_user = crud.get_user_by_discord_id(session, user_id)
        if not db_user:
            return UserPicksResponse(
                user_id=user_id,
                username=None,
                picks=[],
            )

        stmt = (
            select(Pick)
            .join(Match)
            .options(selectinload(Pick.match).selectinload(Match.contest))
            .where(Pick.user_id == db_user.id)
        )

        if not include_past:
            stmt = stmt.where(Match.scheduled_time > now_utc)

        stmt = stmt.order_by(Match.scheduled_time).limit(50)
        picks = session.exec(stmt).all()

        pick_responses = []
        for p in picks:
            m = p.match
            pick_responses.append(
                PickResponse(
                    id=p.id,
                    match_id=p.match_id,
                    chosen_team=p.chosen_team,
                    status=p.status,
                    is_correct=p.is_correct,
                    score=p.score,
                    timestamp=p.timestamp,
                    match=MatchResponse(
                        id=m.id,
                        team1=m.team1,
                        team2=m.team2,
                        team1_id=m.team1_id,
                        team2_id=m.team2_id,
                        best_of=m.best_of,
                        scheduled_time=m.scheduled_time,
                        contest_name=m.contest.name if m.contest else None,
                        status=m.status,
                    ) if m else None,
                )
            )

        return UserPicksResponse(
            user_id=user_id,
            username=db_user.username,
            picks=pick_responses,
        )


@router.get("/health")
async def health_check():
    """Health check endpoint (no auth required)."""
    return {"status": "ok", "service": "matchpoint-api"}
