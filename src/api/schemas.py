# src/api/schemas.py
"""Pydantic schemas for API requests/responses."""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel


class MatchResponse(BaseModel):
    """Match data for API responses."""
    id: int
    team1: str
    team2: str
    team1_id: Optional[int] = None
    team2_id: Optional[int] = None
    best_of: Optional[int] = None
    scheduled_time: datetime
    contest_name: Optional[str] = None
    status: Optional[str] = None

    class Config:
        from_attributes = True


class PickRequest(BaseModel):
    """Request body for submitting a pick."""
    user_id: str  # Discord user ID
    username: Optional[str] = None  # For creating new users
    match_id: int
    chosen_team: str


class PickResponse(BaseModel):
    """Pick data for API responses."""
    id: int
    match_id: int
    chosen_team: str
    status: Optional[str] = None
    is_correct: Optional[bool] = None
    score: Optional[int] = None
    timestamp: datetime
    match: Optional[MatchResponse] = None

    class Config:
        from_attributes = True


class PickSubmitResponse(BaseModel):
    """Response after submitting a pick."""
    success: bool
    message: str
    pick_id: Optional[int] = None
    action: str  # "created" or "updated"


class UserPicksResponse(BaseModel):
    """Response for user's picks."""
    user_id: str
    username: Optional[str] = None
    picks: List[PickResponse]
