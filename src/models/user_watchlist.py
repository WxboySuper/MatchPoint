from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func, text


class UserWatchlist(SQLModel, table=True):
    """User watchlist entries for matches or teams.

    Minimal model to support per-user bookmarks for upcoming matches.
    """

    __tablename__ = "user_watchlist"

    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: str = Field(sa_column=Column("user_id", String, nullable=False), index=True)
    match_id: int = Field(sa_column=Column("match_id", Integer, nullable=False), index=True)
    team_id: Optional[int] = Field(default=None, sa_column=Column("team_id", Integer, nullable=True))
    is_watched: bool = Field(default=False, nullable=False, sa_column=Column("is_watched", Boolean, nullable=False, server_default=text("0")))
    created_at: datetime = Field(default_factory=datetime.utcnow, sa_column=Column("created_at", DateTime(timezone=True), server_default=func.now()))
