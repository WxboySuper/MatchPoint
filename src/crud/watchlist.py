from typing import List, Optional

from sqlmodel import select, Session
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models import UserWatchlist


# Synchronous helpers
def add_watch(session: Session, user_id: str, match_id: int, team_id: Optional[int] = None) -> UserWatchlist:
    """Create a new watchlist entry and return it."""
    rec = UserWatchlist(user_id=user_id, match_id=match_id, team_id=team_id)
    session.add(rec)
    session.commit()
    session.refresh(rec)
    return rec


def remove_watch(session: Session, watch_id: int) -> bool:
    rec = session.get(UserWatchlist, watch_id)
    if rec is None:
        return False
    session.delete(rec)
    session.commit()
    return True


def list_watches_for_user(session: Session, user_id: str) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.user_id == user_id)
    return session.exec(stmt).all()


def list_watchers_for_match(session: Session, match_id: int) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.match_id == match_id)
    return session.exec(stmt).all()


def mark_as_watched(session: Session, watch_id: int) -> Optional[UserWatchlist]:
    rec = session.get(UserWatchlist, watch_id)
    if rec is None:
        return None
    rec.is_watched = True
    session.commit()
    session.refresh(rec)
    return rec


# Async helpers for runtime code paths
async def list_watches_for_user_async(session: AsyncSession, user_id: str) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.user_id == user_id)
    res = await session.exec(stmt)
    return res.all()


async def list_watchers_for_match_async(session: AsyncSession, match_id: int) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.match_id == match_id)
    res = await session.exec(stmt)
    return res.all()


# Async write helpers
async def add_watch_async(session: AsyncSession, user_id: str, match_id: int, team_id: Optional[int] = None) -> UserWatchlist:
    """Async variant to create a new watchlist entry."""
    rec = UserWatchlist(user_id=user_id, match_id=match_id, team_id=team_id)
    session.add(rec)
    await session.commit()
    await session.refresh(rec)
    return rec


async def remove_watch_async(session: AsyncSession, watch_id: int) -> bool:
    rec = await session.get(UserWatchlist, watch_id)
    if rec is None:
        return False
    await session.delete(rec)
    await session.commit()
    return True


async def mark_as_watched_async(session: AsyncSession, watch_id: int) -> Optional[UserWatchlist]:
    rec = await session.get(UserWatchlist, watch_id)
    if rec is None:
        return None
    rec.is_watched = True
    await session.commit()
    await session.refresh(rec)
    return rec
