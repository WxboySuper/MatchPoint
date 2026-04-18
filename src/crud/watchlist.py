from typing import List, Optional

from sqlmodel import select, Session
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models import UserWatchlist

# Synchronous helpers


def _make_watch(
    user_id: str,
    match_id: int,
    team_id: Optional[int] = None,
) -> UserWatchlist:
    """Construct a UserWatchlist instance (no DB side-effects)."""
    return UserWatchlist(user_id=user_id, match_id=match_id, team_id=team_id)


def _set_watched_flag(rec: UserWatchlist) -> UserWatchlist:
    """Set the watched flag on a record (shared logic)."""
    rec.is_watched = True
    return rec


def _add_watch_sync(
    session: Session,
    user_id: str,
    match_id: int,
    team_id: Optional[int] = None,
) -> UserWatchlist:
    rec = _make_watch(user_id, match_id, team_id)
    session.add(rec)
    session.commit()
    session.refresh(rec)
    return rec


def _mark_as_watched_sync(
    session: Session, watch_id: int
) -> Optional[UserWatchlist]:
    rec = session.get(UserWatchlist, watch_id)
    if rec is None:
        return None
    _set_watched_flag(rec)
    session.commit()
    session.refresh(rec)
    return rec


def add_watch(
    session: Session,
    user_id: str,
    match_id: int,
    team_id: Optional[int] = None,
) -> UserWatchlist:
    """Create a new watchlist entry and return it."""
    return _add_watch_sync(session, user_id, match_id, team_id)


def remove_watch(session: Session, watch_id: int) -> bool:
    rec = session.get(UserWatchlist, watch_id)
    if rec is None:
        return False
    session.delete(rec)
    session.commit()
    return True


def list_watches_for_user(
    session: Session, user_id: str
) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.user_id == user_id)
    return session.exec(stmt).all()


def list_watchers_for_match(
    session: Session, match_id: int
) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.match_id == match_id)
    return session.exec(stmt).all()


def mark_as_watched(
    session: Session, watch_id: int
) -> Optional[UserWatchlist]:
    return _mark_as_watched_sync(session, watch_id)


# Async helpers for runtime code paths
async def list_watches_for_user_async(
    session: AsyncSession, user_id: str
) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.user_id == user_id)
    res = await session.exec(stmt)
    return res.all()


async def list_watchers_for_match_async(
    session: AsyncSession, match_id: int
) -> List[UserWatchlist]:
    stmt = select(UserWatchlist).where(UserWatchlist.match_id == match_id)
    res = await session.exec(stmt)
    return res.all()


async def _add_watch_async(
    session: AsyncSession,
    user_id: str,
    match_id: int,
    team_id: Optional[int] = None,
) -> UserWatchlist:
    rec = _make_watch(user_id, match_id, team_id)
    session.add(rec)
    await session.commit()
    await session.refresh(rec)
    return rec


async def _mark_as_watched_async(
    session: AsyncSession, watch_id: int
) -> Optional[UserWatchlist]:
    rec = await session.get(UserWatchlist, watch_id)
    if rec is None:
        return None
    _set_watched_flag(rec)
    await session.commit()
    await session.refresh(rec)
    return rec


# Async write helpers
async def add_watch_async(
    session: AsyncSession,
    user_id: str,
    match_id: int,
    team_id: Optional[int] = None,
) -> UserWatchlist:
    """Async variant to create a new watchlist entry."""
    return await _add_watch_async(session, user_id, match_id, team_id)


async def remove_watch_async(session: AsyncSession, watch_id: int) -> bool:
    rec = await session.get(UserWatchlist, watch_id)
    if rec is None:
        return False
    await session.delete(rec)
    await session.commit()
    return True


async def mark_as_watched_async(
    session: AsyncSession, watch_id: int
) -> Optional[UserWatchlist]:
    return await _mark_as_watched_async(session, watch_id)
