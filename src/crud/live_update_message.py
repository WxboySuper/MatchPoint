from typing import Optional
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from datetime import datetime, timezone

from src.models import LiveUpdateMessage

from dataclasses import dataclass


@dataclass
class LiveMessagePayload:
    """Container for scope information for live update messages."""

    scope_type: Optional[str] = "guild_live"
    scope_key: Optional[str] = None


def set_live_message_v2(
    session,
    guild_id: int,
    channel_id: int,
    message_id: int,
    payload: Optional[LiveMessagePayload] = None,
) -> LiveUpdateMessage:
    """Wrapper accepting LiveMessagePayload."""
    scope_type = payload.scope_type if payload is not None else "guild_live"
    scope_key = payload.scope_key if payload is not None else None
    return set_live_message(
        session, guild_id, channel_id, message_id, scope_type, scope_key
    )


async def set_live_message_async_v2(
    session: AsyncSession,
    guild_id: int,
    channel_id: int,
    message_id: int,
    payload: Optional[LiveMessagePayload] = None,
) -> LiveUpdateMessage:
    """Async wrapper accepting LiveMessagePayload."""
    scope_type = payload.scope_type if payload is not None else "guild_live"
    scope_key = payload.scope_key if payload is not None else None
    return await set_live_message_async(
        session, guild_id, channel_id, message_id, scope_type, scope_key
    )


def get_live_message(
    session,
    guild_id: int,
    scope_type: Optional[str] = None,
    scope_key: Optional[str] = None,
) -> Optional[LiveUpdateMessage]:
    """
    Retrieve a LiveUpdateMessage for a guild optionally scoped by
    `scope_type` and `scope_key`.
    If scope_type is None, matches any scope_type (backwards compatible).
    If scope_key is None, matches any scope_key.
    """
    stmt = select(LiveUpdateMessage).where(
        LiveUpdateMessage.guild_id == guild_id
    )
    if scope_type is not None:
        stmt = stmt.where(LiveUpdateMessage.scope_type == scope_type)
    if scope_key is not None:
        stmt = stmt.where(LiveUpdateMessage.scope_key == scope_key)
    return session.exec(stmt).first()


def set_live_message(
    session,
    guild_id: int,
    channel_id: int,
    message_id: int,
    scope_type: str = "guild_live",
    scope_key: Optional[str] = None,
) -> LiveUpdateMessage:
    # Find existing record scoped to the provided scope_type/scope_key so
    # we update the correct canonical message for that scope.
    rec = get_live_message(session, guild_id, scope_type, scope_key)
    if rec is None:
        rec = LiveUpdateMessage(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
            scope_type=scope_type,
            scope_key=scope_key,
            last_rendered_at=datetime.now(timezone.utc),
        )
        session.add(rec)
    else:
        rec.channel_id = channel_id
        rec.message_id = message_id
        rec.scope_type = scope_type
        rec.scope_key = scope_key
        rec.last_rendered_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(rec)
    return rec


def delete_live_message(session, guild_id: int) -> None:
    rec = get_live_message(session, guild_id)
    if rec:
        session.delete(rec)
        session.commit()


# Async helpers for runtime code paths
async def get_live_message_async(
    session: AsyncSession,
    guild_id: int,
    scope_type: Optional[str] = None,
    scope_key: Optional[str] = None,
) -> Optional[LiveUpdateMessage]:
    stmt = select(LiveUpdateMessage).where(
        LiveUpdateMessage.guild_id == guild_id
    )
    if scope_type is not None:
        stmt = stmt.where(LiveUpdateMessage.scope_type == scope_type)
    if scope_key is not None:
        stmt = stmt.where(LiveUpdateMessage.scope_key == scope_key)
    res = await session.exec(stmt)
    return res.first()


async def set_live_message_async(
    session: AsyncSession,
    guild_id: int,
    channel_id: int,
    message_id: int,
    scope_type: str = "guild_live",
    scope_key: Optional[str] = None,
) -> LiveUpdateMessage:
    rec = await get_live_message_async(
        session, guild_id, scope_type, scope_key
    )
    now = datetime.now(timezone.utc)
    if rec is None:
        rec = LiveUpdateMessage(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
            scope_type=scope_type,
            scope_key=scope_key,
            last_rendered_at=now,
        )
        session.add(rec)
        await session.commit()
        await session.refresh(rec)
        return rec

    rec.channel_id = channel_id
    rec.message_id = message_id
    rec.scope_type = scope_type
    rec.scope_key = scope_key
    rec.last_rendered_at = now
    await session.commit()
    await session.refresh(rec)
    return rec


async def delete_live_message_async(
    session: AsyncSession, guild_id: int
) -> None:
    rec = await get_live_message_async(session, guild_id)
    if rec:
        await session.delete(rec)
        await session.commit()
