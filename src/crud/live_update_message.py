from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple, Union

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models import LiveUpdateMessage


@dataclass
class LiveMessageTarget:
    """Container for the guild/channel/message identifiers."""

    guild_id: int
    channel_id: int
    message_id: int


@dataclass
class LiveMessagePayload:
    """Container for scope information for live update messages."""

    scope_type: Optional[str] = "guild_live"
    scope_key: Optional[str] = None


def _normalize_payload(
    payload: Optional[LiveMessagePayload] = None,
    legacy_scope: Tuple[Optional[str], ...] = (),
    scope_key: Optional[str] = None,
) -> LiveMessagePayload:
    """Normalize legacy scope arguments into a payload."""
    if payload is not None:
        return payload

    legacy_scope_type = legacy_scope[0] if legacy_scope else "guild_live"
    legacy_scope_key = legacy_scope[1] if len(legacy_scope) > 1 else scope_key
    return LiveMessagePayload(
        scope_type=legacy_scope_type or "guild_live",
        scope_key=legacy_scope_key,
    )


def _normalize_target(
    args: Tuple[Union[int, LiveMessageTarget], ...],
) -> Tuple[LiveMessageTarget, Tuple[Optional[str], ...]]:
    """Support both target-based and legacy positional call shapes."""
    if len(args) == 1 and isinstance(args[0], LiveMessageTarget):
        return args[0], ()

    if len(args) not in {3, 4, 5}:
        raise TypeError(
            "Expected LiveMessageTarget or guild_id, channel_id, "
            "message_id[, scope_type[, scope_key]]"
        )

    guild_id, channel_id, message_id = args[:3]
    target = LiveMessageTarget(
        guild_id=int(guild_id),
        channel_id=int(channel_id),
        message_id=int(message_id),
    )
    legacy_scope = tuple(args[3:])
    return target, legacy_scope


def _apply_live_message(
    rec: LiveUpdateMessage,
    target: LiveMessageTarget,
    payload: LiveMessagePayload,
    now: datetime,
) -> LiveUpdateMessage:
    rec.guild_id = target.guild_id
    rec.channel_id = target.channel_id
    rec.message_id = target.message_id
    rec.scope_type = payload.scope_type or "guild_live"
    rec.scope_key = payload.scope_key
    rec.last_rendered_at = now
    return rec


def _build_live_message(
    target: LiveMessageTarget, payload: LiveMessagePayload, now: datetime
) -> LiveUpdateMessage:
    return LiveUpdateMessage(
        guild_id=target.guild_id,
        channel_id=target.channel_id,
        message_id=target.message_id,
        scope_type=payload.scope_type or "guild_live",
        scope_key=payload.scope_key,
        last_rendered_at=now,
    )


def set_live_message_v2(
    session,
    target: LiveMessageTarget,
    payload: Optional[LiveMessagePayload] = None,
) -> LiveUpdateMessage:
    """Persist a live message using typed target/payload containers."""
    return set_live_message(session, target, payload=payload)


async def set_live_message_async_v2(
    session: AsyncSession,
    target: LiveMessageTarget,
    payload: Optional[LiveMessagePayload] = None,
) -> LiveUpdateMessage:
    """Async variant of the typed target/payload API."""
    return await set_live_message_async(session, target, payload=payload)


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
    *args: Union[int, LiveMessageTarget],
    payload: Optional[LiveMessagePayload] = None,
) -> LiveUpdateMessage:
    """Persist a live message using either target or legacy arguments."""
    target, legacy_scope = _normalize_target(args)
    normalized_payload = _normalize_payload(
        payload=payload,
        legacy_scope=legacy_scope,
    )

    rec = get_live_message(
        session,
        target.guild_id,
        normalized_payload.scope_type,
        normalized_payload.scope_key,
    )
    now = datetime.now(timezone.utc)
    if rec is None:
        rec = _build_live_message(target, normalized_payload, now)
        session.add(rec)
    else:
        _apply_live_message(rec, target, normalized_payload, now)
    session.commit()
    session.refresh(rec)
    return rec


def delete_live_message(
    session,
    guild_id: int,
    scope_type: Optional[str] = None,
    scope_key: Optional[str] = None,
) -> None:
    rec = get_live_message(session, guild_id, scope_type, scope_key)
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
    *args: Union[int, LiveMessageTarget],
    payload: Optional[LiveMessagePayload] = None,
) -> LiveUpdateMessage:
    """Async persist helper supporting typed and legacy arguments."""
    target, legacy_scope = _normalize_target(args)
    normalized_payload = _normalize_payload(
        payload=payload,
        legacy_scope=legacy_scope,
    )

    rec = await get_live_message_async(
        session,
        target.guild_id,
        normalized_payload.scope_type,
        normalized_payload.scope_key,
    )
    now = datetime.now(timezone.utc)
    if rec is None:
        rec = _build_live_message(target, normalized_payload, now)
        session.add(rec)
        await session.commit()
        await session.refresh(rec)
        return rec

    _apply_live_message(rec, target, normalized_payload, now)
    await session.commit()
    await session.refresh(rec)
    return rec


async def delete_live_message_async(
    session: AsyncSession,
    guild_id: int,
    scope_type: Optional[str] = None,
    scope_key: Optional[str] = None,
) -> None:
    rec = await get_live_message_async(
        session, guild_id, scope_type=scope_type, scope_key=scope_key
    )
    if rec:
        await session.delete(rec)
        await session.commit()
