"""
Persistent rate-limit state management for PandaScore API.

Reads rate-limit headers from API responses and persists state
so quota tracking survives restarts.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# PandaScore rate-limit headers (standard-ish)
HEADER_REMAINING = "X-RateLimit-Remaining"
HEADER_LIMIT = "X-RateLimit-Limit"
HEADER_RESET = "X-RateLimit-Reset"  # Unix timestamp
HEADER_RETRY_AFTER = "Retry-After"


def extract_rate_limit_headers(
    headers: Dict[str, Any],
) -> Dict[str, Any]:
    """Extract rate-limit info from response headers.

    Returns a dict with keys: remaining, limit, reset_at, retry_after.
    Missing headers return None for that field.
    """
    remaining = headers.get(HEADER_REMAINING)
    limit = headers.get(HEADER_LIMIT)
    reset = headers.get(HEADER_RESET)
    retry_after = headers.get(HEADER_RETRY_AFTER)

    result: Dict[str, Any] = {}

    if remaining is not None:
        try:
            result["remaining"] = int(remaining)
        except (ValueError, TypeError):
            pass

    if limit is not None:
        try:
            result["limit"] = int(limit)
        except (ValueError, TypeError):
            pass

    if reset is not None:
        try:
            reset_ts = int(reset)
            result["reset_at"] = datetime.fromtimestamp(
                reset_ts, tz=timezone.utc
            )
        except (ValueError, TypeError, OSError):
            pass

    if retry_after is not None:
        try:
            result["retry_after"] = int(retry_after)
        except (ValueError, TypeError):
            pass

    return result


def should_backoff(
    remaining: Optional[int],
    reset_at: Optional[datetime],
    min_remaining: int = 50,
) -> Optional[int]:
    """Check if we should back off before making a request.

    Returns the number of seconds to wait, or None if no backoff needed.
    """
    if remaining is not None and remaining <= 0:
        if reset_at is not None:
            now = datetime.now(timezone.utc)
            delta = (reset_at - now).total_seconds()
            if delta > 0:
                return min(int(delta) + 1, 3600)  # Cap at 1 hour
        return 60  # Default: wait 1 minute

    if remaining is not None and remaining < min_remaining:
        if reset_at is not None:
            now = datetime.now(timezone.utc)
            delta = (reset_at - now).total_seconds()
            if delta > 0:
                # Spread requests over remaining window
                return min(max(int(delta / max(remaining, 1)), 1), 300)
        return 5  # Light backoff

    return None


def exponential_backoff(
    attempt: int,
    max_wait: int = 60,
    base: int = 2,
    jitter: bool = True,
) -> int:
    """Calculate exponential backoff with optional jitter.

    Args:
        attempt: Current retry attempt (0-indexed)
        max_wait: Maximum wait time in seconds
        base: Base for exponential calculation
        jitter: Add random jitter to avoid thundering herd

    Returns:
        Seconds to wait
    """
    wait = min(base**attempt, max_wait)
    if jitter:
        import random
        wait = int(wait * (0.5 + random.random()))
    return wait


class InMemoryRateLimitStore:
    """In-memory rate-limit state store (fallback when DB unavailable)."""

    def __init__(self):
        self._state: Dict[str, Dict[str, Any]] = {}

    async def get(self, resource: str) -> Optional[Dict[str, Any]]:
        return self._state.get(resource)

    async def set(self, resource: str, state: Dict[str, Any]) -> None:
        state["updated_at"] = datetime.now(timezone.utc)
        self._state[resource] = state


class DatabaseRateLimitStore:
    """Persistent rate-limit state store using the database."""

    def __init__(self, session_factory):
        """
        Args:
            session_factory: Async context manager that yields a DB session.
        """
        self._session_factory = session_factory

    async def get(self, resource: str) -> Optional[Dict[str, Any]]:
        try:
            from src.models import RateLimitState
            async with self._session_factory() as session:
                from sqlalchemy import select
                stmt = select(RateLimitState).where(
                    RateLimitState.resource == resource
                )
                result = await session.execute(stmt)
                row = result.scalar_one_or_none()
                if row is None:
                    return None
                return {
                    "remaining": row.remaining,
                    "limit": row.limit,
                    "reset_at": row.reset_at,
                    "updated_at": row.updated_at,
                }
        except Exception:
            logger.debug("Failed to read rate-limit state from DB", exc_info=True)
            return None

    async def set(self, resource: str, state: Dict[str, Any]) -> None:
        try:
            from src.models import RateLimitState
            async with self._session_factory() as session:
                from sqlalchemy import select
                stmt = select(RateLimitState).where(
                    RateLimitState.resource == resource
                )
                result = await session.execute(stmt)
                row = result.scalar_one_or_none()

                now = datetime.now(timezone.utc)
                if row is None:
                    row = RateLimitState(
                        resource=resource,
                        remaining=state.get("remaining", 1000),
                        limit=state.get("limit", 1000),
                        reset_at=state.get("reset_at"),
                        updated_at=now,
                    )
                    session.add(row)
                else:
                    row.remaining = state.get("remaining", row.remaining)
                    row.limit = state.get("limit", row.limit)
                    if "reset_at" in state:
                        row.reset_at = state["reset_at"]
                    row.updated_at = now

                await session.commit()
        except Exception:
            logger.debug("Failed to persist rate-limit state", exc_info=True)
