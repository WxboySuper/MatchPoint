"""Tests for rate-limit state management."""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

from src.rate_limit import (
    extract_rate_limit_headers,
    should_backoff,
    exponential_backoff,
    InMemoryRateLimitStore,
    HEADER_REMAINING,
    HEADER_LIMIT,
    HEADER_RESET,
    HEADER_RETRY_AFTER,
)


class TestExtractRateLimitHeaders:
    def test_extracts_all_headers(self):
        future_ts = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
        headers = {
            HEADER_REMAINING: "42",
            HEADER_LIMIT: "1000",
            HEADER_RESET: str(future_ts),
            HEADER_RETRY_AFTER: "30",
        }
        result = extract_rate_limit_headers(headers)
        assert result["remaining"] == 42
        assert result["limit"] == 1000
        assert result["retry_after"] == 30
        assert isinstance(result["reset_at"], datetime)

    def test_missing_headers_returns_empty(self):
        result = extract_rate_limit_headers({})
        assert result == {}

    def test_invalid_values_ignored(self):
        headers = {
            HEADER_REMAINING: "abc",
            HEADER_LIMIT: "",
            HEADER_RESET: "not_a_timestamp",
        }
        result = extract_rate_limit_headers(headers)
        assert result == {}

    def test_partial_headers(self):
        headers = {HEADER_REMAINING: "100"}
        result = extract_rate_limit_headers(headers)
        assert result["remaining"] == 100
        assert "limit" not in result
        assert "reset_at" not in result


class TestShouldBackoff:
    def test_no_backoff_when_healthy(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        assert should_backoff(remaining=500, reset_at=future) is None

    def test_backoff_when_exhausted(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=5)
        wait = should_backoff(remaining=0, reset_at=future)
        assert wait is not None
        assert wait > 0
        assert wait <= 3600

    def test_backoff_when_exhausted_no_reset(self):
        wait = should_backoff(remaining=0, reset_at=None)
        assert wait == 60

    def test_light_backoff_when_low(self):
        future = datetime.now(timezone.utc) + timedelta(minutes=5)
        wait = should_backoff(remaining=10, reset_at=future, min_remaining=50)
        assert wait is not None
        assert wait > 0

    def test_no_backoff_above_threshold(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        assert should_backoff(remaining=100, reset_at=future, min_remaining=50) is None

    def test_backoff_cap_at_one_hour(self):
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        wait = should_backoff(remaining=0, reset_at=future)
        assert wait is not None
        assert wait <= 3600


class TestExponentialBackoff:
    def test_increases_with_attempt(self):
        w0 = exponential_backoff(0, jitter=False)
        w1 = exponential_backoff(1, jitter=False)
        w2 = exponential_backoff(2, jitter=False)
        assert w0 <= w1 <= w2

    def test_caps_at_max_wait(self):
        w = exponential_backoff(10, max_wait=30, jitter=False)
        assert w == 30

    def test_jitter_varies(self):
        # With jitter, repeated calls should produce different values sometimes
        results = {exponential_backoff(3, jitter=True) for _ in range(20)}
        assert len(results) > 1  # Should have some variation


class TestInMemoryRateLimitStore:
    @pytest.mark.asyncio
    async def test_get_set(self):
        store = InMemoryRateLimitStore()
        assert await store.get("lol") is None

        state = {"remaining": 500, "limit": 1000}
        await store.set("lol", state)
        result = await store.get("lol")
        assert result["remaining"] == 500
        assert result["limit"] == 1000
        assert "updated_at" in result

    @pytest.mark.asyncio
    async def test_multiple_resources(self):
        store = InMemoryRateLimitStore()
        await store.set("lol", {"remaining": 100})
        await store.set("csgo", {"remaining": 200})

        lol_state = await store.get("lol")
        csgo_state = await store.get("csgo")
        assert lol_state["remaining"] == 100
        assert csgo_state["remaining"] == 200

    @pytest.mark.asyncio
    async def test_update_existing(self):
        store = InMemoryRateLimitStore()
        await store.set("lol", {"remaining": 500})
        await store.set("lol", {"remaining": 100})
        result = await store.get("lol")
        assert result["remaining"] == 100
