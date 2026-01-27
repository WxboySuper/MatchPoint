"""
PandaScore API Client for Esports Pickem Bot.

Provides async methods to interact with the PandaScore REST API
for fetching upcoming matches, running (live) matches, and match results.
Implements rate limiting, retry logic, and error handling.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union, Tuple

import aiohttp
from aiohttp import ClientError, ClientResponseError, ClientTimeout

from src.config import PANDASCORE_API_KEY

logger = logging.getLogger(__name__)

# Broad JSON type for arbitrary PandaScore responses (dicts, lists, primitives)
JSONType = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

# PandaScore API configuration
BASE_URL = "https://api.pandascore.co"
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100

# Rate limit: 1,000 requests/hour = ~16.7 req/min
# We'll be conservative and track our usage
RATE_LIMIT_REQUESTS = 1000
RATE_LIMIT_WINDOW_SECONDS = 3600


class PandaScoreError(Exception):
    """Base exception for PandaScore API errors."""

    pass


class RateLimitError(PandaScoreError):
    """Raised when API rate limit is exceeded."""

    def __init__(self, retry_after: Optional[int] = None):
        self.retry_after = retry_after
        super().__init__(
            f"Rate limit exceeded. Retry after {retry_after} seconds."
        )


class PandaScoreClient:
    """
    Async client for the PandaScore REST API.

    Handles authentication, rate limiting, retries, and provides
    methods for fetching League of Legends match data.
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the PandaScore client.

        Args:
            api_key: PandaScore API key. If not provided, uses
                PANDASCORE_API_KEY from config.
        """
        self.api_key = api_key or PANDASCORE_API_KEY

        # Determine whether the client is operational or disabled due to
        # a missing API key. In disabled mode we avoid creating HTTP
        # sessions without Authorization headers and make network calls
        # fail with a clear exception. Tests that patch instance methods
        # can still replace them as needed.
        if not (isinstance(self.api_key, str) and self.api_key.strip()):
            self._disabled = True
            logger.warning(
                "PandaScore API key is not configured; client created in "
                "disabled mode. Network calls will raise PandaScoreError."
            )
        else:
            self._disabled = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._request_count = 0
        self._window_start = datetime.now(timezone.utc)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        if self._session is None or self._session.closed:
            headers = self._build_headers()
            timeout = ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                headers=headers, timeout=timeout
            )

        return self._session

    def _build_headers(self) -> Dict[str, Any]:
        """Construct and validate headers for aiohttp sessions.

        Extracted to simplify `_get_session` and reduce cyclomatic
        complexity. Raises `PandaScoreError` if the API key is missing.
        """
        if getattr(self, "_disabled", False):
            raise PandaScoreError(
                (
                    "PandaScore client is disabled because no API key "
                    "is configured"
                )
            )
        headers: Dict[str, Any] = {"Accept": "application/json"}
        if isinstance(self.api_key, str) and self.api_key.strip():
            headers["Authorization"] = f"Bearer {self.api_key}"
            return headers

        raise PandaScoreError(
            ("Attempted to create aiohttp session without " "API key")
        )

    @staticmethod
    def _build_url(endpoint: str) -> str:
        return f"{BASE_URL}{endpoint}"

    @staticmethod
    async def _do_request_once(
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> JSONType:
        """Perform a single HTTP GET and return parsed JSON or raise.

        Keeps the request-scoped branching small so _make_request is simpler.
        """
        async with session.get(url, params=params) as response:
            if response.status == 429:
                retry_after = response.headers.get("Retry-After")
                retry_seconds = int(retry_after) if retry_after else 60
                logger.warning(
                    "PandaScore rate limit hit. Retry in %d seconds",
                    retry_seconds,
                )
                raise RateLimitError(retry_after=retry_seconds)

            response.raise_for_status()
            return await response.json()

    @staticmethod
    async def _handle_client_response_error(
        e: ClientResponseError, attempt: int, max_retries: int, url: str
    ) -> None:
        """Handle ClientResponseError with logging and backoff."""
        logger.error(
            "PandaScore API error: %s %s - %s",
            e.status,
            getattr(e, "message", str(e)),
            url,
        )
        if attempt == max_retries - 1:
            raise PandaScoreError(
                f"API error after {max_retries} attempts: {e}"
            )
        await asyncio.sleep(2**attempt)

    @staticmethod
    async def _handle_client_error(
        e: ClientError, attempt: int, max_retries: int
    ) -> None:
        """Handle ClientError with logging and backoff."""
        logger.error("PandaScore connection error: %s", e)
        if attempt == max_retries - 1:
            raise PandaScoreError(
                f"Connection error after {max_retries} attempts: {e}"
            )
        await asyncio.sleep(2**attempt)

    @staticmethod
    async def _handle_timeout_error(
        e: asyncio.TimeoutError, attempt: int, max_retries: int
    ) -> None:
        """Handle asyncio.TimeoutError with logging and backoff."""
        logger.error("PandaScore request timeout: %s", e)
        if attempt == max_retries - 1:
            raise PandaScoreError(
                f"Request timeout after {max_retries} attempts: {e}"
            )
        await asyncio.sleep(2**attempt)

    @staticmethod
    async def _handle_rate_limit_error(
        e: RateLimitError, attempt: int, max_retries: int
    ) -> None:
        """Handle PandaScore rate-limit errors with Retry-After/backoff.

        Extracted from `_request_with_retries` to reduce its cyclomatic
        complexity and centralize rate-limit behavior.
        """
        retry_seconds = getattr(e, "retry_after", None)
        if attempt == max_retries - 1:
            raise e
        if retry_seconds is None:
            retry_seconds = min(60, 2**attempt)
        logger.warning(
            "PandaScore rate limited; sleeping %s seconds before retry",
            retry_seconds,
        )
        await asyncio.sleep(retry_seconds)

    async def _request_with_retries(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> JSONType:
        """Perform request with retry/backoff handling.

        Extracted to reduce complexity in `_make_request`.
        """
        for attempt in range(max_retries):
            try:
                self._request_count += 1
                logger.debug(
                    "PandaScore request #%d: GET %s params=%s",
                    self._request_count,
                    url,
                    params,
                )

                return await self._do_request_once(session, url, params)

            except RateLimitError as e:
                await self._handle_rate_limit_error(e, attempt, max_retries)
            except ClientResponseError as e:
                await self._handle_client_response_error(
                    e, attempt, max_retries, url
                )
            except ClientError as e:
                await self._handle_client_error(e, attempt, max_retries)
            except asyncio.TimeoutError as e:
                await self._handle_timeout_error(e, attempt, max_retries)

        raise PandaScoreError("Request failed after all retries")

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _check_rate_limit(self):
        """Check if we're within rate limits."""
        now = datetime.now(timezone.utc)
        elapsed = (now - self._window_start).total_seconds()

        # Reset window if an hour has passed
        if elapsed >= RATE_LIMIT_WINDOW_SECONDS:
            self._request_count = 0
            self._window_start = now
            return

        # Check if we've exceeded the limit
        if self._request_count >= RATE_LIMIT_REQUESTS:
            remaining = RATE_LIMIT_WINDOW_SECONDS - elapsed
            raise RateLimitError(retry_after=int(remaining))

    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> JSONType:
        """
        Make an authenticated request to the PandaScore API.

        Args:
            endpoint: API endpoint path (e.g., "/lol/matches/upcoming")
            params: Optional query parameters
            max_retries: Maximum number of retry attempts

        Returns:
            JSON response as a dictionary

        Raises:
            PandaScoreError: On API errors
            RateLimitError: When rate limit is exceeded
        """
        self._check_rate_limit()

        url = self._build_url(endpoint)
        session = await self._get_session()

        return await self._request_with_retries(
            session, url, params, max_retries
        )

    async def _fetch_matches(
        self,
        endpoint: str,
        params: Dict[str, Any],
        description: str,
    ) -> List[JSONType]:
        """
        Generic helper to fetch matches from a specific endpoint.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            description: Description for logging

        Returns:
            List of match objects
        """
        try:
            result = await self._make_request(endpoint, params=params)
            count = len(result) if isinstance(result, list) else 0
            logger.info("Fetched %d %s", count, description)
            return result if isinstance(result, list) else []
        except PandaScoreError:
            logger.exception("Failed to fetch %s", description)
            return []

    @staticmethod
    def _build_params(
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build query params for PandaScore match endpoints from options.

        Accepts a single `options` dict to avoid long arg lists and keep
        the helper signature small for linters.
        """
        opts = options or {}
        params: Dict[str, Any] = {}
        sort = opts.get("sort")
        page_size = opts.get("page_size", DEFAULT_PAGE_SIZE)
        page = opts.get("page")
        filter_key = opts.get("filter_key")
        filter_values = opts.get("filter_values")

        if sort:
            params["sort"] = sort

        params["page[size]"] = min(page_size, MAX_PAGE_SIZE)
        if page is not None:
            params["page[number]"] = page

        if filter_key and filter_values:
            params[f"filter[{filter_key}]"] = ",".join(map(str, filter_values))

        return params

    def _prepare_fetch_context(
        self, kind: str, opts: Dict[str, Any], desc_template: str
    ) -> Tuple[Dict[str, Any], str]:
        """Prepare params and description for fetch request.

        Handles the different parameter structures for running vs other
        matches.
        """
        if kind == "running":
            params = {
                "page[size]": opts.get("page_size", DEFAULT_PAGE_SIZE),
                "page[number]": opts.get("page", 1),
            }
            description = desc_template
        else:
            sort = opts.get("sort") or (
                "scheduled_at" if kind == "upcoming" else "-scheduled_at"
            )
            build_opts = {
                "sort": sort,
                "page_size": opts.get("page_size", DEFAULT_PAGE_SIZE),
                "page": opts.get("page"),
                "filter_key": opts.get("filter_key"),
                "filter_values": opts.get("filter_values"),
            }
            params = self._build_params(build_opts)
            description = desc_template.format(page=opts.get("page", 1))
        return params, description

    async def fetch_matches(
        self,
        kind: str,
        options: Optional[Dict[str, Any]] = None,
        game: str = "lol",
    ) -> List[JSONType]:
        """Unified fetch entrypoint for different match types.

        `kind` may be one of: "upcoming", "recent_past", "past", "running".
        `game` specifies the game title (e.g., "lol", "dota2", "csgo").
        This consolidates parameter construction and endpoint selection to
        avoid duplicated code across the specific fetch helpers.
        """
        opts = options or {}
        k = (kind or "").lower()
        g = (game or "lol").lower()

        # Endpoint selection mapping
        mapping = {
            "upcoming": (
                f"/{g}/matches/upcoming",
                f"upcoming {g} matches (page {{page}})",
            ),
            "recent_past": (f"/{g}/matches/past", f"recent past {g} matches"),
            "past": (f"/{g}/matches/past", f"past {g} matches"),
            "running": (f"/{g}/matches/running", f"running {g} matches"),
        }

        entry = mapping.get(k)
        if not entry:
            raise ValueError(f"Unknown match fetch kind: {kind}")

        endpoint, desc_template = entry
        params, description = self._prepare_fetch_context(
            k, opts, desc_template
        )

        return await self._fetch_matches(endpoint, params, description)

    async def fetch_upcoming_matches(
        self,
        league_ids: Optional[List[int]] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        page: int = 1,
        game: str = "lol",
    ) -> List[JSONType]:
        """
        Fetch upcoming matches.

        Args:
            league_ids: Optional list of league IDs to filter by
            page_size: Number of results per page (max 100)
            page: Page number for pagination
            game: Game slug (default: "lol")

        Returns:
            List of match objects from PandaScore
        """
        return await self.fetch_matches(
            "upcoming",
            {
                "filter_key": "league_id",
                "filter_values": league_ids,
                "page_size": page_size,
                "page": page,
            },
            game=game,
        )

    async def fetch_running_matches(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        page: int = 1,
        game: str = "lol",
    ) -> List[JSONType]:
        """
        Backwards-compatible helper to fetch running matches.

        Tests and older call sites may patch or call this method; keep a
        thin wrapper around the unified `fetch_matches` API.
        """
        return await self.fetch_matches(
            "running",
            {"page_size": page_size, "page": page},
            game=game,
        )

    async def fetch_all_upcoming_matches(
        self,
        league_ids: Optional[List[int]] = None,
        max_pages: int = 5,
        game: str = "lol",
    ) -> List[JSONType]:
        """
        Fetch all upcoming matches across multiple pages.

        Args:
            league_ids: Optional list of league IDs to filter by
            max_pages: Maximum number of pages to fetch
            game: Game slug (default: "lol")

        Returns:
            Combined list of all match objects
        """
        # This helper is deprecated; `fetch_matches("upcoming", ...)` with
        # explicit pagination should be used instead. Keep a thin wrapper for
        # backward compatibility while discouraging usage.
        all_matches = []

        for p in range(1, max_pages + 1):
            matches = await self.fetch_matches(
                "upcoming",
                {
                    "filter_key": "league_id",
                    "filter_values": league_ids,
                    "page_size": MAX_PAGE_SIZE,
                    "page": p,
                },
                game=game,
            )
            if not matches:
                break
            all_matches.extend(matches)
            if len(matches) < MAX_PAGE_SIZE:
                break

        logger.info("Fetched total of %d upcoming matches", len(all_matches))
        return all_matches

    async def fetch_match_by_id(
        self, match_id: int, game: str = "lol"
    ) -> Optional[JSONType]:
        """
        Fetch a specific match by its PandaScore ID.

        Args:
            match_id: PandaScore match ID
            game: Game slug (default: "lol")

        Returns:
            Match object or None if not found
        """
        try:
            result = await self._make_request(f"/{game}/matches/{match_id}")
            logger.debug(
                "Fetched match %d: status=%s", match_id, result.get("status")
            )
            return result
        except PandaScoreError as e:
            logger.error("Failed to fetch match %d: %s", match_id, e)
            return None


# Module-level singleton instance


class DisabledPandaScoreClient:
    """A minimal, import-safe disabled client used when the real client
    cannot be constructed (e.g. missing API key during test collection).

    This provides harmless defaults for high-level methods so importing
    modules and tests won't fail. Network methods return empty results or
    None and attempting to open a session raises `PandaScoreError`.
    """

    def __init__(self) -> None:
        self._disabled = True
        self.api_key = None
        self._session = None
        self._request_count = 0
        self._window_start = datetime.now(timezone.utc)

    # skipcq: PYL-R0201
    async def _get_session(
        self,
    ) -> aiohttp.ClientSession:  # pragma: no cover - simple sentinel
        raise PandaScoreError("PandaScore client is disabled (no API key)")

    # skipcq: PYL-R0201
    async def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 0,
    ) -> JSONType:
        raise PandaScoreError("PandaScore client is disabled (no API key)")

    # skipcq: PYL-R0201
    async def fetch_matches(
        self,
        kind: str,
        options: Optional[Dict[str, Any]] = None,
        game: str = "lol",
    ) -> List[JSONType]:
        return []

    # skipcq: PYL-R0201
    async def fetch_upcoming_matches(
        self,
        league_ids: Optional[List[int]] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        page: int = 1,
        game: str = "lol",
    ) -> List[JSONType]:
        return []

    # skipcq: PYL-R0201
    async def fetch_running_matches(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        page: int = 1,
        game: str = "lol",
    ) -> List[JSONType]:
        return []

    # skipcq: PYL-R0201
    async def fetch_all_upcoming_matches(
        self,
        league_ids: Optional[List[int]] = None,
        max_pages: int = 5,
        game: str = "lol",
    ) -> List[JSONType]:
        return []

    # skipcq: PYL-R0201
    async def fetch_match_by_id(
        self, match_id: int, game: str = "lol"
    ) -> Optional[JSONType]:
        return None

    # skipcq: PYL-R0201
    async def close(self) -> None:
        return None


class _LazyPandaScoreClientProxy:
    """Proxy that lazily constructs the real PandaScore client on first use.

    This preserves the module-level `pandascore_client` object shape so
    existing call sites can continue to use `pandascore_client.method(...)`
    without triggering client construction during import.
    """

    def __init__(self) -> None:
        self._client = None

    def _construct(self):
        if self._client is None:
            try:
                self._client = PandaScoreClient()
            except Exception:
                self._client = DisabledPandaScoreClient()

    def __getattr__(self, item):
        self._construct()
        return getattr(self._client, item)


# Export a module-level proxy instance that will construct the real
# client lazily when first accessed.
pandascore_client = _LazyPandaScoreClientProxy()
