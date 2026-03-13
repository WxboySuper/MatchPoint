# Developer & Agent Guidelines

This file is the rulebook for contributors and automated agents working on the
MatchPoint (Esports Pick'em Discord Bot) repository. It contains build/lint/test
commands, coding standards, and repository-specific conventions that agents
should follow.

All contributors (human and AI) must follow these guidelines.

1. Tech stack
- Language: Python 3.10+
- Async & HTTP: `asyncio`, `aiohttp`
- DB: SQLite + `sqlmodel`, migrations with `alembic`
- Discord client: `discord.py`
- Scheduler: `APScheduler`
- Dev tools: `pytest`, `black`, `flake8`

2. Build / Lint / Test commands
- Install runtime deps: `pip install -r requirements.txt`
- Install dev deps: `pip install -r dev-requirements.txt`
- Run full test suite: `python -m pytest` (pyproject.toml sets `pythonpath = ["."]`)
- Run a single test file: `python -m pytest tests/test_pick_logic.py`
- Run a single test by node id: `python -m pytest tests/test_pick_logic.py::test_some_case`
- Run tests with coverage: `pytest --cov=src --cov-report=term-missing`
- Format code: `black .` (configured line-length: 79 in `pyproject.toml`)
- Lint (static checks): `flake8`

Notes for single-test runs: use the test file path or the node id after `::` to
target an individual test. When running tests that touch async code, `pytest-asyncio`
is in `dev-requirements.txt` and is configured in tests.

3. Project layout and important files
- App code: `src/` (commands, crud, parsers, scheduling, sync)
- Tests: `tests/` (use `conftest.py` fixtures)
- DB config & sessions: `src/db.py`
- Config loader: `src/config.py`
- Alembic: `alembic/`
- Dev requirements: `dev-requirements.txt`

4. Code style & conventions
- Formatting: Run `black .` with the line length set in `pyproject.toml` (79).
- Linting: `flake8` for style checks; address F401/F811 type errors carefully.
- Imports:
  - Use absolute imports within the project (e.g. `from src.crud import match`).
  - Group imports in order: standard library, third-party, local packages;
    separate groups with a blank line.
  - Do not import large modules in hot-path functions; prefer local imports when
    needed to avoid circular deps.
- Typing:
  - Use type annotations for public functions, coroutine signatures, and data
    models. Prefer `typing.Optional[...]` over bare `None` defaults.
  - Use `SQLModel` models for DB-backed types; annotate fields with explicit
    types and use `TZDateTime` helper where appropriate for timezone-aware
    datetimes.
- Naming conventions:
  - Modules and packages: `snake_case` (e.g. `pandascore_client.py`).
  - Classes: `PascalCase` (e.g. `PandaScoreClient`).
  - Functions and variables: `snake_case`.
  - Constants: `UPPER_SNAKE_CASE` (e.g. `SQLITE_BUSY_TIMEOUT`).
- Async patterns:
  - Use `async def` for coroutine functions; prefer `asyncio.gather` to run
    independent tasks concurrently.
  - Use `asynccontextmanager` or context manager helpers (see `src/db.py`) when
    opening sessions or network resources.
  - Never perform blocking I/O in the event loop; delegate to threads/processes
    only when unavoidable.
- Error handling:
  - Prefer explicit exceptions over returning `None` for failure cases when the
    caller must react differently. Document exceptional behavior in docstrings.
  - Catch only the exceptions you expect and can handle; re-raise or wrap
    unexpected exceptions after adding context.
  - Use `tenacity` for retryable network operations (PandaScore calls) and keep
    retry logic isolated in `pandascore_client.py`.

5. Database & ORM
- Use `src/crud/` for all DB operations. Keep business logic out of CRUD.
- Avoid N+1 queries: when fetching models with relationships, use
  `.options(selectinload(...))` to eager-load related rows.
- Use the async SQLModel session for runtime code paths that run in the bot
  (Discord commands, schedulers). The synchronous engine is primarily for CLI
  and migration tasks.
- Alembic migrations must be idempotent for SQLite (check for existing
  columns/tables before adding). `alembic/env.py` reads `DATABASE_URL` from the
  environment; do not hardcode a DB URL in migration scripts.

6. Testing practices
- Tests live in `tests/`. Use `conftest.py` fixtures for shared setup.
- Mock the immediate dependency under test (e.g. mock `src.crud.get_match_by_id`
  when testing command handlers) rather than low-level DB internals.
- For `discord.ui.View` tests, mock `discord.Interaction` and set up items added
  to views. Use `pytest-asyncio` markers for async tests.
- Keep tests hermetic: avoid calling the real PandaScore API. Use test doubles
  and recorded responses where needed.

7. Async jobs & scheduling
- Jobs are scheduled via `APScheduler` wrappers in `src/scheduler.py`.
- Key jobs:
  - `perform_pandascore_sync` — hourly full sync
  - `poll_running_matches_job` — runs every minute to update live scores
- Keep job implementations small and testable; extract network calls to
  `pandascore_client.py` and processing to `pandascore_processing.py`.

8. Logging
- Use module-level `logger = logging.getLogger(__name__)` and structured
  messages where helpful. Avoid logging secrets (API keys, DB passwords).
- Use `_sanitize_database_url` pattern (see `src/db.py`) when logging DB URLs to
  avoid leaking credentials.

9. Security / secrets
- Read secrets from environment variables or `dotenv` in development. Do not
  commit `.env` files to source control. Example sample: `.env.example`.

10. CI / Git
- Follow semantic commit messages: `feat:`, `fix:`, `docs:`, `style:`,
  `refactor:`, `test:`, `chore:`.
- Pre-commit checklist before PRs:
  1. Run `black .` and `flake8` locally.
  2. Run `python -m pytest` and ensure tests pass.
  3. Ensure migrations are idempotent for SQLite.

11. Agent-specific rules
- Agents should not make destructive git operations (no `reset --hard`, no
  force-pushes). If asked to commit changes, stage only relevant files and
  create a single focused commit.
- Agents must not commit secrets (files such as `.env`, keyfiles) and must
  warn the user if such files are about to be included in a commit.

12. Cursor / Copilot rules
- Repository contains no Cursor rules or `.cursorrules` directories.
- There are no Copilot instruction files in `.github/` (search returned none).

13. Where to look next
- Key files to inspect when making changes:
  - `src/db.py`
  - `src/pandascore_client.py`
  - `src/crud/` (all files)
  - `tests/` (for examples of usage and fixtures)

Consider adding pre-commit hooks (for example, `pre-commit` with
`black`/`flake8`) and a `CONTRIBUTING.md` checklist for reviewers.

End of file.
