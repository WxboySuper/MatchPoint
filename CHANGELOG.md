# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- markdownlint-disable -->

## [v1.4] - 2026-04-16

### Added

- Watchlist: per-user bookmark support via new `user_watchlist` table and model (`src/models.py`, migration `alembic/versions/d9f7a6b5c4e3_add_user_watchlist.py`).
- `/watch` slash command: add/remove/list bookmarks (`src/commands/watch.py`).
- `/catchup` command: spoilered catchup list and `Mark as Watched` interactive action (`src/commands/catchup.py`).
- DM reminders: scheduler job to DM users when bookmarked matches start (`src/watchlist_reminder.py`, scheduled in `src/scheduler.py`).

### Changed

- Internal: Added CRUD helpers for watchlist and async variants (`src/crud/watchlist.py`).
- Tests: Added unit tests for watchlist CRUD and catchup flows (`tests/test_watchlist.py`).

### Fixed

- Resolved an accidental package collision by consolidating models into `src/models.py` (removed stray `src/models/` package).


## [v1.3] - 2026-04-16

### Added

- Release consolidation: v1.2 and v1.3 combined; CS2 parser stabilized and merged into main (`src/parsers/cs2.py`).

### Changed

- Roadmap updated to reflect the combined v1.2/v1.3 release and CS2 availability.

## [v1.2] - 2026-03-11

### Added

- **Guild Configuration Model:** Persist per-guild settings for announcement and live-update channels (`GuildConfig`) and live message pointers (`LiveUpdateMessage`).
- **CS2 Parser:** Initial `CS2Parser` adapter to parse PandaScore CS2 payloads and wire into the parser factory (`src/parsers/cs2.py`, `src/parsers/factory.py`).
- **Parser Factory Registered:** The parser factory now supports `cs2` in addition to `lol` (`src/parsers/factory.py`).
- **Guild Configuration Commands:** New `/config` command group with `view`, `set_channel` and `set_games` to manage per-guild settings (permission-checked for guild owners/admins).
- **Per-guild enabled_games persisted:** `GuildConfig.enabled_games` can be set per guild (comma-separated slugs like `lol,cs2`) and is enforced when delivering notifications.
- **Tests:** Added unit tests covering the new config commands, CS2 parsing paths, and game-aware sync/polling flows.

### Changed

- **Game-aware defaults:** Added `DEFAULT_GAMES` config allowing deployments to select default game(s) to sync/poll (`src/config.py`). PandaScore sync uses the configured default game.
- **Game-aware sync & polling:** `perform_pandascore_sync` accepts a `game` parameter and sync jobs are scheduled per-entry in `DEFAULT_GAMES` (scheduler now creates per-game hourly sync jobs). Polling now fetches running matches for configured default games concurrently and de-duplicates results before processing.
- **CS2 wiring:** The CS2 parser is fully wired into sync/polling/notification flows so CS2 matches are parsed and delivered when enabled.
- **Notification delivery respects per-guild games:** The notification batcher and delivery flow skip guilds that have not enabled the game's slug in their `enabled_games` config.

- **Persistent Live Messages (per-guild, per-game):** Implemented three canonical live message slots per guild and game (`upcoming`, `running`, `results`). Notifications now edit those existing messages in-place when possible instead of posting new messages for each notification. Delivery falls back to announcement channels when editing isn't possible. (See `src/crud/live_update_message.py`, `src/notification_batcher.py`)

- **Announcements persistence:** The announce flow now persists admin announcement messages as a scoped live message (`scope_type="announcement"`) so the bot can reference or update admin announcements later (`src/commands/announce.py`).

### Fixed

- **Compatibility:** Fixed import-time compatibility for the `setup` command so command modules import cleanly in test environments.

## [v1.1] - 2026-01-24

### Added

- **Batch Notifications:** Simultaneous match notifications are now batched into single announcements to reduce channel spam (#202).
- **Match Time Updates:** Notifications are now sent when a match's scheduled time changes.
- **Paginated Pick View:** The `/pick` command has been refactored to use a paginated Embed View for a better user experience.

### Changed

- **Match Polling:** Refactored match polling logic for better reliability.

### Fixed

- **N+1 Queries:** Optimized database queries in pick commands to resolve performance issues (#202).

## [v1.0.2] - 2026-01-24

### Fixed

- **Pick Resolution:** Fixed an issue where picks were not resolving with the correct status and score.
- **Repair Job:** Added a background job to retroactively fix unresolved pick statuses (#201).

## [v1.0.1] - 2026-01-24

### Fixed

- **Migrations:** Made the `add_is_correct_to_pick_model` migration idempotent to prevent errors on existing databases (#188).
