# MatchPoint Roadmap

## Overview

MatchPoint v1.0 is a production Discord bot for picks and contests (currently supports League of Legends).

This document is a concise, versioned roadmap describing refactors, feature milestones, priorities for additional titles, and release procedures.

## Goals

- Expand PandaScore-backed title support (CS2, Valorant, Rocket League, Dota2) using a reusable adapter template.
- Improve reliability: accurate rate-limit handling, feature flags, and stronger test coverage.
- Provide operational tooling (internal admin dashboard) and, later, a public-facing site for users.
- Defer advanced live/play-by-play features until feasibility, cost, and data-source options are evaluated.

## Versioning Policy

- Use a lightweight semantic scheme: `MAJOR.MINOR.PATCH`.

  - MINOR: add a supported title or a substantial feature (e.g., v1.1 = CS2).
  - PATCH: small command changes, bugfixes, or tests.
  - MAJOR: breaking changes.

## Milestones & Releases

Milestones are scoped for fast, incremental releases. Each milestone below maps to a target release.

### v1.0 — Baseline (released)

- Production LoL support: picks, contests, announcements

- Core scheduler, PandaScore ingestion, DB models, baseline tests

### v1.1 — Short-term UX & Reminders (released)

**Goal:** quick, high-impact UX improvements: 24-hour reminders and a bulk-friendly `/pick` redesign.

**Checklist:**

- [x] Add configurable 24-hour reminders (server opt-in, per-user opt-out) and scheduler integration.

- [x] Redesign `/pick` output for faster multi-match picking (compact, bulk-friendly UI using Discord components).

- [x] Audit primary command UX and apply small quick wins (consistent ordering, shorter messages, clearer help text).

- [x] Update docs/changelog and release notes for v1.1.

**Acceptance:** increased engagement via reminders and a more usable `/pick` flow; tests and docs updated.

**Estimate:** 1–3 weeks.

### v1.2/v1.3 — Refactor, Hardening & CS2 (Released)

**Goal:** provide a reusable adapter, harden polling, and deliver CS2 support.

**Checklist:**

- Split parsing into: core parsing helpers and per-game adapters (e.g., `parsers/core.py`, `parsers/lol.py`).

- Define a per-game parser interface and helper utilities.

- Replace inferred rate-limit counters with real reset/lift tracking from API responses; persist reset state as needed and add backoff/retry logic.

- Implement feature flags (config-driven per-title/feature toggles) for staged rollouts and quick rollback.

- Add fixtures and mocked PandaScore responses; extend unit/integration tests and CI to run them.

- Implement `parsers/cs2.py` and wire CS2 into sync/polling/notification flows; add per-title configuration and tests.

- Apply DB/Alembic migrations for map/series fields if required.

**Acceptance:** CS2 parser merged and enabled; no regressions; CI passing; documented rollback path.

**Estimate:** 2–4 weeks (combined).

### v1.4 — Fan Experience: Watchlist & Catchup (feature)

**Goal:** personal watchlist and spoiler-free catchups to increase engagement and reduce announcement noise.

**Checklist:**

- Implement `user_watchlist` DB table and `/watch` command for bookmarking matches or teams.

- Implement DM reminders when a bookmarked match is starting and an opt-in `reminder_channel` preference (DM vs channel).

- Implement `/catchup` command that lists finished bookmarked matches with scores hidden behind spoilers and a `Mark as Watched` action.

**Acceptance:** users can bookmark matches, receive DM start reminders, and view spoiler-free catchup lists.

**Estimate:** 2–4 weeks.

### v1.5 — Valorant

- Same pipeline as v1.3 using `parsers/valorant.py`.

### v1.6 — Subscriptions & Role-Based Pings (feature)

**Goal:** granular subscriptions (team/tournament/tier/region/country) and role-based notifications to reduce global spam.

**Checklist:**

- Implement subscriptions DB and a lightweight rules engine to match announcements to subscribers.

- Admin-facing role mapping (map a `league.slug` or `region` to a Discord Role for opt-in pings).

- Per-user preference for DM vs channel notifications and tier filters (e.g., S-tier only).

**Estimate:** 2–4 weeks.

### v1.7 — Rocket League

- Same pipeline as v1.3 using `parsers/rocketleague.py`.

### v1.8 — Stream & Engagement Features (feature)

**Goal:** improve announcements and in-chat engagement: smart stream selection, upset/highlight badges, real-time result DMs.

**Checklist:**

- Implement `streams_list` priority logic (official+en/main) and replace raw lists with a single `Watch Live` action + `All Streams` view.

- Add upset/highlight heuristics (tier-based badges, playoff color accents).

- Trigger instant result DMs to users who picked correctly or subscribed to a match for immediate feedback.

**Estimate:** 2–4 weeks.

### v1.9 — Dota2

- Same pipeline as v1.3 using `parsers/dota2.py` (Dota2 may require extra fields and validation).

### v2.0 — Admin Operations Dashboard (Milestone C)

**Goal:** internal ops tooling for visibility and recovery.

**Checklist:**

- Select lightweight stack (FastAPI/Flask + minimal frontend) and admin auth (Discord OAuth or local accounts).

- Views: match list/detail, job queue, logs, errors, feature-flag controls.

- Panels: PandaScore quota/usage, health checks, manual override and retry/cancel actions.

- Tests for key admin flows and documented runbook.

**Estimate:** 4–6 weeks (MVP).

### v3.0 — Public-Facing Site (Milestone D)

**Goal:** user-facing site with leaderboards, pick history, and account linking.

**Checklist (MVP):**

- Public match pages and leaderboards.

- Discord account linking and user settings.

- Pick history and basic exports.

- Opt-in notifications and privacy review (TOS/Privacy policy).

**Estimate:** 6–12 weeks (MVP depending on scope).

## Advanced / Deferred Items

- Live, play-by-play statistics and deep in-game analytics are deferred due to cost and data availability. Evaluate alternate providers, scraping legality, or funding before implementation.

- Monetization or premium tiers can be considered later to offset paid-data costs.

## Implementation Notes

- Rate limits: batch requests, cache results, prefetch around reset windows, and use exponential backoff.

- Parser pattern: keep a lean core and move only game-specific transforms into adapters.

- DB changes: create small, incremental Alembic migrations per title.

- Testing: use recorded API fixtures and mocks for CI to avoid hitting PandaScore directly.

- Observability: add health endpoints, structured logs, and optional Sentry/metrics during Milestone C.

## Release Procedure (template)

For each release:

- Update `pyproject.toml` or version source.

- Add changelog entry and release notes with acceptance criteria met.

- Run full test suite in CI (including parser fixtures).

- Tag release and open PR with release notes.

- Smoke test in staging if available before merging to `main`.

## Next Steps

1. Begin parser refactor for LoL and implement improved rate-limit tracking.
2. Start v1.2 (CS2) once v1.1 is validated in CI.
