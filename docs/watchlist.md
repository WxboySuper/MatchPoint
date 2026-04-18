# Watchlist & Catchup (v1.4)

This feature provides a personal watchlist for users to bookmark matches or teams and a spoiler-free catchup flow.

Usage

- /watch add <match_id>  — Bookmark a match to receive a DM reminder when it is about to start.
- /watch remove <watch_id> — Remove a bookmark by its id (see /watch list to find watch ids).
- /watch list — List your active bookmarks (includes match time and watch id).
- /catchup now — Show finished bookmarked matches with scores hidden behind Discord spoilers. Each item includes a "Mark as Watched" interactive action to remove it from your unread list.

Developer notes

- DB: model `UserWatchlist` in `src/models.py`; migration `alembic/versions/d9f7a6b5c4e3_add_user_watchlist.py` creates the `user_watchlist` table.
- CRUD: helpers in `src/crud/watchlist.py` provide sync and async APIs used by commands and the reminder job.
- Reminders: background job `send_watchlist_reminders_job` in `src/watchlist_reminder.py` queries upcoming matches and DMs users; scheduler registration is in `src/scheduler.py`.
- Permissions: Reminders are DMed by default; server-channel reminders require `GuildConfig.reminder_channel` to be configured and a future enhancement to add channel opt-in.

Testing

- Unit tests for watchlist CRUD are in `tests/test_watchlist.py`.

Migration

- To apply the migration locally: `alembic upgrade head` (ensure `DATABASE_URL` points to the test DB). Verify table `user_watchlist` exists after migration.

Notes

- The watchlist stores the PandaScore match id by default. If a project-wide decision changes, update `src/crud/watchlist.py` and command parsing accordingly.
