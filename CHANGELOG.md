# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.2] - 2026-02-01

### Added
- **Text-Based Pick Commands:** New prefix commands for agent/bot accessibility:
  - `!pick list` - Show available matches with IDs
  - `!pick <match_id> <team>` - Submit or update a pick
  - `!picks` - View your current picks
- **Message Content Intent:** Enabled for prefix command parsing.

### Why
- Enables AI agents (like Nimbus) to interact with MatchPoint without requiring Discord interaction tokens that only work with slash commands.

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
