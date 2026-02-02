## 2026-01-24 - Turning Dead Ends into Information
**Learning:** When a user hits an "empty state" (e.g., no matches to pick), a simple error message feels like a dead end. Providing context ("Next match in 5 days") and an alternative action ("Check leaderboard") transforms a negative experience into a helpful one.
**Action:** Always check if we can provide "what's next" or "what else" when a primary list is empty.

## 2026-02-06 - Visual Feedback for Stale States
**Learning:** In asynchronous interfaces like Discord, messages can become "stale" (e.g., a match starts while the pick menu is open). Relying on error messages after interaction ("Cannot pick: Match started") is frustrating. Proactively disabling UI elements and adding visual indicators (🔒 emoji) prevents the error loop entirely.
**Action:** When rendering interactive views that depend on time or state, always check the current state and render "disabled/locked" views if the window has passed, rather than just validating on submit.

## 2026-02-17 - Context-Aware UI Disabling
**Learning:** Users in different contexts (DMs vs Guilds) encounter buttons that only work in one context. Leaving them enabled but returning an error upon click is confusing and adds friction. Disabling them implicitly teaches the user about the feature's constraints and prevents the "error click."
**Action:** Check context (e.g., `interaction.guild`) in `View.__init__` and preemptively disable invalid options (like "Server" filters in DMs).
