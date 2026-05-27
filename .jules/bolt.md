## 2023-10-27 - Cache Matches in Pagination Views

**Learning:** Re-querying the database on every page turn in pagination views (e.g., `DayNavigationView`) creates redundant load and significantly impacts performance, especially if the dataset for the view doesn't change during pagination.
**Action:** When designing or optimizing pagination in `discord.ui.View` components, fetch the necessary results once and cache them within the View instance (e.g., `self.matches`). Reuse this cache for page navigation, and only query the database when the dataset boundaries change (e.g., navigating to a different day).
