## 2025-05-12 - Stats Calculation Performance
**Learning:** The application was fetching all `Pick` objects for a user (potentially thousands) just to count how many were "correct". This is a classic N+1-like issue where data transfer and object hydration overhead dominate.
**Action:** Always use SQL aggregation (`count()`, `sum()`) for statistics instead of fetching objects to application memory. In this case, it yielded a ~10x speedup for 1000 records.

## 2025-02-27 - N+1 Query in Match Result Updates
**Learning:** Iterating over ORM model objects and updating them individually within a session creates an N+1 query pattern during the commit phase, severely degrading performance as the dataset scales (e.g., scoring 1000 match picks).
**Action:** Replace iterative updates with bulk `UPDATE` operations using SQLAlchemy's `update()` construct (e.g., `session.exec(update(Pick).where(...).values(...))`). Furthermore, when unit testing these bulk operations, make sure to explicitly mock the `rowcount` property on the `session.exec()` return value to prevent mock objects from leaking into formatted output strings.
