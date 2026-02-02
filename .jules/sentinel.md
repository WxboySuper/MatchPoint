## 2026-02-02 - Missing Unique Constraint on Picks
**Vulnerability:** The `Pick` table allowed duplicate entries for the same user and match due to a missing unique constraint, enabling users to submit multiple picks via race conditions.
**Learning:** Checking for existence (`select().first()`) before creating (`insert()`) is insufficient for enforcing uniqueness in concurrent environments (Check-Then-Act race condition). Database-level constraints are required.
**Prevention:** Always define `UniqueConstraint` in SQLModel/SQLAlchemy models for logical uniqueness. Verify migrations enforce these constraints, especially when using SQLite which requires `batch_alter_table`.
