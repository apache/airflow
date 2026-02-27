---
applyTo: "**"
excludeAgent: "coding-agent"
---

# Airflow Code Review Instructions

Use these rules when reviewing pull requests to the Apache Airflow repository.

## Architecture Boundaries

- **Scheduler must never run user code.** It only processes serialized Dags. Flag any scheduler-path code that deserializes or executes Dag/task code.
- **Workers must not access the metadata DB directly.** Task execution communicates with the API server through the Execution API (`/execution` endpoints) only.
- **Dag Processor and Triggerer run user code in isolated processes.** Code in these components should maintain that isolation.
- **Providers must not import core internals** like `SUPERVISOR_COMMS` or task-runner plumbing. Providers interact through the public SDK and execution API only.

## Database and Query Correctness

- **N+1 queries**: Flag SQLAlchemy queries that access relationships inside loops without `joinedload()` or `selectinload()`.
- **`run_id` is only unique per Dag.** Queries that group, partition, or join on `run_id` alone (without `dag_id`) will collide across Dags. Always require `(dag_id, run_id)` together.
- **Cross-database compatibility**: SQL changes must work on PostgreSQL, MySQL, and SQLite. Flag database-specific features (lateral joins, window functions) without cross-DB handling.
- **Session discipline**: In `airflow-core`, functions receiving a `session` parameter must not call `session.commit()`. Use keyword-only `session` parameters.

## Code Quality Rules

- No `assert` in production code (stripped in optimized Python).
- `time.monotonic()` for durations, not `time.time()`.
- Imports at top of file. Valid exceptions: circular imports, lazy loading for worker isolation, `TYPE_CHECKING` blocks.
- Guard heavy type-only imports (e.g., `kubernetes.client`) with `TYPE_CHECKING` in multi-process code paths.
- Unbounded caches are bugs: all `@lru_cache` must have `maxsize`.
- Resources (files, connections, sessions) must use context managers or `try/finally`.

## Testing Requirements

- New behavior requires tests covering success, failure, and edge cases.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Use `time_machine` for time-dependent tests.
- Imports belong at the top of test files, not inside test functions.
- Issue numbers do not belong in test docstrings.

## API Correctness

- `map_index` must be handled correctly for mapped tasks. Queries without `map_index` filtering may return arbitrary task instances.
- Execution API changes must follow Cadwyn versioning (CalVer format).

## UI Code (React/TypeScript)

- Avoid `useState + useEffect` to sync derived state. Use nullish coalescing or nullable override patterns instead.
- Extract shared logic into custom hooks rather than copy-pasting across components.

## AI-Generated Code Signals

Flag these patterns that indicate low-quality AI-generated contributions:

- **Fabricated diffs**: Changes to files or code paths that don't exist in the repository.
- **Unrelated files included**: Changes to files that have nothing to do with the stated purpose of the PR.
- **Description doesn't match code**: PR description describes something different from what the code actually does.
- **No evidence of testing**: Claims of fixes without test evidence, or author admitting they cannot run the test suite.
- **Over-engineered solutions**: Adding caching layers, complex locking, or benchmark scripts for problems that don't exist or are misunderstood.
- **Narrating comments**: Comments that restate what the next line does (e.g., `# Add the item to the list` before `list.append(item)`).
- **Empty PR descriptions**: PRs with just the template filled in and no actual description of the changes.

## Quality Signals to Check

The absence of these signals in a "fix" or "optimization" PR is itself a red flag:

- **Bug fixes need regression tests**: A test that fails without the fix and passes with it.
- **Existing tests must still pass without modification**: If existing tests need changes to pass, the PR may introduce a behavioral regression.
