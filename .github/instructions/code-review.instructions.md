---
applyTo: "**"
excludeAgent: "coding-agent"
---

# Airflow Code Review Instructions

Use these rules when reviewing pull requests to the Apache Airflow repository.

## Architecture Boundaries

- **Scheduler must never run user code.** It only processes serialized Dags. Flag any scheduler-path code that deserializes or executes Dag/task code.
- **Flag any task execution code that accesses the metadata DB directly** instead of through the Execution API (`/execution` endpoints).
- **Flag any code in Dag Processor or Triggerer that breaks process isolation** — these components run user code in isolated processes.
- **Flag any provider importing core internals** like `SUPERVISOR_COMMS` or task-runner plumbing. Providers interact through the public SDK and execution API only.

## Database and Query Correctness

- **Flag any SQLAlchemy relationship access inside a loop** without `joinedload()` or `selectinload()` — this is an N+1 query.
- **Flag any query on `run_id` without `dag_id`.** `run_id` is only unique per Dag. Queries that filter, group, partition, or join on `run_id` alone will silently collide across Dags.
- **Flag any `session.commit()` call in `airflow-core`** code that receives a `session` parameter. Session lifecycle is managed by the caller, not the callee.
- **Flag any `session` parameter that is not keyword-only** (`*, session`) in `airflow-core`.
- **Flag any database-specific SQL** (e.g., `LATERAL` joins, PostgreSQL-only functions, MySQL-only syntax) without cross-DB handling. SQL must work on PostgreSQL, MySQL, and SQLite.

## Code Quality Rules

- **Flag any `assert` in non-test code.** `assert` is stripped in optimized Python (`python -O`), making it a silent no-op in production.
- **Flag any `time.time()` used for measuring durations.** Use `time.monotonic()` instead — `time.time()` is affected by system clock adjustments.
- **Flag any `from` or `import` statement inside a function or method body.** Imports must be at the top of the file. The only valid exceptions are: (1) circular import avoidance, (2) lazy loading for worker isolation, (3) `TYPE_CHECKING` blocks. If the import is inside a function, ask the author to justify why it cannot be at module level.
- **Flag any `@lru_cache(maxsize=None)`.** This creates an unbounded cache — every unique argument set is cached forever. Note: `@lru_cache()` without arguments defaults to `maxsize=128` and is fine.
- **Flag any heavy import** (e.g., `kubernetes.client`) in multi-process code paths that is not behind a `TYPE_CHECKING` guard.
- **Flag any file, connection, or session opened without a context manager or `try/finally`.**

## Testing Requirements

- **Flag any new public method or behavior without corresponding tests.** Tests must cover success, failure, and edge cases.
- **Flag any `unittest.TestCase` subclass.** Use pytest patterns instead.
- **Flag any `mock.Mock()` or `mock.MagicMock()` without `spec` or `autospec`.** Unspec'd mocks silently accept any attribute access, hiding real bugs.
- **Flag any `time.sleep` or `datetime.now()` in tests.** Use `time_machine` for time-dependent tests.
- **Flag any issue number in test docstrings** (e.g., `"""Fix for #12345"""`) — test names should describe behavior, not track tickets.

## API Correctness

- **Flag any query on mapped task instances that does not filter on `map_index`.** Without it, queries may return arbitrary instances from the mapped set.
- **Flag any Execution API change without a Cadwyn version migration** (CalVer format).

## UI Code (React/TypeScript)

- Avoid `useState + useEffect` to sync derived state. Use nullish coalescing or nullable override patterns instead.
- Extract shared logic into custom hooks rather than copy-pasting across components.

## Generated Files

- **Flag any manual edits to files in `openapi-gen/`** or Task SDK generated models. These must be regenerated, not hand-edited.

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

- **For bug-fix PRs, flag if there is no regression test** — a test that fails without the fix and passes with it.
- **Flag any existing test modified to accommodate new behavior** — this may indicate a behavioral regression rather than a genuine fix.
