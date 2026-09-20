---
name: airflow-async-endpoints
description: Write or migrate Airflow FastAPI endpoints to async I/O, preserving API behavior, transaction ownership, and backend compatibility. Use for AsyncSessionDep adoption or auditing blocking work in an async route and its dependencies.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Write and migrate async endpoints

Use the current checkout as the source of truth. A migration should release the
event loop while waiting for I/O and preserve the endpoint's observable contract.
An `async def` declaration alone establishes neither property.

## Trace the request before editing

Read the route, router/app dependencies, authorization, domain helpers, and response
serialization. Classify their I/O: native async, synchronous code dispatched by
FastAPI, or synchronous code called directly on the event loop. FastAPI can offload
a synchronous dependency; an ordinary sync function called inside `async def` is
still a direct blocking call.

Identify session ownership, lazy/deferred ORM attributes, external clients, secrets
resolution, cache IPC, filesystem operations, and initialization on the request
path. Choose a small route or cohesive batch whose complete path can be made safe.
For new endpoints, also follow the target API's authorization and versioning rules.

For pure migrations retain method/path, parameter validation, response shape,
ordering/count semantics, errors, permissions, team filters, and API-version
behavior. Leave sibling routes alone unless a shared helper forces an adaptation.

## Use the existing database infrastructure

- Inspect `airflow-core/src/airflow/api_fastapi/common/db/common.py` and
  `airflow-core/src/airflow/utils/session.py`. Reuse `AsyncSessionDep` and
  `create_session_async`; do not build a parallel engine/session configuration.
- Await I/O such as `execute`, `scalar`, `scalars`, `flush`, and `refresh`.
  Buffered results are consumed synchronously, for example
  `(await session.scalars(statement)).all()`. Operations such as `add` and
  `expunge` are not coroutines. Streaming results have different consumption rules.
- Keep transaction ownership at the existing boundary. `AsyncSessionDep` is
  function-scoped so commit failure reaches the client before success is sent.
  Do not add route-level commits. A helper receiving a session must not commit,
  close it, or create a second transaction to perform the same work.
- Preserve locks, update predicates, flush ordering, rowcount branches, and rollback
  behavior. SQLite cannot validate production row-lock semantics. Do not run
  concurrent operations on one `AsyncSession`.
- Prefer explicit columns or eager loading so validation and serialization cannot
  trigger implicit SQL. Materialize response data before its owning session ends;
  treat streaming responses separately because function-scoped teardown precedes
  body consumption.
- Make shared decision helpers session-independent where practical: let each
  caller execute its own sync/async query, then pass plain data. The heartbeat
  history check in `execution_api/routes/task_instances.py` demonstrates this.

## Handle synchronous boundaries explicitly

Do not pass an `AsyncSession` to `@provide_session` or synchronous model methods.
`AsyncSession.run_sync` adapts SQLAlchemy work through a greenlet; it does not make
arbitrary network, filesystem, or secrets-backend calls nonblocking.

Use native async I/O when available. For legacy code, offload a complete sync unit
with the project's existing thread-offload facilities and keep its session/client
ownership within that unit. State when this is a compatibility bridge: it still
consumes worker capacity. Never share a session across the thread boundary or hide
a broken async DB configuration by silently retrying with a sync session.

Variable/Connection value resolution is a separate integration from key listing.
Trace the backend chain and inspect current async capabilities before choosing a
conversion. The SDK already has async connection dispatch and
`ExecutionAPISecretsBackend.aget_connection/aget_variable`; that does not make
the core model resolvers or every provider async. Preserve backend precedence,
legacy method overrides, team propagation, cache semantics, and terminal access
denials. A synchronous custom backend must not run directly on the event loop.

## Prove the conversion

Run existing HTTP contract tests before and after the change, including supported
Execution API versions. Preserve assertions; adapt async session mocks and fixtures
where needed instead of requiring test source to remain byte-identical. Use
spec/autospec and awaited mocks for coroutine methods, normal mocks for buffered
result accessors. Add regressions only for behavior introduced or endangered by
this conversion, including failure paths; reuse existing coverage for old behavior.
An awaited-call assertion (`mock.patch.object(AsyncSession, "scalar", autospec=True)`
plus `assert_awaited_once()`) adds signal only for a route whose handler calls the
session directly; a route already exercised by DB-backed contract tests gains
coupling, not coverage, from it.

Check that fixtures commit seed data visible to the async connection.

Match engine and event-loop lifetimes. The async engine's pool binds each connection
to the event loop that opened it. The harness builds a fresh `TestClient` loop per
test and `asyncio_default_fixture_loop_scope` is `function`, so a pooled connection
reused across tests fails with "attached to a different loop" on every backend,
including SQLite (`aiosqlite` file databases use `AsyncAdaptedQueuePool`). The
Execution API `client` fixture in `tests/unit/api_fastapi/execution_api/conftest.py`
disposes `settings.async_engine` through `client.portal` at teardown, while its loop
is still alive; tests that use this fixture need no per-class handling. Do not add
`reconfigure_async_db_engine` fixtures or `usefixtures` tags to Execution API test
classes: `_configure_async_session()` rebinds the engine without disposing the
previous one and leaks a pool per call. Outside that fixture (core API
`test_client` yields a bare `TestClient` with no portal; pytest-asyncio tests that
open `settings.AsyncSession()` themselves) reconfigure is still the available
workaround: bound it to the affected tests and dispose the engine on the test's own
loop before it closes. A global loop-scope redesign is separate work.

`_configure_async_session()` rebinds the module globals `settings.async_engine` and
`settings.AsyncSession`. Read them through the module object
(`from airflow import settings` then `settings.AsyncSession()`), or import inside the
function after the call. `from airflow.settings import AsyncSession` at test or module
top binds the stale object and silently keeps using the previous engine. Importing
the function `_configure_async_session` by name is fine.

Observe client status with server-exception reraising disabled when testing
commit-before-response behavior.

Exercise DB-dependent conversions through Breeze on SQLite, PostgreSQL, and MySQL
using the current configured drivers. Inspect `settings.py` and the database setup
guide rather than hardcoding an old asyncpg default. Record backend, driver, test
command, and result; an unavailable backend is unverified, not a pass. Where the
change affects SQL/driver or connection configuration, also cover supported
overrides and the relevant PgBouncer mode. Apply the repository's Ruff, mypy, and
prek requirements to the changed surface.

For migrations, API version/schema generation is unnecessary when the HTTP contract
is unchanged. If a contract change becomes necessary, separate that decision and
follow `execution_api/AGENTS.md` and the versioning guide.

## Keep claims and scope defensible

Describe improved scheduling of I/O separately from measured performance. Driver
microbenchmarks and synthetic sleep routes do not establish production throughput
or explain an inversion without profiling. Report both sync and async pool limits,
overflow, worker/replica counts, and other DB users when evaluating capacity; check
actual configuration before prescribing pool changes.

Keep experiment routers and harnesses out of a production endpoint patch unless
explicitly part of the requested deliverable. Follow the contribution skills for
release notes, commit text, and publication; this skill adds no publication authority.

## Evidence and maintained references

- Reference conversion: commit `45bad9f54ddb186ba75f7062a76c0770cc125f29`,
  [heartbeat PR](https://github.com/apache/airflow/pull/67800).
- [Commit-before-response review](https://github.com/apache/airflow/pull/67800#discussion_r3359872625)
  and [test-loop lifetime discussion](https://github.com/apache/airflow/pull/67800#discussion_r3348412912).
- [Migration tracker](https://github.com/apache/airflow/issues/67799).
- Test loop-lifetime handling: the Execution API `client` fixture disposal
  (`tests/unit/api_fastapi/execution_api/conftest.py`) supersedes the per-class
  reconfigure fixtures in [#67800](https://github.com/apache/airflow/pull/67800),
  [#73403](https://github.com/apache/airflow/pull/73403) and
  [#73407](https://github.com/apache/airflow/pull/73407); verified on SQLite,
  PostgreSQL and MySQL via Breeze.
- [Historical async proposal](https://github.com/apache/airflow/pull/36504): useful
  motivation and compatibility questions, not an Airflow 3 implementation template.
- Current driver/configuration guidance:
  `airflow-core/docs/howto/set-up-database.rst` and `airflow-core/src/airflow/settings.py`.
