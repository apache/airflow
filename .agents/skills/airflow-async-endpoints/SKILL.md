---
name: airflow-async-endpoints
description: Write or migrate Airflow FastAPI endpoints to async I/O, preserving API behavior, transaction ownership, and backend compatibility. Use for AsyncSessionDep adoption or auditing blocking work in an async route and its dependencies.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Write and migrate async endpoints

Use the current checkout as the source of truth. A migration should release the event loop while waiting for I/O and preserve the endpoint's observable contract. An `async def` declaration alone establishes neither property.

## Trace the request before editing

Read the route, router/app dependencies, authorization, domain helpers, and response serialization. Classify their I/O: native async, synchronous code dispatched by FastAPI, or synchronous code called directly on the event loop. FastAPI can offload a synchronous dependency; an ordinary sync function called inside `async def` is still a direct blocking call. Converting a handler to `async def` also moves its CPU work from a threadpool worker onto the event loop, and awaiting database I/O does not offload it: ORM result processing (hydrating entities and their eagerly loaded relationships), JSON decoding, serialized-Dag deserialization, and response validation all run on the loop and stall every other request it serves. A replacement that looks mechanical can regress this way, so measure it as described under "Measure event-loop availability".

Identify session ownership, lazy/deferred ORM attributes, external clients, secrets resolution, cache IPC, filesystem operations, and initialization on the request path. Choose a small route or cohesive batch whose complete path can be made safe. For new endpoints, also follow the target API's authorization and versioning rules.

For pure migrations retain method/path, parameter validation, response shape, ordering/count semantics, errors, permissions, team filters, and API-version behavior. Leave sibling routes alone unless a shared helper forces an adaptation.

## Use the existing database infrastructure

- Inspect `airflow-core/src/airflow/api_fastapi/common/db/common.py` and `airflow-core/src/airflow/utils/session.py`. Reuse `AsyncSessionDep` and `create_session_async`; do not build a parallel engine/session configuration. On shutdown, the API server lifespan in `api_fastapi/app.py` and the in-process Execution API transport call `settings.dispose_async_engine()`, which closes the pooled connections and keeps the engine and session factory usable. Routes and helpers must not create or dispose engines.
- Await I/O such as `execute`, `scalar`, `scalars`, `flush`, and `refresh`. Buffered results are consumed synchronously, for example `(await session.scalars(statement)).all()`. Operations such as `add` and `expunge` are not coroutines. Streaming results have different consumption rules.
- Keep transaction ownership at the existing boundary. `AsyncSessionDep` is function-scoped so commit failure reaches the client before success is sent. Do not add route-level commits. A helper receiving a session must not commit, close it, or create a second transaction to perform the same work.
- Preserve locks, update predicates, flush ordering, rowcount branches, and rollback behavior. SQLite cannot validate production row-lock semantics. Do not run concurrent operations on one `AsyncSession`.
- Audit each query's projection as described below, including queries inside reused helpers. Materialize response data before its owning session ends; treat streaming responses separately because function-scoped teardown precedes body consumption.
- Make shared decision helpers session-independent where practical: let each caller execute its own sync/async query, then pass plain data. The heartbeat history check in `execution_api/routes/task_instances.py` demonstrates this. Where a sync helper only executes a statement built by a separate builder, call the builder and await its statement instead of adapting the helper: `SerializedDagModel.get(dag_id)` only executes `SerializedDagModel.latest_item_select_object(dag_id)`, so an async caller awaits `session.scalar(SerializedDagModel.latest_item_select_object(dag_id))` and passes the row to the synchronous, session-free `DBDagBag._read_dag`.

### Audit individual query projections

Follow every `SELECT` reached by the route, including dependencies and backend helpers. For each query, identify the selected fields, the fields its consumers actually read, mapper-added eager relationships, and expected row count. Include fields used for authorization, errors, and helper decisions, not just the response. Record the proposed projection or why the full entity is required.

When consumers need only values, select those columns directly rather than an ORM entity. A one-value lookup can use `session.scalar(select(Model.value).where(...))`; calling `scalar(select(Model))` still hydrates the entire entity. Preserve predicates, ordering, locks, and the distinction between a missing row and a nullable value: `scalar()` returns `None` for both, so when the column is nullable, fetch `(await session.execute(statement)).first()` and test the row. Keep shared callers that need model methods or relationships working; narrow a shared query only when its complete caller contract permits it.

Inspect mapper loading too: `select(TaskInstance)` also loads its joined `dag_run`. Column projections avoid both unused database fields and entity hydration; they also prevent response access from triggering lazy SQL. `load_only` still constructs entities and is useful only when consumers actually need them.

Replacing `session.get(TaskInstance, id)` with a column select changes which attempts are visible: primary-key lookups bypass the session hook that hides archived attempts, and a select filtered on `TaskInstance.id` does not (see the `TaskInstance` docstring). Write the intended `working_set` predicate into the query rather than relying on either behavior.

Apply this audit even to one-row getters and pre-existing async helpers newly used by the conversion. Report the concrete fields avoided; measure before claiming a material latency or event-loop improvement. For an observed example, see [the asset-state-store review](references/asset-state-store-review.md).

## Handle synchronous boundaries explicitly

Do not pass an `AsyncSession` to `@provide_session` or synchronous model methods. The call does not fail at the call site: a body calling `session.scalar(...)` gets back an un-awaited coroutine, and one using `.query` or `.scalars(...).all()` raises `AttributeError`.

Choose the adaptation for each synchronous helper on an async route by what the helper does:

- **Executes a statement from a separate builder:** await the builder's statement on the route's session, as described under session-independent helpers above.
- **Only SQL, called by sync code too:** keep one implementation and call it through `AsyncSession.run_sync`, which passes a synchronous `Session` as the first positional argument. Most repository helpers take `session` as keyword-only, so wrap the call: `await session.run_sync(lambda s: helper(arg, session=s))`. Database waits then yield the event loop. Re-check the helper for non-SQL I/O on every conversion that relies on it.
- **Only SQL, no sync callers, or being restructured anyway:** write an async version, prefixed `a` as in `ExecutionAPISecretsBackend.aget_variable`, that takes an `AsyncSession` and awaits its queries.
- **Blocking network, filesystem, or secrets-backend I/O without an async API, or CPU work such as Dag deserialization on a DagBag cache miss:** offload the complete sync unit with `starlette.concurrency.run_in_threadpool`, as `auth/middlewares/team_authorization.py` does, and keep its session/client ownership within that unit: the unit opens `create_session(scoped=False)` itself rather than taking a `session` parameter. State in the PR that this is a compatibility bridge: it still consumes worker capacity. `AsyncSession.run_sync` is not a thread offload; it runs its function on the event loop.

  An offloaded unit with its own session checks out a connection from the sync pool, possibly while the route's `AsyncSession` holds one from the async pool, and runs in a separate transaction. Before splitting a lookup from the write that depends on it, search the offloaded path for `with_for_update`, `skip_locked`, and `nowait`: a lock taken there is released before the write, so keep that unit on `run_sync`. Report both effects in the PR.

Never share a session across the thread boundary or hide a broken async DB configuration by silently retrying with a sync session.

Code that receives a session from its caller must use it: pass the route's `AsyncSessionDep` session to every helper that accepts one. Do not add a create-a-session-if-omitted fallback, including an async counterpart of `provide_session`, to helpers whose callers hold a session: a forgotten `session=` silently checks out a second connection and can starve the pool. A fallback is acceptable only where the calling interface cannot carry a session, such as a secrets-backend method with the `get_variable(key, team_name)` signature of `BaseSecretsBackend`. Even there, accept an optional keyword-only `session`, pass it whenever the caller has one, and add the fallback in the PR that introduces its first caller.

Variable/Connection value resolution is a separate integration from key listing. Trace the backend chain and inspect current async capabilities before choosing a conversion. The SDK already has async connection dispatch and `ExecutionAPISecretsBackend.aget_connection/aget_variable`; that does not make the core model resolvers or every provider async. Preserve backend precedence, legacy method overrides, team propagation, cache semantics, and terminal access denials. A synchronous custom backend must not run directly on the event loop.

## Prove the conversion

Run existing HTTP contract tests before and after the change, including supported Execution API versions. Preserve assertions; adapt async session mocks and fixtures where needed instead of requiring test source to remain byte-identical. Use spec/autospec and awaited mocks for coroutine methods, normal mocks for buffered result accessors. Add regressions only for behavior introduced or endangered by this conversion, including failure paths; reuse existing coverage for old behavior. An awaited-call assertion (`mock.patch.object(AsyncSession, "scalar", autospec=True)` plus `assert_awaited_once()`) adds signal only for a route whose handler calls the session directly; a route already exercised by DB-backed contract tests gains coupling, not coverage, from it.

Check that fixtures commit seed data visible to the async connection.

Router-level and Cadwyn re-solved dependencies that still use `SessionDep` open a second session per request. An `after_begin` listener on the `Session` class also fires for `AsyncSession`, so counting distinct sessions across N requests shows whether the dependencies and the handler share the route's session.

Match engine and event-loop lifetimes. The async engine's pool binds each connection to the event loop that opened it. The harness builds a fresh `TestClient` loop per test and `asyncio_default_fixture_loop_scope` is `function`, so a pooled connection reused across tests fails with "attached to a different loop" on every backend, including SQLite (`aiosqlite` file databases use `AsyncAdaptedQueuePool`).

In `tests/unit/api_fastapi/execution_api/conftest.py`, the `async_db_engine` fixture builds a fresh engine per test with `_configure_async_session()` and restores the previous `settings` globals afterwards. The `client` fixture depends on it and disposes that engine through `client.portal` while its loop is still alive. Tests that use `client` need no per-class handling. Do not add `reconfigure_async_db_engine` fixtures, `usefixtures` tags, or `_configure_async_session()` calls to Execution API tests: `client` disposes only the engine `async_db_engine` created, so an engine rebound inside the test keeps its pooled connections after teardown. A pytest-asyncio test in that directory that opens `settings.AsyncSession()` itself should request `async_db_engine` and `await async_db_engine.dispose()` before it returns; the next test gets a new engine either way, but skipping the dispose drops open connections at garbage collection.

Any `with TestClient(app)` runs the API server lifespan, which closes the async engine's pooled connections on the client's loop at exit. The core API fixtures in `tests/unit/api_fastapi/conftest.py` enter `TestClient` as a context manager, so core API tests need no reconfigure fixture either. A `TestClient(app)` used without `with` skips the lifespan and its disposal. A pytest-asyncio test outside these fixtures that opens `settings.AsyncSession()` itself should call `await settings.dispose_async_engine()` on its own loop before returning, as `tests/unit/utils/test_session.py` does; the engine stays usable, so do not reconfigure it afterwards. A global loop-scope redesign is separate work.

`_configure_async_session()` rebinds the module globals `settings.async_engine` and `settings.AsyncSession`. Read them through the module object (`from airflow import settings` then `settings.AsyncSession()`), or import inside the function after the call. `from airflow.settings import AsyncSession` at test or module top binds the stale object and silently keeps using the previous engine. Importing the function `_configure_async_session` by name is fine.

Observe client status with server-exception reraising disabled when testing commit-before-response behavior.

Exercise DB-dependent conversions through Breeze on SQLite, PostgreSQL, and MySQL using the current configured drivers. Inspect `settings.py` and the database setup guide rather than hardcoding an old asyncpg default. Record backend, driver, test command, and result; an unavailable backend is unverified, not a pass. Where the change affects SQL/driver or connection configuration, also cover supported overrides and the relevant PgBouncer mode. Apply the repository's Ruff, mypy, and prek requirements to the changed surface.

For migrations, API version/schema generation is unnecessary when the HTTP contract is unchanged. If a contract change becomes necessary, separate that decision and follow `execution_api/AGENTS.md` and the versioning guide.

### Measure event-loop availability

Measure every converted route whose work grows with data: rows per Dag run, mapped tasks, XComs, serialized Dag size, or bytes/elements in a single JSON value. One database row can still require substantial decoding and validation. Run the same harness against the base revision and the conversion. The regression to catch is how long the route blocks the event loop, which can worsen while the request itself gets faster, so request duration alone does not reveal it.

Before collecting samples, verify the interpreter and database drivers inside the container; a requested Breeze version can fall back to another runtime. Exercise a request and remove unrelated fixture errors from both revisions. For example, when auth is independently overridden, a fake bearer header can still trigger repeated token-refresh exceptions in middleware. Keep the intended auth coverage explicit.

1. Write the probe as a test function in a temporary `dev/` script. The script calls `pytest.main` on an existing test of the route's module, passing a plugin whose `pytest_collection_modifyitems` swaps the collected item for the probe, so the probe gets that directory's `dag_maker`, `session`, and `client` fixtures. Anchor on a module-level test and replace the items with `pytest.Function.from_parent(anchor.parent, name=..., callobj=probe)`, where `probe` is a closure whose signature names the fixtures it needs; pytest resolves fixtures from that signature, and a collected bound method swapped for a free function would not receive `self`. Run it with `breeze run python dev/<harness>.py`. An untracked file in `dev/` counts as a changed CI-tooling file, so move the harness out of the checkout before `breeze verify`, prek, or a commit. Seed realistic volume and commit it; for task-instance routes, use one Dag run with 1,000 and with 5,000 mapped instances.
2. In a coroutine run with `client.portal.call(...)`, start a ticker task that loops on `await asyncio.sleep(0.001)` and records each sleep's own duration with `time.monotonic()`. The maximum is the stall. Timing each sleep, rather than the gaps between recorded ticks, also captures a stall that ends as the request does.
3. Send the real HTTP request from a worker thread: `await asyncio.to_thread(client.get, url, params=...)`. Calling `client.get` directly in the coroutine blocks the loop being measured, and going through HTTP includes dependencies, validation, and serialization.
4. Assert the response status and the number of returned items in the harness, so an error response cannot pass as a fast one.
5. Send at least ten sequential requests per size, drop the first, which also pays for cold caches, and run each revision twice; medians can differ twofold between runs. Report the median and the maximum of the warm stalls. When the path can reach the DagBag (`TaskCoordinateResolver`, `get_dag*`, `DBDagBag`), also measure a cold scenario that clears `app.state.dag_bag` before every request: a cache miss deserializes the Dag, and under `run_sync` it does so on the loop, a regression no warm sample shows.
6. To locate the cost, profile the loop thread: enable a `cProfile.Profile()` inside the portal coroutine around the request and sort by `tottime`. It separates the handler's own code, ORM result processing, and response serialization. Try each candidate fix in a disposable worktree with the same harness before proposing it. Collect profiles on separate requests excluded from timing samples, with the same per-request setup, such as clearing the cache, as the scenario being explained. A loop-thread profile excludes the synchronous base handler's worker-thread CPU: it locates on-loop work, not a change in total CPU consumption.

Report, per revision and size: returned items, warm median and maximum stall, warm request duration, backend, driver, and the command. These are small local samples; present them as event-loop availability, not throughput.

The bar is the base revision measured in the same session. The synchronous base stalls the loop too, while its threadpool worker holds the GIL, and occasionally by hundreds of milliseconds, so the bar is not zero. Expect a small fixed increase at every size: async statement execution and result handling now run on the loop instead of in a worker thread, about 1 ms per request on SQLite and PostgreSQL in the task-state-store conversion. No step below removes it; report it. Beyond that fixed increase, the conversion's warm median and maximum stall must not exceed the base's at any size. Until the conversion meets the bar, choose the applicable steps below in order and re-measure after each; row-processing remedies do not address a single large JSON value:

1. Select only the columns the response uses.
2. Unpack rows as tuples (`.tuples()`), or use `scalars()` for a single column, and build the response directly; per-row `Row` attribute access and temporary containers add up across thousands of rows.
3. For results that grow with data, stream them and process each partition as it arrives, so the loop regains control between partitions: `result = await session.stream(statement.execution_options(yield_per=500))`, then `async for partition in result.tuples().partitions()`. This uses a server-side cursor, so consume it fully before the next statement on the session, run the contract tests on PostgreSQL and MySQL, and repeat the stall measurement on PostgreSQL, since each partition adds a database round trip.
4. Offload remaining CPU work with `run_in_threadpool` on detached data. For JSON reads, move decoding and response-model construction together: moving only `json.loads` leaves recursive model validation on the loop. Framework response validation and serialization can still dominate: FastAPI runs them in the threadpool for a synchronous handler but on the loop for an async one, and validating a `JsonValue` visits every element. When the data is already valid JSON, as a JSON column's value is, return a `JSONResponse` built in `run_in_threadpool` and keep `response_model=` in the route decorator; FastAPI then skips response validation and the OpenAPI schema stays the same. Compare `/openapi.json?version=<v>` for every API version, and confirm no `convert_response_to_previous_version_for` targets the model. Cadwyn decodes and re-encodes a returned `JSONResponse` body on the loop (cadwyn#51), which costs far less than validation, and encodes a plain `Response` with an `application/json` content type a second time, so return a `JSONResponse`. See [the XCom measurements](references/xcom-conversion.md).

Large request bodies have no remedy in the handler. In the task-state-store conversion, PUT stalls grew with body size while loop-thread CPU stayed the same as the base: FastAPI and cadwyn validate the body on the loop in both revisions, and the async route probably loses the thread hops that split that work in the base. Offloading `json.dumps` and adding a yield at handler entry did not bring the stall back to base. See [the task-state-store measurements](references/task-state-store-conversion.md).

If the conversion still exceeds the base after the applicable steps, state the measurements in the PR and leave the decision to reviewers.

Guard the mechanism with a deterministic test. For an entity projection, record loaded entities with a SQLAlchemy `load` listener on the entity the route stopped hydrating, or with `loaded_as_persistent` on the `Session` class, which also fires for `AsyncSession` loads. Clear it after seeding and assert it recorded nothing during the request, covering each affected path. Prove each guard by undoing only its projection on the conversion and watching the test fail: on the base revision, another hydration on the same path can fail the test and hide an unguarded projection. Capture selected columns when field width is the concern. For CPU offload, verify the complete parsing/model-building unit runs off the loop thread and retain HTTP result assertions. That test passes on the base too, whose synchronous handler already ran in a worker thread; its failing control is the async handler without the offload, so say so in the PR. Do not assert on timings; they are unreliable on shared CI. Leave the harness out of the PR and attach its output.

## Keep claims and scope defensible

Describe improved scheduling of I/O separately from measured performance. Driver microbenchmarks and synthetic sleep routes do not establish production throughput or explain an inversion without profiling. Report both sync and async pool limits, overflow, worker/replica counts, and other DB users when evaluating capacity; check actual configuration before prescribing pool changes.

Keep experiment routers and harnesses out of a production endpoint patch unless explicitly part of the requested deliverable. Follow the contribution skills for release notes, commit text, and publication; this skill adds no publication authority.

## Evidence and maintained references

- Reference conversion: commit `45bad9f54ddb186ba75f7062a76c0770cc125f29`, [heartbeat PR](https://github.com/apache/airflow/pull/67800).
- [Commit-before-response review](https://github.com/apache/airflow/pull/67800#discussion_r3359872625) and [test-loop lifetime discussion](https://github.com/apache/airflow/pull/67800#discussion_r3348412912).
- [Migration tracker](https://github.com/apache/airflow/issues/67799).
- [Event-loop stall from a full-entity select](https://github.com/apache/airflow/pull/73966#pullrequestreview-5376699657): harness design and base/head/projection measurements for `/task-instances/states`.
- Test loop-lifetime handling: the `async_db_engine` fixture and `client` disposal from [#73554](https://github.com/apache/airflow/pull/73554) (`tests/unit/api_fastapi/execution_api/conftest.py`) supersede the per-class reconfigure fixtures in [#67800](https://github.com/apache/airflow/pull/67800), [#73403](https://github.com/apache/airflow/pull/73403) and [#73407](https://github.com/apache/airflow/pull/73407); verified on SQLite, PostgreSQL and MySQL via Breeze.
- Async pool disposal at API server and in-process Execution API shutdown through `settings.dispose_async_engine()`, and the core API fixture changes: [#73838](https://github.com/apache/airflow/pull/73838).
- [Historical async proposal](https://github.com/apache/airflow/pull/36504): useful motivation and compatibility questions, not an Airflow 3 implementation template.
- Current driver/configuration guidance: `airflow-core/docs/howto/set-up-database.rst` and `airflow-core/src/airflow/settings.py`.
