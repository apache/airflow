<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# XCom conversion evidence

Use this example when a converted route returns a large JSON value or a long list, reaches the DagBag through `TaskCoordinateResolver`, or offloads a lookup that needs its own session. The measurements concern the six Execution API XCom routes, converted on branch `2610/async-xcoms` and measured against base `8cc830d4ef`. They are observations from that checkout, not current API guarantees or performance thresholds.

## Setup

Python 3.14.8 and SQLAlchemy 2.0.54 in the Breeze CI image. Contract tests ran on `sqlite+aiosqlite`, `postgresql+psycopg_async`, and `mysql+aiomysql`; stall samples on SQLite and PostgreSQL only. The harness followed "Measure event-loop availability". Revisions were compared by swapping only the route file in the mounted worktree and restoring it on exit. Cells are `median / max` warm stall in ms, run 1; run 2.

| Variant | Meaning |
|---|---|
| `run_sync` | Async handler; resolver helpers through `AsyncSession.run_sync`; FastAPI response validation |
| Offloaded | Resolver helpers in `run_in_threadpool` with their own session; `JSONResponse` built in `run_in_threadpool` |

## One 3.9 MB value: response validation

`GET /xcoms/...` returning one value of 100,000 small objects.

| Variant | PostgreSQL | SQLite |
|---|---|---|
| Synchronous base | 977 / 1085; 965 / 1028 | 930 / 943; 936 / 959 |
| `run_sync` | 1315 / 1712; 1156 / 1581 | 1847 / 2614; 1665 / 2255 |
| Offloaded | 165 / 553; 164 / 556 | 151 / 156; 157 / 175 |

The loop-thread profile of the `run_sync` variant on PostgreSQL spent 2.12 s: pydantic `validate_python` 1.09 s cumulative, with `pydantic/types.py:_get_type_name` called 1.2 million times, `SchemaSerializer.to_python` 0.67 s, and the driver's JSON decoding only 0.12 s. Returning a plain `Response` with pre-serialized JSON failed every GET contract test, because Cadwyn encoded the body a second time. The remaining offloaded stall is the driver decoding plus Cadwyn's decode and re-encode of the `JSONResponse` body.

## Cold DagBag: Dag deserialization

A loop-task producer read with a 1,000-task serialized Dag, clearing `app.state.dag_bag` before every request.

| Variant | PostgreSQL | SQLite |
|---|---|---|
| Synchronous base | 8.8 / 14.7; 11.0 / 17.6 | 10.6 / 13.2; 9.6 / 18.1 |
| `run_sync` | 78.6 / 105.3; 70.7 / 78.3 | 109.8 / 139.9; 104.6 / 130.9 |
| Offloaded | 10.6 / 14.2; 11.7 / 21.6 | 10.7 / 14.2; 11.2 / 13.7 |

The same read with a warm cache showed no `run_sync` regression (PostgreSQL 5.0 / 5.7; 6.1 / 9.3 against base 6.3 / 6.7; 6.3 / 7.2), so a warm-only protocol would have passed it. The offloaded warm read cost about 4 ms more median stall than the base on SQLite (10.2 / 11.7; 10.9 / 13.8 against 6.3 / 6.4; 6.2 / 8.6), the price of the thread hop, and none on PostgreSQL (5.4 / 7.9; 4.6 / 5.7).

## Slice of 5,000 values

| Variant | PostgreSQL | SQLite |
|---|---|---|
| Synchronous base | 52.7 / 67.5; 55.6 / 64.9 | 58.1 / 397.7; 34.7 / 450.4 |
| `run_sync` | 77.4 / 94.9; 71.2 / 76.0 | 104.3 / 138.9; 102.7 / 600.0 |
| Offloaded, `Row` access | 70.4 / 78.9; 56.8 / 76.2 | 46.3 / 59.3; 50.0 / 60.8 |
| Offloaded, `scalars()` | 35.8 / 51.0; 42.5 / 69.9 | 50.3 / 55.7; 50.4 / 55.6 |

## Sessions, transactions and guard tests

The offloaded lookups open `create_session(scoped=False)` on the worker thread, so each request uses two sessions, one from each pool. A test that counted distinct `after_begin` sessions across six trigger-header requests went from 6 to 12 and was updated with that reason. Writes resolve their target task instance in the lookup's transaction and write in the route's; the lookup path took no row locks, so the split released none early.

A `loaded_as_persistent` listener recorded one `XComModel` per read on the base and none after the value-column projection. A stored `NULL` value still returned 200 because the handler tests the fetched row, not `scalar()`. The off-loop test patched the offloaded helpers and asserted that no event loop ran in their thread; it failed against the `run_sync` variant, its failing control.
