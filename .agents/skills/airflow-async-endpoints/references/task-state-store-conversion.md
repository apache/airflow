<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Task-state-store conversion evidence

Use this example when a converted route accepts or returns a large JSON value, or when deciding how far to chase a stall that stays above the base. The measurements concern the four Execution API task-state-store routes, converted in commit `b265496e2c` and measured as patches on `8cc830d4ef` against that base. They are observations from that checkout, not current API guarantees or performance thresholds.

## Setup

Python 3.14.8 and SQLAlchemy 2.0.54 in the Breeze CI image. Drivers: `sqlite+aiosqlite`, `postgresql+psycopg_async`, and `mysql+aiomysql` for the contract tests; stall samples on SQLite and PostgreSQL only. The harness followed "Measure event-loop availability" with integer-array values. Each version ran twice, interleaved, in one container: the revisions were applied as patches with `patch -p1` between runs, because Breeze container start-up took longer than all the samples together. Cells are `median / max` warm stall in ms, round 1; round 2.

## GET: decoding and model building

| PostgreSQL, 688,890-byte value | Stall |
|---|---|
| Synchronous base | 143 / 153; 147 / 155 |
| Async conversion | 206 / 256; 199 / 232 |
| Decoding and model construction offloaded together | 152 / 171; 127 / 153 |

SQLite showed the same order (base 106 / 144; 119 / 200, async 144 / 217; 144 / 158, offloaded 123 / 145; 102 / 139). The offload removed the route's own model validation from the loop profile; response serialization stayed on the loop in every version, base included.

## PUT: request-body validation

| PostgreSQL, 688,890-byte body | Stall |
|---|---|
| Synchronous base | 79 / 91; 101 / 120 |
| Async conversion | 141 / 172; 145 / 183 |
| `await asyncio.sleep(0)` at handler entry | 124 / 145; 134 / 165 |

An earlier PostgreSQL round measured base 90 / 104; 86 / 95 and async 133 / 161; 121 / 170, and offloading `json.dumps` gave 128 / 165; 125 / 138. `json.dumps` cost about 7 ms of the request.

The loop-thread profiles of all versions contain the same work: three pydantic validations of the `JsonValue` body (about 300,000 `_get_type_name` calls for 100,000 elements) and one cadwyn `model_dump`. The mechanism below is read from the code, not instrumented. Cadwyn's request migration (`cadwyn/structure/versions.py`, `_migrate_request`) runs FastAPI's `solve_dependencies` again. With the synchronous `SessionDep`, the generator dependency goes through `contextmanager_in_threadpool`, which yields the loop in the middle of that work, and the synchronous handler adds another thread hop. `AsyncSessionDep` and an async handler give no such yields, so the same CPU runs as one longer block. A yield at handler entry splits only the end of that block.

## Small requests

Every route, including DELETE and a CLEAR of 1,000 or 5,000 keys, measured about 1 ms more median stall than the base: roughly 2–3 ms against 3–4 ms on both backends and in both rounds. Request duration was equal or lower.

## Guard tests

A `loaded_as_persistent` listener on `Session` recorded nothing for the converted routes. On the base revision, all four route cases failed because `session.get(TaskInstance, ...)` loaded `TaskInstance` and its joined `DagRun`, so the base alone could not show whether the value-column projection was guarded. Two controls on the conversion settled it: restoring the full-entity value getter failed only the GET case, and restoring `session.get` failed with `TaskInstance` and `DagRun`.
