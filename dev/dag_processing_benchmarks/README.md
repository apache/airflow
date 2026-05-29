<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [DAG-processing performance bottlenecks](#dag-processing-performance-bottlenecks)
  - [Bottleneck list](#bottleneck-list)
  - [Scripts](#scripts)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
 TODO: This license is not consistent with the license used in the project.
       Delete the inconsistent license and above line and rerun pre-commit to insert a good license.

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# DAG-processing performance bottlenecks

A survey of potential performance bottlenecks in the DAG file processing pipeline
(`airflow.dag_processing`) plus benchmark scripts to establish baselines before
any optimisation work. **No fixes are implemented here** — these scripts measure
the *current* behaviour so later changes have a baseline to beat.

The pipeline, end to end:

```
DagFileProcessorManager (manager.py)         # parent: loop, queue, bundles, DB persistence
  └─ DagFileProcessorProcess (processor.py)  # one forked subprocess per file
       └─ BundleDagBag (dagbag.py)           # collect + import the file
            └─ PythonDagImporter (importers/python_importer.py)   # exec user code
       └─ _serialize_dags -> DagSerialization.to_dict (serialization/serialized_objects.py)
  └─ update_dag_parsing_results_in_db (collection.py)            # write results to metadata DB
```

> ⚠️ Benchmarking note (the hard-won "alert on weird benchmarks" lesson): absolute
> milliseconds are machine- and load-dependent, especially under WSL. The scripts
> report the **min** of several samples plus the median, and the value is in the
> **scaling across input sizes**, not the raw numbers. Always re-measure on the
> target machine before/after a change, ideally with an interleaved A/B.

---

## Bottleneck list

Ordered roughly by impact-at-scale. Each entry notes the source location and
whether a benchmark script in this directory targets it.

### 1. File-queue de-duplication is `O(N × queue-size)` — `[bench_file_queue.py]`

`manager.py :: DagFileProcessorManager._add_files_to_queue` de-duplicates with a
linear membership test against a `deque`:

- `mode="front"`/`"back"`: `new_files = list(f for f in files if f not in self._file_queue)`
  — `x in deque` is `O(Q)` (manager.py ~L1518-1523).
- `mode="frontprio"`: `self._file_queue.remove(file)` per file — also `O(Q)`
  (manager.py ~L1511-1517).

Filling an **empty** queue in one shot is fine (`O(N)`), but the priority path
(`_queue_requested_files_for_parsing` → `frontprio`), callback adds (one file per
loop via `_add_callback_to_queue` → `front`), and any re-add against a populated
queue are **quadratic** in the steady-state file count. Measured: ~35 ms (500
files) → ~2.6 s (4000 files) for the frontprio/drip paths — a clean 4× per
doubling.

Related per-tick set rebuilds over *every* tracked file:
`prepare_file_queue` (manager.py ~L1412-1413), `_add_new_files_to_queue`
(~L1319-1321), and `processed_recently` (~L1378-1393) which does a full linear
scan of `self._file_stats` **per call**.

### 2. Each file is read + AST-parsed multiple times per parse — `[bench_file_scan.py]`

Before user code even executes, per file, per loop:

- `utils/file.py :: might_contain_dag` reads the whole file as bytes, `.lower()`-copies
  it, and calls `conf.getimport(...)` on **every** call (file.py L134-161). Runs
  during directory scan *and* again inside the importer (`_load_modules_from_file`).
- `utils/file.py :: iter_airflow_imports` does `ast.parse(...)` of the file to find
  airflow imports to pre-import (file.py L173-181), driven by
  `processor.py :: _pre_import_airflow_modules` (L183-201, L575).
- `utils/dag_version_inflation_checker.py :: check_dag_file_stability` does
  `ast.parse(...)` **again** and a full `ast.NodeVisitor` walk of the tree on every
  parse, regardless of whether the file changed (L569-590; called unconditionally in
  `processor.py :: _parse_file` L234).

So the same bytes are read 2–3× and `ast.parse`-d 2× per file before the real
import. Measured combined pre-exec overhead: ~13 ms for a 500-task / 48 KB file,
dominated by the stability AST walk (~8 ms) and the duplicate parse (~4 ms).

### 3. DAG serialization is the subprocess CPU hot path — `[bench_serialize_dag.py]`

`serialization/serialized_objects.py :: serialize_dag` (L1698) serializes every
task (`cls.serialize(task)`, L1703) and runs `detect_dependencies` per task
(L1705-1709). Known/observed sub-costs:

- `inspect.signature` per task — largely fixed by caching `_FORBIDDEN_TEMPLATE_FIELDS`
  (PR #67701, already merged).
- `ensure_serialized_asset()` doing an encode→decode roundtrip per asset outlet
  inside `detect_task_dependencies` (~L863-933) just to read `.name`/`.uri`.
  Measured: +55% per-task cost at 3 outlets/task vs none.
- `_is_excluded` is called ~3× per field (837 K calls for 250 tasks in the
  profile) and `serialize_template_field` → `conf.getint(...)` runs a config
  lookup **per task** (serialized_objects.py L1369 region / helpers.py L36;
  ~15% of serialize wall-time in the profile). Run with `--profile` to see.

Measured: ~112 µs/task (no assets), roughly linear; a 250-task DAG ≈ 28 ms.

### 4. Result persistence is per-file with many small queries — `[bench_persist_results.py]` (needs DB)

`manager.py :: _collect_results` (L1208-1219) calls `handle_parsing_result`
(`@provide_session`, L1093) once per finished file → each parsed file is its own
DB transaction calling `collection.py :: update_dag_parsing_results_in_db` (L432).
Inside, per file / per DAG:

- `_RunInfo.calculate` (L179-210) runs ~2 queries **per DAG** — a latest-run
  scalar-subquery select plus `DagRun.active_runs_of_dags`.
- `_update_import_errors` (L346) issues `SELECT bundle_name, filename FROM
  import_error` with **no WHERE clause** — a full scan of the whole `import_error`
  table on **every** file persist (L355-357) — then per error does
  `UPDATE` + a re-`SELECT` + a listener call (L373-401) and a per-row
  `UPDATE dag … synchronize_session="fetch"` (L417-429, the `fetch` strategy adds
  a SELECT).
- `_update_dag_warnings` (L322-343) does `session.merge` per warning (SELECT+upsert
  each).

Because persistence is not batched across files, query count grows with
`files × DAGs`, and the unfiltered `import_error` scan grows with the global
import-error table size.

### 5. `write_dag` does an extra full-row SELECT for deadline DAGs

`models/serialized_dag.py :: write_dag` — the bulk `_prefetch_dag_write_metadata`
(L530) already collapses the per-DAG update-interval/hash/version lookups into 2
bulk queries, but DAGs with deadlines still `session.scalar(select(cls)…)` to load
the full serialized row (L644-646) to try to reuse deadline UUIDs.

### 6. Interval cleanups load whole tables into Python and delete row-by-row

- `manager.py :: deactivate_stale_dags` (L411) selects **all** non-stale `DagModel`
  rows and iterates them in Python every `parsing_cleanup_interval`; the final
  `UPDATE … synchronize_session="fetch"` (L456-460) adds a SELECT.
- `manager.py :: clear_orphaned_import_errors` (L922) loads all `ParseImportError`
  rows for a bundle and issues a per-row `session.delete` (L938-940).
- `purge_inactive_dag_warnings` runs every loop iteration (L502).

These violate the "batch bulk DELETE/UPDATE with LIMIT" guidance in `CLAUDE.md`
for scheduler-loop cleanups and are unbounded.

### 7. Process-per-file fork + IPC overhead

Each file is parsed in a freshly forked `DagFileProcessorProcess`
(`processor.py`, started from `manager.py :: _create_process` L1278). `gc.freeze()`
(manager.py L329) reduces copy-on-write, but per-file fork, socket setup, a
per-file log file open (`_get_logger_for_dag_file` L1259), and pydantic
encode/decode of `DagFileParsingResult` (carrying every `LazyDeserializedDAG`)
across the socket are paid for **every** file, every loop. Dominates when there
are many small DAG files.

### 8. Repeated `zipfile.is_zipfile` / stat calls during scan & refresh

`utils/file.py :: find_dag_file_paths` (L101-117) and
`manager.py :: _get_observed_filelocs` (L890) call `zipfile.is_zipfile(path)`
(opens the file to read magic bytes) per path on every bundle refresh, in addition
to the `os.path.getmtime` stats in `_sort_by_mtime` (manager.py L1356).

---

## Scripts

| Script | Targets | Needs DB? |
| --- | --- | --- |
| `bench_file_queue.py` | #1 queue de-dup quadratic behaviour | no |
| `bench_file_scan.py` | #2 redundant per-file read + AST parsing | no |
| `bench_serialize_dag.py` | #3 serialization hot path (`--profile`, `--assets`) | no |
| `bench_persist_results.py` | #4 per-file DB persistence (query counts) | **yes** |
| `_common.py` | shared timing helpers (min/median, warmup) | — |

### Running

CPU/in-memory scripts run on the host via `uv` (no Breeze needed):

```bash
uv run --project airflow-core python dev/dag_processing_benchmarks/bench_file_queue.py
uv run --project airflow-core python dev/dag_processing_benchmarks/bench_file_scan.py
uv run --project airflow-core python dev/dag_processing_benchmarks/bench_serialize_dag.py
uv run --project airflow-core python dev/dag_processing_benchmarks/bench_serialize_dag.py --tasks 250 --assets 3 --profile
```

The DB script needs a migrated metadata DB — run it under Breeze:

```bash
breeze shell
# inside the container:
python /opt/airflow/dev/dag_processing_benchmarks/bench_persist_results.py --files 50 --dags-per-file 1
```

It creates a throwaway bundle (`bench_persist_bundle`) and DAGs prefixed
`bench_persist_`, and removes them on exit (`--no-cleanup` to keep them).

### Baselines captured on this machine (WSL2, Python 3.10) — indicative only

```
file queue (ms):    files   fill-empty   front re-add   frontprio   incremental
                      500        0.17         35.4         35.6        36.5
                     4000        0.31       2636.7       2537.9      2640.5      # ~4x per doubling

file scan (ms):     tasks   might_contain   iter_imports   stability   combined
                       10        0.034         0.093         0.214        0.435
                      500        0.075         4.008         8.285       13.158

serialize (µs/task):   no assets ≈ 112-172;   3 assets/task ≈ 175 (+55%)
                       250-task DAG ≈ 28 ms (no assets), ≈ 44 ms (3 assets/task)
```
