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

# ADR-0009: Native Dag Processing — DagImporter Registration and Routing

## Status

Proposed

## Context

`AbstractDagImporter` (AIP-85, `task-sdk/src/airflow/sdk/importers/`) makes the Dag processor aware of source formats beyond `.py`. It exposes `can_handle`, `list_dag_definitions`,
`import_definition` and `get_source_code`, and returns `DagImportResult.dags: list[DAG]` — `airflow.sdk.DAG` objects.

A Lang-SDK artifact can be one of those sources: a Dag authored entirely in Go or Java, with `Dag(spec)` on the SDK side. Parsing it means launching a runtime, which is what a
coordinator does. This ADR settles where that importer comes from, which coordinator instance backs it, and how the two avoid being configured twice.

`JavaDagImporter` and `JavaCoordinator` are the examples throughout. `ExecutableDagImporter` (Go) and `NodeDagImporter` (TypeScript) follow the same shape.

## Decision

### The coordinator hands out its importer

```
BaseCoordinator.get_dag_importer() -> AbstractDagImporter | None
    default: None — this coordinator contributes no importer
    JavaCoordinator.get_dag_importer() -> JavaDagImporter(coordinator=self)
```

The importer comes back already bound to the coordinator, so an operator never configures which coordinator an importer uses. `[sdk] coordinators` stays the one place a runtime is
declared.

### Registration order

```
DagImporterRegistry.from_config(bundle_name)
  ├── defaults                              PythonDagImporter, ZipImporter
  ├── CoordinatorManager.for_bundle(bundle_name)                    (new)
  │     └── register(coordinator.get_dag_importer())
  ├── [dag_processor] dag_importer_configs             (global, unchanged)
  └── that bundle's own `importers` list                   (unchanged)
```

`dag_importer_configs` remains the door for importers with no runtime behind them — a YAML importer, say. A Lang SDK never arrives that way.

### `CoordinatorManager.for_bundle`

```
CoordinatorManager
  ├── for_queue(queue)    → one coordinator      (shipped — task execution)
  └── for_bundle(name)    → the coordinators serving that bundle    (new)
```

`for_queue` answers "who runs this task". `for_bundle` answers "who can parse Dags in this bundle", which is what the registry tier above needs. It reads the same `[sdk]
coordinators` specs, selecting by the artifact source below.

### One coordinator instance owns a DagBundle

```
EXPLICIT_ROOT   jars_root / executables_root / bundles_root
                  → no DagBundle at all      → not returned by for_bundle
NAMED_BUNDLE    dag_bundle_name
                  → that bundle, at the version current when work starts
TASK_BUNDLE     neither set
                  → the bundle the delegating Dag lives in, at the run's version
```

Importers are keyed by file extension, one per extension, so two `JavaCoordinator` instances on different JDKs would both claim `.jar`. Only `NAMED_BUNDLE` names a bundle, which
makes it the mode a deployment running two runtimes of the same language has to use. `EXPLICIT_ROOT` has no DagBundle, so nothing scans its artifacts and it cannot produce a Dag to
persist; it serves mixed-language work only. Appendix A covers the three modes in full.

### The result has to be `airflow.sdk.DAG`

```
JavaDagImporter.import_definition(definition, bundle=...)
  │
  ├── JavaCoordinator.parse_dag(...)      ← bridge mode (ADR-0008)
  │     ├── _build_parse_dag_command()  → (command, schema_version)
  │     └── Spawn JVM, forward fd 0 ⇄ comm socket
  │           manager ──DagFileParseRequest───▶ JVM
  │           manager ◀─DagFileParsingResult─── JVM
  │           ┌────────────────────────────────────────────────────┐
  │           │  serialized_dags: ["java_report"]  (@Builder.Dag)  │
  │           └────────────────────────────────────────────────────┘
  │         TaskHandler registrations have no Dag to serialize —
  │         no "etl" entry exists to be discarded.
  │
  ├── LazyDeserializedDAG(data=...) → airflow.sdk.DAG
  ├── Return DagImportResult(dags=[DAG("java_report")])
  ▼
DagModelOperation → PERSIST "java_report" only
```

For comparison, a pure Python file with no stub tasks:

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  no stub tasks, nothing to cross-validate
  ├── Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST
```

`DagImportResult.dags` is `list[DAG]`, so the importer wraps each serialized entry as a `LazyDeserializedDAG` and transforms it into an `airflow.sdk.DAG`. `LazyDeserializedDAG`
lives in `airflow-core` today, so where a Task SDK importer reaches it from is settled by AIP-85's own `list[DAG]` / `list[LazyDeserializedDAG]` discussion, not here.

## Consequences

- A Lang-SDK importer is never configured by hand. The runtime is declared once, in `[sdk] coordinators`, and the importer follows from it.
- One coordinator instance per DagBundle becomes a deployment constraint: two JDKs mean two `dag_bundle_name` values and two bundle-scoped registries. This is what keeps
  extension-keyed registration unambiguous.
- A coordinator in `EXPLICIT_ROOT` mode cannot back a Dag importer. Its artifacts live outside any DagBundle, so nothing scans them.
- `CoordinatorManager` gains `for_bundle`, a second lookup axis beside `for_queue`.
- A packed Go bundle claims the empty extension, which three call sites currently treat as absent rather than as a key. Appendix B lists them.
- `get_source_code` is abstract, so every Lang-SDK importer must implement it, and a native Lang-SDK Dag has no Python source to return. What it should return, and how that squares
  with [ADR-0006](0006-no-lang-sdk-source-display.md), is not settled here.

## References

- [ADR-0008](0008-lang-sdk-parse-protocol.md) — `parse_dag` and the coordinator interface it belongs to
- [ADR-0010](0010-mixed-language-dag-processing.md) — why `TaskHandler` registrations never reach a `DagImporter`
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`, build-time artifact inventory
- [ADR-0004](0004-dag-parsing.md) — `can_handle_dag_file`, the subprocess bridge
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- `task-sdk/src/airflow/sdk/importers/` — `AbstractDagImporter`, `DagImportResult`, `DagImporterRegistry`
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix

### Appendix A — Artifact sources and what `for_bundle` returns

`SubprocessCoordinator` classifies artifact ownership at construction. The explicit root and `dag_bundle_name` are mutually exclusive, and both that conflict and a
`dag_bundle_name` naming an unconfigured bundle are rejected there.

`NAMED_BUNDLE` is the unambiguous case. `for_bundle(name)` returns it when its `dag_bundle_name` matches, so its importer lands in exactly one bundle-scoped registry. Two
`dag_bundle_name` values give two registries, and `.jar` is claimed once in each.

`TASK_BUNDLE` has no fixed bundle — its artifacts ride along with whichever Dag delegates to them — so `for_bundle` returns it for every bundle and its importer registers
everywhere. That is sound only while it is the sole claimant of its extension, which is the co-located single-runtime deployment.

`EXPLICIT_ROOT` points at a filesystem path outside any DagBundle. The Dag processor never scans it, so there is no file for an importer to claim and `for_bundle` never returns it.
Such a coordinator is reachable only by queue, for mixed-language work.

`get_importer_registry(bundle_name)` is already cached per bundle, and `CoordinatorManager` caches instances separately, so the two caches have to be reset together.

### Appendix B — Extensionless artifacts

A packed Go bundle has no suffix. `ExecutableDagImporter` claims the empty extension as a first-class key rather than depending on a `can_handle` scan, whose winner varies with
registration order because `_ordered_importers` is scanned in reverse.

Three places assume a non-empty suffix today:

- `_normalize_extensions` rewrites `""` to `"."`.
- `get_importer` and `can_handle` guard on `if suffix:`, which skips the extension map entirely for an extensionless file.
- `find_file_dag_definitions` filters on `path.suffix.lower()`.

Empty has to pass through all three, with the guards testing `suffix is not None`.

### Appendix C — Resolving artifact roots at parse time

`_init_root_source` already resolves roots for all three modes, but publishes them through `_get_scan_roots()`, which is scoped to an active task and raises outside one. Both
parse-side commands need the same roots with no `TaskInstance` in hand: `EXPLICIT_ROOT` and `NAMED_BUNDLE` resolve from the coordinator's own configuration, and `TASK_BUNDLE`
resolves against the bundle the Dag processor is parsing. The scope that publishes the roots has to open for a parse as well as for a task.
