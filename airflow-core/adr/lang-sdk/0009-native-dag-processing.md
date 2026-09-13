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

`AbstractDagImporter` (AIP-85, `task-sdk/src/airflow/sdk/importers/`) makes the
Dag processor aware of source formats beyond `.py`: `can_handle`,
`list_dag_definitions`, `import_definition`, `get_source_code`, returning
`DagImportResult.dags: list[DAG]` — `airflow.sdk.DAG` objects.

A Lang-SDK artifact (JAR, packed Go executable) can be one of those sources: a
Dag authored entirely in Go or Java, with `Dag(spec)` on the SDK side. Parsing
it means launching a runtime, which is what a coordinator does. This ADR settles
where that importer comes from, which coordinator instance backs it, and how
the two are kept from being configured twice.

`JavaDagImporter` and `JavaCoordinator` are the examples throughout;
`ExecutableDagImporter` (Go) and `NodeDagImporter` (TypeScript) follow the same
shape.

## Decision

### A Lang-SDK DagImporter Comes From Its Coordinator

`AbstractDagImporter` is about source formats. The coordinator is about
processes. A Lang-SDK importer composes one, and the coordinator is what hands
it out — `JavaCoordinator.get_dag_importer()` returns a `JavaDagImporter`
already bound to that coordinator.

An operator therefore never configures an importer's coordinator: that wiring is
an implementation detail of the coordinator that produced it, and
`[sdk] coordinators` stays the one place a runtime is declared. The existing
`[dag_processor] dag_importer_configs` path remains the door for importers with
no runtime behind them — a YAML importer, say. A Lang SDK never arrives that
way. Appendix A gives the registration tiers.

### One Coordinator Instance Owns a DagBundle

Importers are keyed by file extension, one per extension, with the later
registration evicting the earlier one and logging a warning. Two
`JavaCoordinator` instances on different JDKs would both claim `.jar`.

The boundary that resolves this is deployment-shaped rather than a tie-break: a
coordinator instance owns a DagBundle. `SubprocessCoordinator` already
classifies that ownership at construction into three artifact sources, and only
`NAMED_BUNDLE` names a bundle — which makes `NAMED_BUNDLE` the mode a deployment
running two runtimes of the same language has to use. `EXPLICIT_ROOT` has no
DagBundle at all, so its artifacts are never scanned by the Dag processor and it
cannot produce a Dag to persist; it stays a coordinator reached by queue, for
mixed-language work only. Appendix B covers all three modes, the roots a parse
needs, and extensionless artifacts.

### The Result Has to Be `airflow.sdk.DAG`

`JavaDagImporter.import_definition` calls `parse_dag`
([ADR-0008](0008-lang-sdk-parse-protocol.md)) and the runtime serializes its
`Dag` registrations. `TaskHandler` registrations have no Dag to serialize, so a
mixed-language `dag_id` is never a candidate for persistence here — there is
nothing to filter out ([ADR-0010](0010-mixed-language-dag-processing.md)).

Because `DagImportResult.dags` is `list[DAG]`, the importer wraps each
serialized entry as a `LazyDeserializedDAG` and transforms it into an
`airflow.sdk.DAG` before returning. The transform is mechanical — the serialized
form encodes the SDK Dag's own fields — but `LazyDeserializedDAG` lives in
`airflow-core` today, so where a Task SDK importer reaches it from is settled by
AIP-85's own `list[DAG]` / `list[LazyDeserializedDAG]` discussion rather than
here.

Appendix C walks both flows: a pure Python file, and a Lang-SDK artifact.

## Consequences

- **A Lang-SDK importer is never configured by hand.** `[sdk] coordinators` is
  the one place a runtime is declared; the importer follows from it through
  `get_dag_importer()`, so the same runtime has no second configuration site to
  drift from.
- **One coordinator instance per DagBundle** becomes a deployment constraint:
  two JDKs mean two `dag_bundle_name` values and two bundle-scoped registries.
  It is what keeps extension-keyed importer registration unambiguous, and it
  makes `NAMED_BUNDLE` the mode a multi-runtime deployment has to use.
- A coordinator in `EXPLICIT_ROOT` mode cannot back a Dag importer at all. Its
  artifacts live outside any DagBundle, so nothing scans them.
- `CoordinatorManager` needs a reverse lookup it does not have: it resolves
  coordinators by queue only today, with nothing that answers "which
  coordinators serve this bundle".
- A packed Go bundle claims the **empty** extension, which three call sites
  currently treat as absent rather than as a key.
- `get_source_code` is abstract, so every Lang-SDK importer must implement it,
  and a native Lang-SDK Dag has no Python source to return. What it should
  return, and how that squares with
  [ADR-0006](0006-no-lang-sdk-source-display.md), is not settled here.

## References

- [ADR-0008](0008-lang-sdk-parse-protocol.md) — `parse_dag` and the coordinator
  interface it belongs to
- [ADR-0010](0010-mixed-language-dag-processing.md) — why `TaskHandler`
  registrations never reach a `DagImporter`
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`,
  build-time artifact inventory
- [ADR-0004](0004-dag-parsing.md) — `can_handle_dag_file`, the subprocess bridge
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
- `task-sdk/src/airflow/sdk/importers/` — `AbstractDagImporter`,
  `DagImportResult`, `DagImporterRegistry`
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs

## Appendix — For Implementation

### Appendix A — Registration

```
BaseCoordinator.get_dag_importer() -> AbstractDagImporter | None
    default: None — this coordinator contributes no importer
    JavaCoordinator.get_dag_importer() -> JavaDagImporter(coordinator=self)
```

```
DagImporterRegistry.from_config(bundle_name)
  ├── defaults                              PythonDagImporter, ZipImporter
  ├── coordinators serving bundle_name                              (new)
  │     └── register(coordinator.get_dag_importer())
  ├── [dag_processor] dag_importer_configs             (global, unchanged)
  └── that bundle's own `importers` list                   (unchanged)
```

`get_importer_registry(bundle_name)` is already cached per bundle, so the
scoping mechanism is in place. What `CoordinatorManager` still needs is the
reverse lookup: it resolves coordinators by queue only today —
`for_queue(queue)`, driven by `[sdk] queue_to_coordinator` — with nothing that
answers "which coordinators serve this bundle".

### Appendix B — Artifact Sources

`SubprocessCoordinator` classifies artifact ownership at construction:

```
EXPLICIT_ROOT   jars_root / executables_root / bundles_root
                  → no DagBundle at all
NAMED_BUNDLE    dag_bundle_name
                  → that bundle, at the version current when work starts
TASK_BUNDLE     neither set
                  → the bundle the delegating Dag itself lives in,
                    at the run's version
```

The first two are mutually exclusive, and both that conflict and a
`dag_bundle_name` naming an unconfigured bundle are rejected at construction.
Which mode a coordinator is in decides whether it can back an importer:

- **`NAMED_BUNDLE` registers into that bundle's registry.** Two
  `dag_bundle_name` values, two bundle-scoped registries, `.jar` claimed once in
  each.
- **`TASK_BUNDLE` has no fixed bundle**, so its importer registers into every
  bundle-scoped registry. Sound only while it is the sole claimant of its
  extension — the co-located single-runtime deployment.
- **`EXPLICIT_ROOT` has no DagBundle at all**, so the Dag processor never scans
  its artifacts and it cannot produce a Dag to persist.

#### Resolving artifact roots at parse time

`_init_root_source` already resolves roots for all three modes, but publishes
them through `_get_scan_roots()`, which is scoped to an active task and raises
outside one. Both parse-side commands need the same roots with no
`TaskInstance` in hand: `EXPLICIT_ROOT` and `NAMED_BUNDLE` resolve from the
coordinator's own configuration, and `TASK_BUNDLE` resolves against the bundle
the Dag processor is parsing. The scope that publishes the roots has to open
for a parse as well as for a task.

#### Extensionless artifacts

A packed Go bundle has no suffix, so `ExecutableDagImporter` claims the **empty**
extension as a first-class key, rather than depending on a `can_handle` scan
whose winner varies with registration order. Three places assume a non-empty
suffix today: `_normalize_extensions` rewrites `""` to `"."`; `get_importer` and
`can_handle` guard on `if suffix:`, which skips the extension map entirely for an
extensionless file; and `find_file_dag_definitions` filters on
`path.suffix.lower()`. Empty has to pass through all three, with the guards
testing `suffix is not None`.

### Appendix C — Flow Walkthroughs

#### Pure Python Dag (no `@task.stub`)

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  no stub tasks, nothing to cross-validate
  ├── Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST
```

#### Native Lang-SDK Dag

```
JavaDagImporter.import_definition(definition, bundle=...)
  │
  ├── JavaCoordinator.parse_dag(...)      ← bridge mode
  │     ├── _build_parse_dag_command()  → (command, schema_version)
  │     └── Spawn JVM, forward fd 0 ⇄ comm socket
  │           manager ──DagFileParseRequest───▶ JVM
  │           manager ◀─DagFileParsingResult─── JVM
  │           ┌────────────────────────────────────────────────────┐
  │           │  serialized_dags: ["java_report"]  (@Builder.Dag)  │
  │           └────────────────────────────────────────────────────┘
  │         The TaskHandler registrations have no Dag to serialize —
  │         no "etl" entry exists to be discarded.
  │
  ├── LazyDeserializedDAG(data=...) → airflow.sdk.DAG
  ├── Return DagImportResult(dags=[DAG("java_report")])
  ▼
DagModelOperation → PERSIST "java_report" only
```
