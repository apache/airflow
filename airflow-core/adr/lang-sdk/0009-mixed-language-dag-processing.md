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

# ADR-0009: Mixed-Language Dag Processing — Task Handlers Are Not Dags

## Status

Proposed

## Context

A Lang-SDK artifact can serve either of the two authoring features the Language SDK spec fixes, or both at once:

| Feature                         | Who owns the graph         | Author writes                    | Artifact contributes |
|---------------------------------|----------------------------|----------------------------------|----------------------|
| **Mixed Language Task Handler** | Python, via `@task.stub`   | `TaskHandler(dagId, taskId, fn)` | Only task bodies     |
| **Native Dag**                  | the Lang-SDK source itself | `Dag(spec)`                      | The entire Dag       |

In the mixed-language role the artifact used to author a Dag too, under the same `dag_id` the Python file already owns. Two conflicting definitions reached persistence and
something downstream had to choose between them. This ADR removes the conflict at the authoring interface, and defines where a stub task is validated against the handler that
implements it.

Terms follow the Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`, spec version `1.0`). Note that the spec's `bundle` is the SDK-side registration container, not Airflow's
`DagBundle` — both appear below.

## Decision

### Two registration kinds, one bundle

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Per-task (Python): is_stub                                              │
│    AbstractOperator.is_stub = False   (default)                          │
│    _StubOperator.is_stub    = True                                       │
│                                                                          │
│  Per-registration (Lang-SDK): which interface the author wrote           │
│    Dag(spec)                       → DagRef                              │
│    TaskHandler(dagId, taskId, fn)  → TaskHandlerRef                      │
│                                                                          │
│  Both kinds go into one bundle, through one verb:                        │
│    bundle.register(dag, handler, ...)  →  bundle.serve()                 │
└──────────────────────────────────────────────────────────────────────────┘
```

A `TaskHandlerRef` has no schedule, no task graph, and no `dag_id` of its own to persist. A `DagRef` has all three. Dag parsing draws on the `Dag` registrations
([ADR-0008](0008-native-dag-processing.md)); validation draws on the `TaskHandler` registrations. Because both kinds share one bundle and one `register` verb, the bundle cannot be
the discriminator — the registration kind is. Appendix A gives the rejected alternative and why.

### One artifact, both kinds

```
analytics.jar
├── EtlTasks            @Builder.TaskHandler(dagId = "etl", taskId = "extract")
│                       @Builder.TaskHandler(dagId = "etl", taskId = "transform")
│                         backs etl.py's stub tasks — no Dag on the Java side
└── JavaReportPipeline  @Builder.Dag(id = "java_report")
                          native, persisted as "java_report"

bundle.register(javaReport, etlExtract, etlTransform);   // one verb, both kinds
bundle.serve(args);
```

The split is per registration, not per file and not per bundle.

### Validation runs in the Dag-file parse, after the Dags exist

Comparing a stub against its handler needs two things at once: the `airflow.sdk.DAG` objects the Python file produced, and the `arg_bindings` that only appear once those Dags are
serialized. `_parse_file` holds both, between `_serialize_dags` and the `DagFileParsingResult` it returns. That is where the handler query is issued.

```
DagFileProcessorProcess(etl.py)                                ← manager spawns, as for any file
  └── _parse_file_entrypoint → _parse_file
        ├── BundleDagBag → PythonDagImporter → airflow.sdk.DAG objects     ← the Dags now exist
        ├── _serialize_dags(bag)  →  is_stub tasks carry arg_bindings (ADR-0007)
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 1: Collect the dag_ids to ask about — every Dag in this       │
        │  │          file with at least one is_stub task    →  ["etl"]          │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 2: Resolve stub tasks → Coordinator instances via             │
        │  │          CoordinatorManager.for_queue(queue)                        │
        │  │          ([sdk] queue_to_coordinator)                               │
        │  │                                                                     │
        │  │  stub task       queue       Coordinator instance                   │
        │  │  ──────────────────────────────────────────────────                 │
        │  │  extract      →  "jdk-11"  → JavaCoordinator(name="jdk-11")         │
        │  │  transform    →  "jdk-17"  → JavaCoordinator(name="jdk-17")         │
        │  │  load         →  "jdk-11"  → JavaCoordinator(name="jdk-11")         │
        │  │                                                                     │
        │  │  Deduplicate → 2 distinct coordinator instances                     │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 3: Locate the artifact backing each dag_id, no JVM launch —   │
        │  │          reuse BundleScanner (ADR-0003), whose build-time inventory │
        │  │          indexes registered (dagId, taskId) handler pairs alongside │
        │  │          the artifact's native Dag ids                              │
        │  │                                                                     │
        │  │  JavaCoordinator(name="jdk-11")                                     │
        │  │    ├── resolve artifact roots for its mode                          │
        │  │    │     EXPLICIT_ROOT → jars_root                                  │
        │  │    │     NAMED_BUNDLE  → dag_bundle_name, pinned                    │
        │  │    │     TASK_BUNDLE   → the bundle being parsed                    │
        │  │    │                                                                │
        │  │    └── BundleScanner.scanBundles(roots)                             │
        │  │          → "etl" → ResolvedBundle(analytics.jar, mainClass, ...)    │
        │  │                                                                     │
        │  │  Group the dag_ids by (coordinator, artifact) — one group, one      │
        │  │  process, one request                                               │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 4: Query each group — one request, one response               │
        │  │                                                                     │
        │  │  SDKTaskHandlerProcessorProcess.start(                              │
        │  │      target=_parse_task_handler_entrypoint,                         │
        │  │      coordinator=JavaCoordinator("jdk-11"),                         │
        │  │      path=analytics.jar)                                            │
        │  │    │                                                                │
        │  │    ├── in the child: _build_parse_task_handler_command()            │
        │  │    │                 coordinator.parse_task_handler() — spawn JVM   │
        │  │    │                                                                │
        │  │    │   ──TaskHandlerParseRequest(file=analytics.jar,                │
        │  │    │                             dag_ids=["etl"])─────▶ JVM         │
        │  │    │                            (ToSDKTaskHandlerProcessor)         │
        │  │    │                                                                │
        │  │    │      JVM answers from its own TaskHandler registrations        │
        │  │    │      whose dagId is one of the requested ids                   │
        │  │    │                                                                │
        │  │    │   ◀─TaskHandlerParsingResult(task_handlers={                   │
        │  │    │        "etl": [extract, transform, load]})─────── JVM          │
        │  │    │                            (ToManager)                         │
        │  │    │                                                                │
        │  │    └── Get* from the JVM relayed up to the manager unchanged        │
        │  │                                                                     │
        │  │  JavaCoordinator(name="jdk-17")                                     │
        │  │    └── its own process, its own resolved artifact and handler set   │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 5: Compare per dag_id against the Dag just serialized —       │
        │  │          union task_handlers[dag_id] across every process first     │
        │  │                                                                     │
        │  │  Python Dag "etl" (stub tasks)     TaskHandlerDeclaration           │
        │  │  ──────────────────────────────    ──────────────────────────────   │
        │  │  task_id                       ↔   task_id     (sets must match)    │
        │  │  arg_bindings[*].name          ↔   params[*].name    (in order)     │
        │  │  arg_bindings[*].value_schema  ↔   params[*].value_schema           │
        │  │        compared only where neither side is null                     │
        │  │                                                                     │
        │  │  On mismatch → import_errors["etl.py"]                              │
        │  └─────────────────────────────────────────────────────────────────────┘
        ▼
  DagFileParsingResult(serialized_dags=["etl"], import_errors={...})
        ▼
  manager → DagModelOperation → PERSIST (the Python Dag is the sole DB record)
```

The parse owns validation, not an importer. `PythonDagImporter` returns `airflow.sdk.DAG` objects and knows nothing about coordinators or queues, so `@task.stub` keeps working for
any importer that can produce a Dag carrying stub tasks. `_parse_file` is also the only place where the whole file's Dags are visible at once, which is what lets one request cover
every `dag_id` that resolved to the same artifact ([ADR-0010](0010-lang-sdk-parse-protocol.md)).

Resolution goes through the coordinator registry, not the filesystem, so the Python Dag and the Lang-SDK artifact **do not need to be in the same DagBundle**. Nothing here needs an
`airflow.sdk.DAG` round-trip either — validation compares against the Dag the Python parser already built. Appendix B states exactly what is compared.

### Decision matrix

| Caller                                     | Coordinator call                                     | What comes back                          | Action                                           |
|--------------------------------------------|------------------------------------------------------|------------------------------------------|--------------------------------------------------|
| `_parse_file` → `PythonDagImporter`        | — (the Python file is parsed in process)             | its own parsed Dags                      | PERSIST                                          |
| `_parse_file`, per (coordinator, artifact) | `parse_task_handler`, scoped to that group's dag_ids | `TaskHandlerParsingResult`               | VALIDATE only — not a Dag, so nothing to persist |
| `_parse_file` → `JavaDagImporter`          | `parse_dag`                                          | `DagFileParsingResult`, native Dags only | PERSIST                                          |

There is no fourth row. A `TaskHandlerRef` has no Dag, so no `DagImporter` — and nothing reading a `DagImporter`'s results — ever sees one.

## Consequences

- Python leads, Lang-SDK follows. The Dag-file parse persists the Dag and drives validation via `queue → Coordinator → parse_task_handler`.
- No importer knows about coordinators. `PythonDagImporter` is unchanged by this ADR; the stub-to-handler comparison sits in `_parse_file`, above every importer.
- A mixed-language `dag_id` never appears in Dag processing results. No `Dag` registration exists for a `dag_id` a Python file already owns, so everything downstream sees exactly
  one record per `dag_id`, with no flag to interpret.
- Stub/implementation mismatches — missing handler, extra handler, parameter name or order, incompatible schema — surface as import errors against the Python file at parse time,
  alongside the errors the parse already reports. An unannotated stub argument is checked by name and position only.
- The Python Dag and Lang-SDK artifact can live in different DagBundles.
- A single Dag can have stubs targeting different queues, some Java, some Go. Each resolves to its own coordinator instance, and validation unions their declarations per `dag_id`
  before comparing task ids.
- Validating a file costs one extra process per (coordinator, artifact) pair its stubs resolve to — one for the common case of a file whose stubs all target a single runtime, and
  none at all for a file with no stub tasks.
- Mixed-language is Python-primary only. Lang-SDK runtimes cannot define stub operators; a native Dag cannot delegate tasks to Python.
- No per-Dag flag, no schema migration, no new `DagModel` column, no REST/UI change.
- Terms track Language SDK spec `1.0`. A spec rename of `TaskHandler`, or of the `register` / `serve` verbs, lands here too.

## References

- [ADR-0010](0010-lang-sdk-parse-protocol.md) — `parse_task_handler` and the `TaskHandlerParsingResult` shape this ADR compares against
- [ADR-0008](0008-native-dag-processing.md) — the `Dag`-registration half, and the importer that persists it
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`, build-time artifact inventory
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display for mixed-language Dags
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` / `TaskArgBinding` / `ArgValueSchema`
- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`)
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
- `airflow-core/src/airflow/dag_processing/processor.py` — `_parse_file`, `_serialize_dags`, `DagFileParsingResult`

## Appendix

### Appendix A — Why the split lives on the interface

The alternative was to keep authoring the Dag on both sides and mark the Lang-SDK copy with a per-Dag flag, `is_mixed_language_dag`, so importers knew which copy to drop. That
keeps producing the definition it then has to discard, and it pushes the question "is this Dag real?" onto every consumer of a serialized Dag — the importer, the persistence layer,
anything later reading the record. Taking the Dag out of the authoring interface answers the question once, at the only point where the answer is known for free: the author already
chose which interface to write against. The only marker left in serialization is `is_stub`, and it is per-task.

The bundle cannot carry the distinction either. The Language SDK spec puts both kinds in one `bundle`, takes them through one `register` verb in any mixture, and serves the process
with one `bundle.serve()`. One artifact can hold both, so neither the file nor the bundle tells a `DagImporter` what it is holding.

### Appendix B — What is compared

Handler declarations from every process spawned in Step 4 are unioned per `dag_id` before comparison, since one Dag's stubs can target several queues.

- `task_id` sets must match exactly. A missing or extra handler is an error.
- `arg_bindings[*].name` against `params[*].name`, in order — both sides bind positionally.
- `arg_bindings[*].value_schema` against `params[*].value_schema`, compared only where neither side is null. An unannotated `@task.stub` parameter produces `null` today, so a
  strict comparison would make every untyped stub argument a parse error.

Any mismatch is reported against the Python file, which is the definition the author can act on, and travels back on `DagFileParsingResult.import_errors` with everything else the
parse found.
