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

# ADR-0010: Mixed-Language Dag Processing — Task Handlers Are Not Dags

## Status

Proposed

## Context

A Lang-SDK artifact can serve either of the two authoring features the Language SDK spec fixes, or both at once:

| Feature                         | Who owns the graph         | Author writes                    | Artifact contributes |
|---------------------------------|----------------------------|----------------------------------|----------------------|
| **Mixed Language Task Handler** | Python, via `@task.stub`   | `TaskHandler(dagId, taskId, fn)` | Only task bodies     |
| **Native Dag**                  | the Lang-SDK source itself | `Dag(spec)`                      | The entire Dag       |

In the mixed-language role the artifact used to author a Dag too, under the same `dag_id` the Python file already owns. Two conflicting definitions reached persistence and
something downstream had to choose between them. This ADR removes the conflict at the authoring interface, and defines how the Python importer validates a stub task against the
handler that implements it.

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
([ADR-0009](0009-native-dag-processing.md)); validation draws on the `TaskHandler` registrations. Because both kinds share one bundle and one `register` verb, the bundle cannot be
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

### Validation is driven by the Python importer

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  is_stub tasks carry arg_bindings (ADR-0007)
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 1: Resolve stub tasks → Coordinator instances via             │
  │  │          CoordinatorManager.for_queue(queue)                        │
  │  │          ([sdk] queue_to_coordinator)                               │
  │  │                                                                     │
  │  │  stub task       queue       Coordinator instance                   │
  │  │  ──────────────────────────────────────────────────                 │
  │  │  extract      →  "jdk-11"  → JavaCoordinator(name="jdk-11")         │
  │  │  transform    →  "jdk-17"  → JavaCoordinator(name="jdk-17")         │
  │  │  load         →  "jdk-11"  → JavaCoordinator(name="jdk-11")         │
  │  │                                                                     │
  │  │  Deduplicate → 2 distinct coordinator instances:                    │
  │  │    JavaCoordinator(name="jdk-11")                                   │
  │  │    JavaCoordinator(name="jdk-17")                                   │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 2: Locate the artifact backing dag_id="etl", no JVM launch —  │
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
  │  │          → Map<dag_id, ResolvedBundle>                              │
  │  │          → "etl" → ResolvedBundle(mainClass=..., classpath=...)     │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-17")                                     │
  │  │    └── (same lookup, its own roots)                                 │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 3: Query the Coordinator — one request, one response          │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-11").parse_task_handler(                 │
  │  │      file=resolved.path, dag_id="etl")                              │
  │  │    │                                                                │
  │  │    ├── _build_parse_task_handler_command()                          │
  │  │    │     → (command, subprocess_schema_version)                     │
  │  │    ├── Spawn JVM, send TaskHandlerParseRequest    (ToRuntime)       │
  │  │    ├── JVM answers from its own TaskHandler registrations           │
  │  │    │     whose dagId is "etl"                                       │
  │  │    ▼                                                                │
  │  │  TaskHandlerParsingResult                      (ToCoordinator)      │
  │  │    → [TaskHandlerDeclaration(task_id, params), ...]                 │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-17")                                     │
  │  │    └── (same call, its own resolved path and handler set)           │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 4: Compare the declarations against the parsed Python Dag —   │
  │  │          union the handlers returned by every coordinator resolved  │
  │  │          in Step 1 before comparing task ids                        │
  │  │                                                                     │
  │  │  Python Dag "etl" (stub tasks)     TaskHandlerDeclaration           │
  │  │  ──────────────────────────────    ──────────────────────────────   │
  │  │  task_id                       ↔   task_id     (sets must match)    │
  │  │  arg_bindings[*].name          ↔   params[*].name    (in order)     │
  │  │  arg_bindings[*].value_schema  ↔   params[*].value_schema           │
  │  │        compared only where neither side is null                     │
  │  │                                                                     │
  │  │  On mismatch → DagImportError                                       │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  ├── Validation passed → Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST (Python Dag is the sole DB record)
```

The `PythonDagImporter` owns validation. It resolves each stub's `queue` to a coordinator, calls `parse_task_handler` ([ADR-0008](0008-lang-sdk-parse-protocol.md)) for the same
`dag_id`, and compares the reply against the Dag it just parsed. No `DagImporter` is involved: this is one request/response, and what comes back is not a Dag.

Resolution goes through the coordinator registry, not the filesystem, so the Python Dag and the Lang-SDK artifact **do not need to be in the same DagBundle**. Nothing here needs an
`airflow.sdk.DAG` round-trip either — validation returns the Dag the Python parser already built. Appendix B states exactly what is compared.

### Decision matrix

| Caller                                       | Coordinator call                             | What comes back                          | Action                                           |
|----------------------------------------------|----------------------------------------------|------------------------------------------|--------------------------------------------------|
| `PythonDagImporter`                          | — (parses the Python file itself)            | its own parsed Dag                       | PERSIST                                          |
| `PythonDagImporter` (via stub → Coordinator) | `parse_task_handler`, scoped to one `dag_id` | `TaskHandlerParsingResult`               | VALIDATE only — not a Dag, so nothing to persist |
| `JavaDagImporter`                            | `parse_dag`                                  | `DagFileParsingResult`, native Dags only | PERSIST                                          |

There is no fourth row. A `TaskHandlerRef` has no Dag, so no `DagImporter` — and nothing reading a `DagImporter`'s results — ever sees one.

## Consequences

- Python leads, Lang-SDK follows. `PythonDagImporter` persists the Dag and drives validation via `queue → Coordinator → parse_task_handler`.
- A mixed-language `dag_id` never appears in Dag processing results. No `Dag` registration exists for a `dag_id` a Python file already owns, so everything downstream sees exactly
  one record per `dag_id`, with no flag to interpret.
- Stub/implementation mismatches — missing handler, extra handler, parameter name or order, incompatible schema — surface as `DagImportError` at parse time. An unannotated stub
  argument is checked by name and position only.
- The Python Dag and Lang-SDK artifact can live in different DagBundles.
- A single Dag can have stubs targeting different queues, some Java, some Go. Each resolves to its own coordinator instance, and validation unions their declarations before
  comparing task ids.
- Mixed-language is Python-primary only. Lang-SDK runtimes cannot define stub operators; a native Dag cannot delegate tasks to Python.
- No per-Dag flag, no schema migration, no new `DagModel` column, no REST/UI change.
- Terms track Language SDK spec `1.0`. A spec rename of `TaskHandler`, or of the `register` / `serve` verbs, lands here too.

## References

- [ADR-0008](0008-lang-sdk-parse-protocol.md) — `parse_task_handler` and the `TaskHandlerParsingResult` shape this ADR compares against
- [ADR-0009](0009-native-dag-processing.md) — the `Dag`-registration half, and the importer that persists it
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`, build-time artifact inventory
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display for mixed-language Dags
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` / `TaskArgBinding` / `ArgValueSchema`
- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`)
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter

## Appendix

### Appendix A — Why the split lives on the interface

The alternative was to keep authoring the Dag on both sides and mark the Lang-SDK copy with a per-Dag flag, `is_mixed_language_dag`, so importers knew which copy to drop. That
keeps producing the definition it then has to discard, and it pushes the question "is this Dag real?" onto every consumer of a serialized Dag — the importer, the persistence layer,
anything later reading the record. Taking the Dag out of the authoring interface answers the question once, at the only point where the answer is known for free: the author already
chose which interface to write against. The only marker left in serialization is `is_stub`, and it is per-task.

The bundle cannot carry the distinction either. The Language SDK spec puts both kinds in one `bundle`, takes them through one `register` verb in any mixture, and serves the process
with one `bundle.serve()`. One artifact can hold both, so neither the file nor the bundle tells a `DagImporter` what it is holding.

### Appendix B — What is compared

Handler declarations from every coordinator resolved in Step 1 are unioned before comparison, since one Dag's stubs can target several queues.

- `task_id` sets must match exactly. A missing or extra handler is an error.
- `arg_bindings[*].name` against `params[*].name`, in order — both sides bind positionally.
- `arg_bindings[*].value_schema` against `params[*].value_schema`, compared only where neither side is null. An unannotated `@task.stub` parameter produces `null` today, so a
  strict comparison would make every untyped stub argument a parse error.

Any mismatch is raised as `DagImportError` against the Python file, which is the definition the author can act on.
