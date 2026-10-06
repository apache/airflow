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

# ADR-0011: Mixed-Language Dag Processing — Task Handlers Are Not Dags

## Status

Proposed

## Context

A Lang-SDK artifact can serve either of the two authoring features the Language SDK spec fixes, or both at once:

| Feature                         | Who owns the graph         | Author writes                | Artifact contributes |
|---------------------------------|----------------------------|------------------------------|----------------------|
| **Mixed Language Task Handler** | Python, via `@task.stub`   | `TaskHandler(dag, task, fn)` | Only task bodies     |
| **Native Dag**                  | the Lang-SDK source itself | `Dag(spec)`                  | The entire Dag       |

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
│    TaskHandler(dag, task, fn)      → TaskHandlerRef                      │
│                                                                          │
│  Both kinds go into one bundle, through one verb:                        │
│    bundle.register(dag, handler, ...)  →  bundle.serve()                 │
└──────────────────────────────────────────────────────────────────────────┘
```

A `TaskHandlerRef` has no schedule, no task graph, and no `dag_id` of its own to persist. A `DagRef` has all three. Dag parsing draws on the `Dag` registrations
([ADR-0010](0010-native-dag-processing.md)); validation draws on the `TaskHandler` registrations. Because both kinds share one bundle and one `register` verb, the bundle cannot be
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
        │  │  Step 1: Group this file's stub tasks by the coordinator their      │
        │  │          queue routes to and by their Dag. A stub task on an        │
        │  │          unrouted queue is left to a worker outside Airflow's       │
        │  │          coordinators.                                              │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 2: Resolve stub tasks → Coordinator instances via             │
        │  │          get_coordinator_key(queue), then get_coordinator(key)      │
        │  │          since the old lookup built a Python coordinator for an     │
        │  │          unrouted queue instead ([sdk] queue_to_coordinator)        │
        │  │                                                                     │
        │  │  stub task       queue       Coordinator instance                   │
        │  │  ──────────────────────────────────────────────────────             │
        │  │  extract      →  "jdk-11"  → JavaCoordinator(name="jdk-11")         │
        │  │  transform    →  "jdk-17"  → JavaCoordinator(name="jdk-17")         │
        │  │  load         →  "jdk-11"  → JavaCoordinator(name="jdk-11")         │
        │  │                                                                     │
        │  │  Deduplicate → 2 distinct coordinator instances                     │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 3: Find the artifact backing each (coordinator, Dag)          │
        │  │          with the coordinator's own scan: the artifact bundle       │
        │  │          named by task_handler_bundle_name, or the Dag's own        │
        │  │          bundle when unset; another team leaves it unchecked        │
        │  │                                                                     │
        │  │  JavaCoordinator(name="jdk-11")                                     │
        │  │    └── _find_task_handler_artifact(bundle_path, dag_id="etl")       │
        │  │          re-runs the scan _build_execute_task_command uses,         │
        │  │          so the artifact probed is the one a worker would run       │
        │  │          → ResolvedBundle(analytics.jar, schema_version)            │
        │  │                                                                     │
        │  │  Group by (coordinator, artifact): one group, one process           │
        │  └─────────────────────────────────────────────────────────────────────┘
        │
        │  ┌─────────────────────────────────────────────────────────────────────┐
        │  │  Step 4: Probe each group: one request, one response                │
        │  │                                                                     │
        │  │  LangSDKTaskHandlerProcessorProcess.run(                            │
        │  │      coordinator="jdk-11", path=analytics.jar,                      │
        │  │      bundle_path=..., bundle_name=..., artifact_rel_path=...,       │
        │  │      deadline=...)                                                  │
        │  │    │                                                                │
        │  │    ├── in the child: _build_parse_task_handler_command()            │
        │  │    │                 coordinator.parse_task_handler() — spawn JVM   │
        │  │    │                                                                │
        │  │    │   ──TaskHandlerParseRequest(file=analytics.jar)─────▶ JVM      │
        │  │    │                            (ToSDKTaskHandlerProcessor)         │
        │  │    │                                                                │
        │  │    │      JVM answers with every TaskHandler registration           │
        │  │    │      in the artifact, or {} when there is none                 │
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
        │  │  Step 5: Check each stub task against the one answer for its        │
        │  │          coordinator and its Dag                                    │
        │  │                                                                     │
        │  │  Python Dag "etl" (stub tasks)     TaskHandlerDeclaration           │
        │  │  ──────────────────────────────    ──────────────────────────────   │
        │  │  (dag_id, task_id)             ↔   its declaration in that answer   │
        │  │        none → an error                                              │
        │  │        a handler with no stub task → not an error                   │
        │  │  arg_bindings[*]               ↔   params[*]   (per binding)        │
        │  │        by position, or by folded or exact name                      │
        │  │        defaulted arguments dropped if that makes the count match    │
        │  │        unmatched by name → a warning, not an error                  │
        │  │  arg_bindings[*].value_schema  ↔   params[*].value_schema           │
        │  │        top-level JSON types, where both sides have a schema         │
        │  │  a mapped stub task, or params=None → the handler's presence only   │
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
every `dag_id` that resolved to the same artifact ([ADR-0012](0012-lang-sdk-parse-protocol.md)).

Resolution goes through the coordinator registry, not the filesystem, so the Python Dag and the Lang-SDK artifact **do not need to be in the same DagBundle**. Nothing here needs an
`airflow.sdk.DAG` round-trip either — validation compares against the Dag the Python parser already built. Appendix B states exactly what is compared.

### Decision matrix

| Caller                                     | Coordinator call                                     | What comes back                          | Action                                           |
|--------------------------------------------|------------------------------------------------------|------------------------------------------|--------------------------------------------------|
| `_parse_file` → `PythonDagImporter`        | — (the Python file is parsed in process)             | its own parsed Dags                      | PERSIST                                          |
| `_parse_file`, per (coordinator, artifact) | `parse_task_handler`, for every handler it registers | `TaskHandlerParsingResult`               | VALIDATE only — not a Dag, so nothing to persist |
| `_parse_file` → `JavaDagImporter`          | `parse_dag`                                          | `DagFileParsingResult`, native Dags only | PERSIST                                          |

There is no fourth row. A `TaskHandlerRef` has no Dag, so no `DagImporter` — and nothing reading a `DagImporter`'s results — ever sees one.

## Consequences

- Python leads, Lang-SDK follows. The Dag-file parse persists the Dag and drives validation via `queue → Coordinator → parse_task_handler`.
- No importer knows about coordinators. `PythonDagImporter` is unchanged by this ADR; the stub-to-handler comparison sits in `_parse_file`, above every importer.
- A mixed-language `dag_id` never appears in Dag processing results. No `Dag` registration exists for a `dag_id` a Python file already owns, so everything downstream sees exactly
  one record per `dag_id`, with no flag to interpret.
- Stub/implementation mismatches, such as a missing handler, a `positional` argument count that does not match or an incompatible schema,
  surface as import errors against the Python file at parse time, alongside the errors the parse already reports.
  A `named` argument or parameter that matches nothing is only logged as a warning, because the runtime runs the task anyway.
  An unannotated stub argument is checked only for how it binds, by position or by name.
- The Python Dag and Lang-SDK artifact can live in different DagBundles.
- A single Dag can have stubs targeting different queues, some Java, some Go. Each resolves to its own coordinator instance,
  and each stub task is checked only against the answer for its own coordinator and Dag.
- Validating a file costs one extra process per (coordinator, artifact) pair its stubs resolve to — one for the common case of a file whose stubs all target a single runtime, and
  none at all for a file with no stub tasks. The scan that picks the artifact also runs on every parse: Go hashes each candidate binary, Java reads every JAR's manifest.
- Only a mismatch a probe answer proves is an import error; a missing artifact, a bundle of another team, an `[sdk]` or coordinator failure, and a failed, timed-out or skipped
  probe are warnings in the parse log instead, so a Dag processor without the artifacts cannot stop Dags that already run.
- The check ends at 90% of `[dag_processor] dag_file_processor_timeout`, counted from the creation of the parse child, and each probe is also bounded by
  `[core] dagbag_import_timeout`, or the `get_dagbag_import_timeout` policy, called with the artifact's path.
- Mixed-language is Python-primary only. Lang-SDK runtimes cannot define stub operators; a native Dag cannot delegate tasks to Python.
- No per-Dag flag, no schema migration, no new `DagModel` column, no REST/UI change.
- Terms track Language SDK spec `1.0`. A spec rename of `TaskHandler`, or of the `register` / `serve` verbs, lands here too.

## References

- [ADR-0012](0012-lang-sdk-parse-protocol.md) — `parse_task_handler` and the `TaskHandlerParsingResult` shape this ADR compares against
- [ADR-0010](0010-native-dag-processing.md) — the `Dag`-registration half, and the importer that persists it
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

Each stub task is compared only against the answer for its own coordinator and Dag, since one Dag's stubs can target several queues.
An answer covers every Dag its artifact registers handlers for; only the parsed file's stub tasks are looked up in it.

- Every stub task needs its handler in that answer, the declaration of its `(dag_id, task_id)`. A stub task with no such declaration is an error.
  A handler with no stub task is not an error: one artifact serves many Dag files, and a handler can outlive its stub task.
- `arg_bindings` are checked against `params` the way the declaration's `binding` binds them, and `arg_bindings[*].value_schema` against
  `params[*].value_schema` where both sides give one; [ADR-0012](0012-lang-sdk-parse-protocol.md) Appendix B states those rules.
  An argument count or value type a parameter does not accept is an error; under `named`, an argument or parameter that matches nothing is only a warning.
  A mapped stub task, whose arguments are not captured at parse time, and a declaration whose `params` is `None` are checked only for the handler's presence.
- A group the parse could not get an answer for (no artifact, a bundle of another team, an `[sdk]` or coordinator failure, or a failed, timed-out or skipped probe) is only logged;
  none of its stub tasks are compared, so none of them can be named in an error.

Every error is reported against the Python file, which is the definition the author can act on, and travels back on `DagFileParsingResult.import_errors` with everything else the
parse found.
