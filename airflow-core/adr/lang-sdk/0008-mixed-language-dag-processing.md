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

# ADR-0008: Mixed-Language Dag Processing — DagImporter Routing and Persistence

## Status

Proposed

## Context

A Lang-SDK artifact (JAR, packed Go executable) can serve two roles:

| Role                    | Dag defined in                      | Author writes   | Artifact contributes      |
|-------------------------|-------------------------------------|-------------------|----------------------------|
| **Mixed-language Dag**  | Python file with `@task.stub` tasks | `@MixedLangDag`   | Only task implementations |
| **Native Lang-SDK Dag** | The Go/Java source itself           | `@Dag`            | The entire Dag            |

Persisting both the Python Dag and the Lang-SDK Dag for the same `dag_id`
creates conflicting definitions. [PR #71213](https://github.com/apache/airflow/pull/71213)
disambiguated them with a per-Dag `is_mixed_language_dag` flag. This ADR
replaces that flag with an Dag authoring level distinction — `@Dag` vs.
`@MixedLangDag` in the case of Java SDK — plus a `DagFileParsingRequest` field, for the reasons below.

This ADR uses `JavaDagImporter` and `JavaCoordinator` as the concrete examples.
`ExecutableDagImporter` (Go) and `NodeDagImporter` (TypeScript) follow the same
flow, each backed by its own `BaseCoordinator` subclass. A common base class is
expected once the second Lang-SDK importer lands.

## Decision

### The Annotation, Not a Flag

```
┌──────────────────────────────────────────────────────────────┐
│  Per-task: is_stub                                           │
│    AbstractOperator.is_stub = False  (default)               │
│    _StubOperator.is_stub   = True                            │
│                                                              │
│  Per-Dag: which annotation the Lang-SDK author applied       │
│    @Dag           — native, the entire Dag        (existing) │
│    @MixedLangDag  — tasks only, backs a Python     (new)     │
│                      file's `@task.stub` operators           │
└──────────────────────────────────────────────────────────────┘
```

`@Dag` (`Builder.Dag`) and `@MixedLangDag` (`Builder.MixedLangDag`, new) drive
the same `BuilderProcessor` annotation processor
([ADR-0003](0003-pure-java-dags.md)) — each generates a `*Builder` whose
`build()` still returns a `DagDef`; `BundleBuilder.getDags(): Iterable<DagDef>`
is unchanged. What differs is which annotation produced it, recorded on the
generated builder itself, so the JVM's parsing entrypoint tells native and
mixed-language Dags apart without a field carried on the Dag object. The
decision is `(annotation × importer)`.

### Processing Flows

#### Flow A — Pure Python Dag (no `@task.stub`)

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  no stub tasks, nothing to cross-validate
  ├── Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST
```

#### Flow B — Lang-SDK Importer (native and mixed)

`JavaDagImporter` **never** persists a Dag produced by `@MixedLangDag`.

```
JavaDagImporter.import_definition(definition, bundle=...)
  │
  ├── BaseCoordinator.run_dag_parsing(
  │       request=DagFileParseRequest(file=..., mixed_language_dags_only=False))
  │     └── Spawn JVM → JVM filters locally: only @Dag-produced entries are serialized
  │           ┌─────────────────────────────────────────┐
  │           │  dag_id: "java_report"                  │   (@Dag)
  │           └─────────────────────────────────────────┘
  │         "etl" (@MixedLangDag) never leaves the JVM — nothing to discard.
  │
  ├── Return DagImportResult(dags=["java_report"])
  ▼
DagModelOperation → PERSIST "java_report" only
```

#### Flow C — Mixed-Language Dag (validation driven by PythonDagImporter)

The `PythonDagImporter` owns validation. When it finds a stub task, it
resolves the stub's `queue` to its Coordinator, then asks that Coordinator
**directly** for the `@MixedLangDag`-produced Dag structure backing the same
`dag_id` — no `DagImporter` involved, since this is a single parse
request/response, not a discovery operation. Either path bottoms out at the
Coordinator sending a `DagFileParseRequest` and getting back a
`DagFileParsingResult` ([ADR-0004](0004-dag-parsing.md)), so validation calls
that primitive directly instead of detouring through the importer contract.

Resolution goes through the coordinator registry, not the filesystem — the Dag
processor is file-at-a-time and the `PythonDagImporter` never sees the `.jar`.
This also means the Python Dag and the Lang-SDK artifact **do not need to be in
the same DagBundle**.

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  is_stub tasks carry arg_bindings (ADR-0007)
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 1: Resolve stub tasks → Coordinator instances via queue       │
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
  │  │  Step 2: Locate the file backing dag_id="etl", no JVM launch —      │
  │  │          reuse BundleScanner (ADR-0003), which reads JAR manifests  │
  │  │          directly                                                   │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-11")                                     │
  │  │    ├── Get artifact root:                                           │
  │  │    │     coordinator.dag_bundle                                     │
  │  │    │       → BaseDagBundle for "java-jdk11-bundle"                  │
  │  │    │     OR coordinator.artifact_roots                              │
  │  │    │       → [Path("/opt/airflow/jars/jdk11/")]                     │
  │  │    │                                                                │
  │  │    └── BundleScanner.scanBundles(artifact_root)                     │
  │  │          → Map<dag_id, ResolvedBundle>                              │
  │  │          → "etl" → ResolvedBundle(mainClass=..., classpath=...)     │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-17")                                     │
  │  │    └── (same lookup, different artifact root)                       │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 3: Call the Coordinator directly — the same primitive         │
  │  │          ADR-0004 already defines for ordinary parsing              │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-11").run_dag_parsing(                    │
  │  │      path=resolved.path, bundle_name=..., bundle_path=...,          │
  │  │      request=DagFileParseRequest(                                   │
  │  │          file=resolved.path,                                        │
  │  │          mixed_language_dags_only=True))                            │
  │  │    │                                                                │
  │  │    ├── Spawn JVM → sends DagFileParseRequest over the bridge        │
  │  │    ├── JVM filters locally: only @MixedLangDag entries serialized   │
  │  │    ▼                                                                │
  │  │  DagFileParsingResult → deserialize → LazyDeserializedDAG "etl"     │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-17")                                     │
  │  │    └── (same call, different resolved path)                         │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 4: Compare directly against the parsed Python Dag — the       │
  │  │          same LazyDeserializedDAG model the scheduler reads         │
  │  │                                                                     │
  │  │  Python Dag "etl" (stub tasks)      LazyDeserializedDAG "etl"       │
  │  │  ───────────────────────────────    ─────────────────────────       │
  │  │  task_id                       ↔    task_id        (sets must match)│
  │  │  arg_bindings[*].name          ↔    declared param name             │
  │  │  arg_bindings[*].value_schema  ↔    declared param schema           │
  │  │                                                                     │
  │  │  On mismatch → DagImportError                                       │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  ├── Validation passed → Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST (Python Dag is the sole DB record)
```

### Decision Matrix

| Caller                                        | Produced by            | `mixed_language_dags_only` | Action                                                             |
|------------------------------------------------|------------------------|------------------------------|---------------------------------------------------------------------|
| `PythonDagImporter`                             | its own parsed Dag    | n/a                          | PERSIST                                                              |
| `PythonDagImporter` (via stub → Coordinator)    | `@MixedLangDag`        | `True`                       | VALIDATE only — compared against the Python Dag, never persisted   |
| `JavaDagImporter`                                | `@Dag`                 | `False` (default)            | PERSIST                                                              |
| `JavaDagImporter`                                | `@MixedLangDag`        | `False` (default)            | never returned — filtered inside the JVM                            |

### Why the Distinction Lives on the Annotation, Not the Bundle

A single artifact can contain both native and mixed-language Dags:

```
analytics.jar  (one BundleBuilder.getDags() call)
├── EtlTasks            @MixedLangDag(id = "etl")      (backs etl.py's stub tasks)
└── JavaReportPipeline  @Dag(id = "java_report")        (native)
```

The importer decides per-Dag, not per-file or per-bundle — and it decides by
which annotation authored each class, not by inspecting a field on the
resulting Dag.

## Consequences

- **Python leads, Lang-SDK follows.** `PythonDagImporter` persists the Dag and
  drives validation via `queue → Coordinator → run_dag_parsing
  (mixed_language_dags_only=True)`, comparing the resulting
  `LazyDeserializedDAG` directly against its own parsed Dag. Lang-SDK importers
  never receive an `@MixedLangDag`-produced Dag to begin with — the JVM
  filters it out of the response before anything crosses the wire.
- Stub/implementation mismatches (missing task, extra task, parameter name or
  schema mismatch) surface as `DagImportError` at parse time.
- The Python Dag and Lang-SDK artifact can live in different DagBundles —
  resolution goes through the coordinator registry, not the filesystem.
- A single Dag can have stubs targeting different queues (some Java, some Go) —
  each resolves to its own coordinator instance independently.
- **Mixed-language is Python-primary only.** Lang-SDK runtimes cannot define stub
  operators — the reverse direction (a native Lang-SDK Dag delegating tasks to
  Python) is not supported.
- **No `is_mixed_language_dag` flag.** This ADR supersedes that part of
  [PR #71213](https://github.com/apache/airflow/pull/71213): the role is
  encoded in which annotation authored the class (`@Dag` vs.
  `@MixedLangDag`) and in the `DagFileParsingRequest.mixed_language_dags_only`
  field, not in a value carried on the serialized Dag itself.
- No schema migration, no new `DagModel` column, no REST/UI change.

## References

- [ADR-0003](0003-pure-java-dags.md) — `BundleBuilder` / `BundleScanner` /
  `BuilderProcessor` (`@Dag` / `@Task` annotation processing)
- [ADR-0004](0004-dag-parsing.md) — `BaseCoordinator` / `DagFileParsingResult`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
  for mixed-language Dags
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` /
  `TaskArgBinding`, compared against `@MixedLangDag` task signatures in Flow C
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
- [PR #71213](https://github.com/apache/airflow/pull/71213) — introduced the
  per-Dag `is_mixed_language_dag` flag, superseded by this ADR's type-level
  distinction
