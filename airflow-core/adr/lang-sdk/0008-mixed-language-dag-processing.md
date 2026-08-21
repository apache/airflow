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

| Role                    | Dag defined in                      | Artifact contributes      |
|-------------------------|-------------------------------------|---------------------------|
| **Mixed-language Dag**  | Python file with `@task.stub` tasks | Only task implementations |
| **Native Lang-SDK Dag** | The Go/Java source itself           | The entire Dag            |

Persisting both the Python Dag and the Lang-SDK Dag for the same `dag_id`
creates conflicting definitions. The `is_mixed_language_dag` flag
([PR #71213](https://github.com/apache/airflow/pull/71213)) disambiguates them.

This ADR uses `JavaDagImporter` as the concrete example. `ExecutableDagImporter`
(Go) and `NodeDagImporter` (TypeScript) follow the same flow. A common base
class is expected once the second Lang-SDK importer lands.

## Decision

### The Flag

```
┌──────────────────────────────────────────────────────────────┐
│  Per-task: is_stub                                           │
│    AbstractOperator.is_stub = False  (default)               │
│    _StubOperator.is_stub   = True                            │
│                                                              │
│  Per-Dag: is_mixed_language_dag                              │
│    Python side — derived at serialization:                   │
│      if any(task.is_stub for task in dag.tasks): True        │
│    Lang-SDK side — set explicitly by the producer            │
└──────────────────────────────────────────────────────────────┘
```

Both sides carry `is_mixed_language_dag: true`, so the flag alone cannot decide
persistence. The decision is `(flag × importer)`.

### Processing Flows

#### Flow A — Pure Python Dag (no `@task.stub`)

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  is_mixed_language_dag absent
  ├── Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST
```

#### Flow B — Lang-SDK Importer (native and mixed)

`JavaDagImporter` **never** persists a Dag with `is_mixed_language_dag: true`.

```
JavaDagImporter.import_definition(definition, bundle=...)
  │
  ├── BaseCoordinator.run_dag_parsing()
  │     └── Spawn JVM → DagFileParsingResult
  │           ┌─────────────────────────────────────────┐
  │           │  dag_id: "etl"                          │
  │           │  is_mixed_language_dag: true            │
  │           ├─────────────────────────────────────────┤
  │           │  dag_id: "java_report"                  │
  │           │  is_mixed_language_dag: false           │
  │           └─────────────────────────────────────────┘
  │
  ├── "etl"          is_mixed=true   → DISCARD + log info
  ├── "java_report"  is_mixed=false  → PERSIST
  │
  ├── Return DagImportResult(dags=["java_report"])
  ▼
DagModelOperation → PERSIST "java_report" only
```

#### Flow C — Mixed-Language Dag (validation driven by PythonDagImporter)

The `PythonDagImporter` owns validation. When it finds `is_mixed_language_dag:
true`, it resolves each stub task's `queue` to its Coordinator, then uses the
Coordinator's DagImporter to fetch the Lang-SDK Dag structure for
cross-validation.

Resolution goes through the coordinator registry, not the filesystem — the Dag
processor is file-at-a-time and the `PythonDagImporter` never sees the `.jar`.
This also means the Python Dag and the Lang-SDK artifact **do not need to be in
the same DagBundle**.

```
PythonDagImporter.import_definition(definition, bundle=...)
  │
  ├── Parse → DAG objects
  ├── serialize_dag(dag)  →  is_mixed_language_dag = true
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
  │  │  Step 2: For each Coordinator, get its DagImporter and              │
  │  │          list_dag_definitions to find the matching Dag              │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-11")                                     │
  │  │    │                                                                │
  │  │    ├── Get artifact root:                                           │
  │  │    │     coordinator.dag_bundle                                     │
  │  │    │       → BaseDagBundle for "java-jdk11-bundle"                  │
  │  │    │     OR coordinator.artifact_roots                              │
  │  │    │       → [Path("/opt/airflow/jars/jdk11/")]                     │
  │  │    │                                                                │
  │  │    ├── JavaDagImporter.list_dag_definitions(                        │
  │  │    │       bundle=coordinator.dag_bundle,                           │
  │  │    │       safe_mode=True)                                          │
  │  │    │     → Iterator[DagDefinition]                                  │
  │  │    │         DagDefinition("app-jdk11.jar")                         │
  │  │    │         DagDefinition("utils-jdk11.jar")                       │
  │  │    │         ...                                                    │
  │  │    │                                                                │
  │  │    └── JavaDagImporter.import_definition(                           │
  │  │            definition,                                              │
  │  │            bundle=coordinator.dag_bundle,                           │
  │  │            discover_mixed_language_dags=True)                       │
  │  │          → DagImportResult with Dag structure for dag_id="etl"      │
  │  │                                                                     │
  │  │  JavaCoordinator(name="jdk-17")                                     │
  │  │    └── (same flow, different artifact root / bundle)                │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  │  ┌─────────────────────────────────────────────────────────────────────┐
  │  │  Step 3: Validate stub tasks against Lang-SDK tasks                 │
  │  │                                                                     │
  │  │  Python Dag "etl"          Lang-SDK Dag "etl"                       │
  │  │  stub tasks:               tasks:                                   │
  │  │    extract  ─────────────  extract          ✓ match                 │
  │  │    transform  ───────────  transform        ✓ match                 │
  │  │    load  ────────────────  load             ✓ match                 │
  │  │                                                                     │
  │  │  On mismatch → DagImportError                                       │
  │  └─────────────────────────────────────────────────────────────────────┘
  │
  ├── Validation passed → Return DagImportResult(dags=[dag])
  ▼
DagModelOperation → PERSIST (Python Dag is the sole DB record)
```

### Decision Matrix

```
┌───────────────────┬───────────────────────┬──────────────────────────────────────────────────────────────┐
│ Importer          │ is_mixed_language_dag │ Action                                                       │
├───────────────────┼───────────────────────┼──────────────────────────────────────────────────────────────┤
│ PythonDagImporter │ false                 │ PERSIST                                                      │
├───────────────────┼───────────────────────┼──────────────────────────────────────────────────────────────┤
│ PythonDagImporter │ true                  │ PERSIST + VALIDATE via queue → Coordinator →                 │
│                   │                       │ DagImporter.list_dag_definitions + import_definition         │
├───────────────────┼───────────────────────┼──────────────────────────────────────────────────────────────┤
│ JavaDagImporter   │ false (or absent)     │ PERSIST                                                      │
├───────────────────┼───────────────────────┼──────────────────────────────────────────────────────────────┤
│ JavaDagImporter   │ true                  │ DISCARD + log info                                           │
└───────────────────┴───────────────────────┴──────────────────────────────────────────────────────────────┘
```

### Why the Flag Lives on the Dag, Not the Bundle

A single artifact can contain both native and mixed-language Dags:

```
analytics.jar
├── dag_id="etl"          is_mixed_language_dag=true  (backs etl.py)
└── dag_id="java_report"  is_mixed_language_dag=false (native)
```

The importer decides per-Dag, not per-file or per-bundle.

## Consequences

- **Python leads, Lang-SDK follows.** `PythonDagImporter` persists the Dag and
  drives validation via `queue → Coordinator → DagImporter.list_dag_definitions
  → import_definition`. Lang-SDK importers silently discard mixed-language Dags.
- Stub/implementation mismatches surface as `DagImportError` at parse time.
- The Python Dag and Lang-SDK artifact can live in different DagBundles —
  resolution goes through the coordinator registry, not the filesystem.
- A single Dag can have stubs targeting different queues (some Java, some Go) —
  each resolves to its own coordinator instance independently.
- **Mixed-language is Python-primary only.** Lang-SDK runtimes cannot define stub
  operators — the reverse direction (a native Lang-SDK Dag delegating tasks to
  Python) is not supported.
- No schema migration, no new `DagModel` column, no REST/UI change.

## References

- [ADR-0004](0004-dag-parsing.md) — `BaseCoordinator` / `DagFileParsingResult`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
  for mixed-language Dags
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
- [PR #71213](https://github.com/apache/airflow/pull/71213) — the
  `is_mixed_language_dag` flag
