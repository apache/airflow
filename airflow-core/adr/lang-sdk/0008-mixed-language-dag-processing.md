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

A Lang-SDK artifact (JAR, packed Go executable) can serve either of the two
authoring features the Language SDK spec fixes, or both at once:

| Feature                         | Who owns the graph         | Author writes                    | Artifact contributes |
|---------------------------------|----------------------------|----------------------------------|----------------------|
| **Mixed Language Task Handler** | Python, via `@task.stub`   | `TaskHandler(dagId, taskId, fn)` | Only task bodies     |
| **Native Dag**                  | the Lang-SDK source itself | `Dag(spec)`                      | The entire Dag       |

In the mixed-language role the artifact used to author a Dag too — under the
same `dag_id` the Python file already owns — so two conflicting definitions
reached persistence and something downstream had to choose between them. This
ADR removes the conflict at the authoring interface instead of resolving it
afterwards: an artifact backing `@task.stub` tasks registers **task handlers**,
which are not Dags, so no second definition for that `dag_id` is ever produced.

Terms follow the Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`, spec
version `1.0`): `Dag`, `TaskHandler`, `DagRef`, `TaskHandlerRef`, `bundle`,
`register`, `serve`. The examples use the Java and Go spellings
(`@Builder.TaskHandler` / `airflow.TaskHandler`); TypeScript spells the same
terms in its own idiom. Note that the spec's `bundle` is the SDK-side
registration container, not Airflow's `DagBundle` — both appear in this ADR.

`JavaDagImporter` and `JavaCoordinator` are the concrete examples throughout.
`ExecutableDagImporter` (Go) and `NodeDagImporter` (TypeScript) follow the same
flow, each pairing its own `SubprocessCoordinator` subclass with its own
`AbstractDagImporter` implementation.

## Decision

### Differentiate at the Authoring Interface

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

`TaskHandler` is a separate interface, not a second flavour of `Dag`. It names
the `(dagId, taskId)` pair Python already declared and the `fn` that implements
it, and returns a `TaskHandlerRef`: no schedule, no task graph, no `dag_id` of
its own to persist. `Dag(spec)` returns a `DagRef`, which owns all three. The
spelling differs per SDK; the split does not.

The spec puts both kinds in one `bundle`, takes them through one `register` verb
in any mixture, and serves the process with one `bundle.serve()`. So the bundle
cannot be the discriminator — and does not need to be. Dag parsing draws on the
`Dag` registrations; handler validation draws on the `TaskHandler`
registrations. Each registration already says which kind it is.

The alternative was to keep authoring the Dag on both sides and mark the
Lang-SDK copy with a per-Dag flag (`is_mixed_language_dag`) so importers know
which copy to drop. That keeps producing the definition it then has to discard,
and pushes the question "is this Dag real?" onto every consumer of a serialized
Dag. Taking the Dag out of the authoring interface answers that question once,
for everyone. The only marker left in serialization is `is_stub`, and it is
per-task.

### Handler Declarations Travel on Their Own Channel

Handler declarations are carried by a dedicated message pair in their own
discriminated unions — `ToRuntime` / `ToCoordinator` — not by a field added to
`DagFileParseRequest`.

Union membership is the only registration step in the supervisor schema
package: it is what puts a body into the generated snapshot, and therefore into
every SDK's generated models. So the choice of union *is* the wire-contract
decision. Neither existing pair fits, because the runtime plays two roles. When
it parses native Dags it **replaces** the parser process and answers the
manager, which the manager ↔ parser unions already describe. When it answers a
handler query it is a short-lived child of the Python parser, and the manager is
not a peer at all. Reusing the parser unions there would make one union mean two
recipients.

The payload is deliberately a stronger contract than the one it parallels.
`DagFileParsingResult.serialized_dags` is `list[LazyDeserializedDAG]`, which
reduces in the snapshot to an opaque object — which is why every SDK
reimplements DagSerialization by hand and validates it against shared fixtures
([ADR-0004](0004-dag-parsing.md)). A handler declaration carries no Dag, so it
is fully typed, code-generated, and schema-validated in every SDK. **The
mixed-language path never asks a Lang SDK to implement DagSerialization at
all.**

Parameter schemas reuse the existing `ArgValueSchema` definition that
`arg_bindings` already carries ([ADR-0007](0007-taskflow-across-language-boundary.md)),
so validation compares like against like rather than translating between two
vocabularies. Appendix A gives the message definitions and the two properties of
that field — nullability and ordering — that validation has to respect.

### Two Parse Verbs, Because There Are Two Conversations

`BaseCoordinator` gains `parse_dag` and `parse_task_handler` alongside the
shipped `execute_task`. They are **not symmetric siblings**, which is the
reason not to collapse them into one `parse(request)`:

- `parse_dag` is **bridge mode**. The coordinator becomes the parser process
  and raw-forwards the manager's request and the runtime's reply. It never
  reads the payload.
- `parse_task_handler` is **query mode**. The coordinator is itself the peer:
  it sends one request and decodes one reply on the channel above.

One collapsed method would also erase a real deployment distinction — a
coordinator can serve mixed-language handlers with no interest in native Dag
parsing, and an absent method says so better than a runtime rejection.
Appendix B gives the layering and the command hooks.

### A Lang-SDK DagImporter Comes From Its Coordinator

`AbstractDagImporter` (AIP-85) is about source formats and returns
`DagImportResult.dags: list[DAG]`, i.e. `airflow.sdk.DAG` objects. The
coordinator is about processes. A Lang-SDK importer composes one, and the
coordinator is what hands it out — `JavaCoordinator.get_dag_importer()` returns
a `JavaDagImporter` already bound to that coordinator. An operator therefore
never configures an importer's coordinator: that wiring is an implementation
detail of the coordinator that produced it, and `[sdk] coordinators` stays the
one place a runtime is declared.

Importers are keyed by file extension, one per extension, so two
`JavaCoordinator` instances on different JDKs would both claim `.jar`. The
boundary that resolves this is deployment-shaped rather than a tie-break: **a
coordinator instance owns a DagBundle.** `SubprocessCoordinator` already
classifies that ownership at construction, and only its `NAMED_BUNDLE` mode
names a bundle — which makes `NAMED_BUNDLE` the mode a deployment running two
runtimes of the same language has to use. Appendix C covers the registration
tiers, the three artifact-source modes, and extensionless artifacts.

### Processing Flows

Three flows, walked through in full in Appendix D.

**Flow A — pure Python Dag.** No stub tasks, nothing to cross-validate.
`PythonDagImporter` parses and returns; the Dag is persisted.

**Flow B — native Lang-SDK Dag.** `JavaDagImporter` calls `parse_dag`, and the
runtime serializes its `Dag` registrations. `TaskHandler` registrations have no
Dag to serialize, so no mixed-language `dag_id` is ever a candidate for
persistence — there is nothing to filter. Because `DagImportResult.dags` is
`list[DAG]`, the importer wraps each serialized entry as a
`LazyDeserializedDAG` and transforms it into an `airflow.sdk.DAG` before
returning. That transform is mechanical, but `LazyDeserializedDAG` lives in
`airflow-core` today, so where a Task SDK importer reaches it from is settled by
AIP-85's own `list[DAG]` / `list[LazyDeserializedDAG]` discussion rather than
here.

**Flow C — mixed-language Dag.** The `PythonDagImporter` owns validation. When
it finds a stub task it resolves the stub's `queue` to a coordinator, calls
`parse_task_handler` for the same `dag_id`, and compares the returned
declarations against the Dag it just parsed. No `DagImporter` is involved —
this is a single request/response rather than a discovery operation, and what
comes back is not a Dag. Resolution goes through the coordinator registry, not
the filesystem, so the Python Dag and the Lang-SDK artifact **do not need to be
in the same DagBundle**. Flow C needs no `sdk.DAG` round-trip at all: it
returns the Dag the Python parser already built.

### Decision Matrix

| Caller                                       | Coordinator call                             | What comes back                          | Action                                           |
|----------------------------------------------|----------------------------------------------|------------------------------------------|--------------------------------------------------|
| `PythonDagImporter`                          | — (parses the Python file itself)            | its own parsed Dag                       | PERSIST                                          |
| `PythonDagImporter` (via stub → Coordinator) | `parse_task_handler`, scoped to one `dag_id` | `TaskHandlerParsingResult`               | VALIDATE only — not a Dag, so nothing to persist |
| `JavaDagImporter`                            | `parse_dag`                                  | `DagFileParsingResult`, native Dags only | PERSIST                                          |

There is no fourth row. A `TaskHandlerRef` has no Dag, so no `DagImporter` —
and nothing reading a `DagImporter`'s results — ever sees one.

### Why the Distinction Lives on the Interface, Not the Bundle

A single artifact registers both kinds through one `register` call:

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

One artifact, one bundle, one `register` call — so neither the file nor the
bundle can tell a `DagImporter` what it is holding. The registration kind can,
and does: the split is per registration, not per file and not per bundle.

## Consequences

- **Python leads, Lang-SDK follows.** `PythonDagImporter` persists the Dag and
  drives validation via `queue → Coordinator → parse_task_handler`, comparing
  the returned declarations against its own parsed Dag.
- **A mixed-language `dag_id` never appears in Dag processing results.** The
  artifact registers handlers only, so no `Dag` registration exists for a
  `dag_id` a Python file already owns. Everything downstream — `DagImportResult`,
  import errors, the Dag list — sees exactly one record per `dag_id`, with no
  flag to interpret.
- **The handler channel is typed where the Dag channel is opaque.** Every SDK
  gets generated models for the declaration shape, and nothing on this path
  requires a DagSerialization implementation.
- Stub/implementation mismatches (missing handler, extra handler, parameter name
  or order, incompatible schema) surface as `DagImportError` at parse time. An
  unannotated stub argument is checked by name and position only.
- The Python Dag and Lang-SDK artifact can live in different DagBundles —
  resolution goes through the coordinator registry, not the filesystem.
- A single Dag can have stubs targeting different queues (some Java, some Go) —
  each resolves to its own coordinator instance independently, and validation
  unions their declarations before comparing task ids.
- **Mixed-language is Python-primary only.** Lang-SDK runtimes cannot define stub
  operators — the reverse direction (a native Dag delegating tasks to Python) is
  not supported.
- **A Lang-SDK importer is never configured by hand.** `[sdk] coordinators` is
  the one place a runtime is declared; the importer follows from it through
  `get_dag_importer()`, so the same runtime has no second configuration site to
  drift from.
- **One coordinator instance per DagBundle** becomes a deployment constraint:
  two JDKs mean two `dag_bundle_name` values and two bundle-scoped registries.
  It is what keeps extension-keyed importer registration unambiguous, and it
  makes `NAMED_BUNDLE` the mode a multi-runtime deployment has to use.
- **No per-Dag flag**, no schema migration, no new `DagModel` column, no
  REST/UI change.
- Each Lang-SDK runtime implements a second request type rather than a new field
  on the existing one. That is the cost of handlers not being Dags: a `DagRef`
  and a `TaskHandlerRef` have different shapes, so one request/result pair could
  not have carried both.
- Terms here track Language SDK spec `1.0`. A spec rename of `TaskHandler`, or
  of the `register`/`serve` verbs, lands in this ADR too.

## References

- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`) — `Dag` /
  `TaskHandler` / `bundle` / `register` / `serve` and their per-SDK spellings
- `task-sdk/src/airflow/sdk/execution_time/schema/` — the supervisor schema
  version bundle, the union registry, and the generated snapshot
- `task-sdk/src/airflow/sdk/importers/` — `AbstractDagImporter`,
  `DagImportResult`, `DagImporterRegistry`
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`
  (`@Builder.Dag` / `@Builder.Task` annotation processing, build-time artifact
  inventory)
- [ADR-0004](0004-dag-parsing.md) — coordinator subprocess bridge,
  `DagFileParseRequest` / `DagFileParsingResult`, `can_handle_dag_file`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
  for mixed-language Dags
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` /
  `TaskArgBinding` / `ArgValueSchema`
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter

## Appendix — For Implementation

Everything below is mechanics: exact shapes, exact call sites, and the code
that has to change. None of it is needed to follow the decision above.

### Appendix A — Message Definitions

```
ToRuntime      = TaskHandlerParseRequest      coordinator → runtime
ToCoordinator  = TaskHandlerParsingResult     runtime → coordinator
```

```
class TaskHandlerParseRequest:
    file: str                     # the artifact the coordinator resolved
    dag_id: str                   # scope: handlers bound to this dag_id only
    type: Literal["TaskHandlerParseRequest"]

class TaskHandlerParsingResult:
    fileloc: str
    task_handlers: list[TaskHandlerDeclaration]
    import_errors: dict[str, str] | None = None
    warnings: list | None = None
    type: Literal["TaskHandlerParsingResult"]

class TaskHandlerDeclaration:
    dag_id: str
    task_id: str
    params: list[TaskHandlerParam]     # ordered — arg_bindings are positional

class TaskHandlerParam:
    name: str
    value_schema: ArgValueSchema | None = None
    required: bool                     # the handler declares no default
```

Two properties of `value_schema` are load-bearing for validation:

- **It is nullable on both sides.** An unannotated `@task.stub` parameter
  produces `value_schema: null` today, so validation compares schemas only
  where neither side is null and falls back to name-and-arity otherwise.
  Without that, every untyped stub argument becomes a parse error.
- **`params` is ordered.** `LiteralArgBinding` and `XComArgBinding` are each
  documented as "one positional stub-task argument", so position is part of the
  contract rather than incidental.

A declaration carries no class, method, or source location. ADR-0006 rules out
Lang-SDK source display, and putting it on the wire would invite a consumer to
render it.

Registration mechanics: `registered_models_by_name()` in
`task-sdk/src/airflow/sdk/execution_time/schema/` introspects a fixed set of
unions — `ToTask` / `ToSupervisor` and `ToManager` / `ToDagProcessor` — so
adding `ToRuntime` / `ToCoordinator` extends that set. Regenerating the
`schema.json` snapshot is what propagates the new bodies into each SDK's
generated models. Two prek hooks guard the snapshot, and their interaction on a
first-introduction body needs checking against the hooks rather than against
that package's `AGENTS.md`, which says no `VersionChange` is required while the
second hook fails when the snapshot moves with nothing under `versions/`
touched.

### Appendix B — Coordinator Interface

```
BaseCoordinator                          execution_time/coordinator.py
  ├── execute_task                       (shipped)
  ├── parse_dag                          (new — Flow B)
  └── parse_task_handler                 (new — Flow C)
        │
SubprocessCoordinator                    coordinators/_subprocess.py
  implements all three; each resolves (command, subprocess_schema_version)
  from a hook and owns the socket lifecycle:
  ├── _build_execute_task_command         (shipped)
  ├── _build_parse_dag_command            (new)
  └── _build_parse_task_handler_command   (new)
        │
JavaCoordinator · ExecutableCoordinator · NodeCoordinator
  supply the three commands; no socket or protocol code
```

Naming follows the shipped `execute_task` / `_build_execute_task_command` pair,
and supersedes ADR-0004's `run_dag_parsing` / `dag_parsing_cmd`. Each hook
returns its own `subprocess_schema_version`, so handler parsing negotiates the
supervisor schema through the mechanism task execution already uses, and `None`
disables migration exactly as it does today. `can_handle_dag_file` still gates
Flow B only; Flow C resolves through the coordinator registry instead.

### Appendix C — DagImporter Registration

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

`dag_importer_configs` stays the door for importers with no runtime behind them
— a YAML importer, say. A Lang SDK never arrives that way.

#### Artifact-source modes

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
Which mode a coordinator is in decides whether it can back a Flow-B importer:

- **`NAMED_BUNDLE` registers into that bundle's registry.** Two
  `dag_bundle_name` values, two bundle-scoped registries, `.jar` claimed once in
  each.
- **`TASK_BUNDLE` has no fixed bundle**, so its importer registers into every
  bundle-scoped registry. Sound only while it is the sole claimant of its
  extension — the co-located single-runtime deployment.
- **`EXPLICIT_ROOT` has no DagBundle at all**, so the Dag processor never scans
  its artifacts and it cannot produce a Dag to persist. It stays a Flow-C
  coordinator, reached by queue.

`get_importer_registry(bundle_name)` is already cached per bundle, so the
scoping mechanism is in place. What `CoordinatorManager` still needs is the
reverse lookup: it resolves coordinators by queue only today —
`for_queue(queue)`, driven by `[sdk] queue_to_coordinator` — with nothing that
answers "which coordinators serve this bundle".

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

### Appendix D — Flow Walkthroughs

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

#### Flow B — Lang-SDK Importer (native Dags only)

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

#### Flow C — Mixed-Language Dag (validation driven by PythonDagImporter)

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
