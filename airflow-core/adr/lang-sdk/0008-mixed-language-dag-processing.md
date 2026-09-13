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
`register`, `serve`. The examples below use the Java and Go spellings
(`@Builder.TaskHandler` / `airflow.TaskHandler`); TypeScript spells the same
terms in its own idiom. Note that the spec's `bundle` is the SDK-side
registration container, not Airflow's `DagBundle` — both appear in this ADR.

This ADR uses `JavaDagImporter` and `JavaCoordinator` as the concrete examples.
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
its own to persist. `Dag(spec)` returns a `DagRef`, which owns all three. Java
spells the pair `@Builder.TaskHandler(dagId, taskId)` and `@Builder.Dag(id,
...)`, both driving the `BuilderProcessor` annotation processor
([ADR-0003](0003-pure-java-dags.md)); Go spells them
`airflow.TaskHandler(dagId, taskId, fn)` and `airflow.Dag(spec)`. The spelling
differs per SDK, the split does not.

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

### The Wire Contract: a Channel of Its Own

Handler declarations travel on their own message pair, in their own
discriminated unions — not as a field on `DagFileParseRequest`:

```
ToRuntime      = TaskHandlerParseRequest      coordinator → runtime
ToCoordinator  = TaskHandlerParsingResult     runtime → coordinator
```

`task-sdk/src/airflow/sdk/execution_time/schema/` introspects exactly the
unions it is given — `ToTask` / `ToSupervisor` (task-execution channel) and
`ToManager` / `ToDagProcessor` (manager ↔ parser channel) — and union
membership is the only registration step: it is what puts a body into the
generated `schema.json` snapshot and therefore into every SDK's generated
models. So the choice of union *is* the wire-contract decision.

Neither existing pair fits, because the runtime plays two different roles. When
it parses native Dags it **replaces** the parser process and answers the
manager, so `ToDagProcessor` / `ToManager` describe it exactly, and those
unions stay as ADR-0004 defined them. When it answers a handler query it is a
short-lived child of the Python parser, and the manager is not a peer at all.
Reusing `ToDagProcessor` there would make one union mean two recipients. A
fifth pair, named for the peers the coordinator already has, keeps both honest.

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

`task_handlers` is this channel's answer to
`DagFileParsingResult.serialized_dags`, and it is deliberately a stronger
contract. `serialized_dags` is `list[LazyDeserializedDAG]`, which reduces in
the snapshot to `{"data": {"type": "object", "additionalProperties": true}}` —
an opaque blob, which is why every SDK reimplements DagSerialization v3 by hand
and validates it against shared fixtures ([ADR-0004](0004-dag-parsing.md)).
A handler declaration carries no Dag, so it can be fully typed, code-generated,
and schema-validated in every SDK. **The mixed-language path never asks a Lang
SDK to implement DagSerialization at all.**

`value_schema` reuses the existing `ArgValueSchema` definition — the same one
`LiteralArgBinding.value_schema` and `XComArgBinding.value_schema` already
point at ([ADR-0007](0007-taskflow-across-language-boundary.md)). Validation
therefore compares like against like rather than translating between two
vocabularies. Two properties of that field are load-bearing:

- **It is nullable on both sides.** An unannotated `@task.stub` parameter
  produces `value_schema: null` today, so validation compares schemas only
  where neither side is null and falls back to name-and-arity otherwise.
  Without that, every untyped stub argument becomes a parse error.
- **`params` is ordered.** `LiteralArgBinding` and `XComArgBinding` are each
  documented as "one positional stub-task argument", so position is part of the
  contract rather than incidental.

A declaration carries no class, method, or source location. ADR-0006 already
rules out Lang-SDK source display, and putting it on the wire would invite a
consumer to render it.

### The Coordinator Interface

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

The two parse methods are **not symmetric siblings**, which is the strongest
reason not to collapse them into one `parse(request)`:

- `parse_dag` is **bridge mode**. The coordinator becomes the parser process;
  the manager's `DagFileParseRequest` and the runtime's `DagFileParsingResult`
  cross unchanged over the raw byte forwarder, on the manager ↔ parser unions
  ([ADR-0004](0004-dag-parsing.md)). The coordinator never reads the payload.
- `parse_task_handler` is **query mode**. The coordinator is itself the peer:
  it sends one `TaskHandlerParseRequest` and decodes one
  `TaskHandlerParsingResult` on the channel above.

One collapsed method would also erase a real deployment distinction — a
coordinator can serve mixed-language handlers with no interest in native Dag
parsing, and an absent method says so better than a runtime rejection.
`can_handle_dag_file` still gates Flow B only; Flow C resolves through the
coordinator registry instead.

Naming follows the shipped `execute_task` / `_build_execute_task_command` pair,
and supersedes ADR-0004's `run_dag_parsing` / `dag_parsing_cmd`. Each hook
returns its own `subprocess_schema_version`, so handler parsing negotiates the
supervisor schema through the mechanism task execution already uses, and
`None` disables migration exactly as it does today.

### DagImporter Registration

`AbstractDagImporter` (AIP-85, `task-sdk/src/airflow/sdk/importers/`) is about
source formats — `can_handle`, `list_dag_definitions`, `import_definition`,
`get_source_code` — and returns `DagImportResult.dags: list[DAG]`, i.e.
`airflow.sdk.DAG` objects. The coordinator is about processes. A Lang-SDK
importer composes one, and the coordinator is what hands it out:

```
BaseCoordinator.get_dag_importer() -> AbstractDagImporter | None
    default: None — this coordinator contributes no importer
    JavaCoordinator.get_dag_importer() -> JavaDagImporter(coordinator=self)
```

Because the coordinator returns the importer already bound to itself, an
operator never configures an importer's coordinator: that wiring is an
implementation detail of the coordinator that produced it.
`DagImporterRegistry.from_config` gains one tier:

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

#### One coordinator instance per DagBundle

The registry keys importers by file extension, one importer per extension,
evicting the previous claimant with a log warning. Two `JavaCoordinator`
instances on different JDKs would both claim `.jar`.

The boundary that resolves this is deployment-shaped rather than a tie-break:
**a coordinator instance owns a DagBundle.** `SubprocessCoordinator` classifies
that ownership at construction into one of three artifact sources, and only one
of them names a bundle:

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

- **`NAMED_BUNDLE` registers into that bundle's registry.** This is the
  unambiguous case, and the mode a deployment running two JDKs has to use: two
  `dag_bundle_name` values, two bundle-scoped registries, `.jar` claimed once
  in each.
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

#### Flow B — Lang-SDK Importer (native Dags only)

A `TaskHandlerRef` carries no Dag, so a mixed-language `dag_id` is never a
candidate for persistence — there is nothing for `JavaDagImporter` to filter.

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

`DagImportResult.dags` is `list[DAG]`, so the importer wraps each serialized
entry as a `LazyDeserializedDAG` and transforms it into an `airflow.sdk.DAG`
before returning. The transform is mechanical — the serialized form encodes the
SDK Dag's own fields — but `LazyDeserializedDAG` lives in `airflow-core` today,
so where a Task SDK importer reaches it from is settled by AIP-85's own
`list[DAG]` / `list[LazyDeserializedDAG]` discussion rather than here. Flow C
needs none of this: it compares declarations and returns the `airflow.sdk.DAG`
the Python parser already built.

#### Flow C — Mixed-Language Dag (validation driven by PythonDagImporter)

The `PythonDagImporter` owns validation. When it finds a stub task, it resolves
the stub's `queue` to its Coordinator, then calls that Coordinator's
`parse_task_handler` **directly** — no `DagImporter` involved, since this is a
single request/response rather than a discovery operation, and what comes back
is not a Dag.

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
  `dag_id` a Python file already owns, and
  `DagFileParsingResult.serialized_dags` carries native Dags exclusively.
  Everything downstream — `DagImportResult`, import errors, the Dag list — sees
  exactly one record per `dag_id`, with no flag to interpret.
- **The handler channel is typed where the Dag channel is opaque.** Every SDK
  gets generated models for `TaskHandlerDeclaration` from the `schema.json`
  snapshot, and nothing on this path requires a DagSerialization
  implementation.
- Stub/implementation mismatches (missing handler, extra handler, parameter name
  or order, incompatible schema) surface as `DagImportError` at parse time.
  An unannotated stub argument is checked by name and position only.
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
  two JDKs mean two `dag_bundle_name` values and two bundle-scoped
  registries. It is what keeps extension-keyed importer registration
  unambiguous, and it makes `NAMED_BUNDLE` the mode a multi-runtime
  deployment has to use.
- **No per-Dag flag**, no schema migration, no new `DagModel` column, no
  REST/UI change.
- Adding `ToRuntime` / `ToCoordinator` extends the set of unions the supervisor
  schema package introspects, and regenerating the `schema.json` snapshot is
  what propagates the new bodies into each SDK's generated models. Two prek
  hooks guard that snapshot, and their interaction on a first-introduction body
  needs checking against the hooks rather than the docs.
- Terms here track Language SDK spec `1.0`. A spec rename of `TaskHandler`, or
  of the `register`/`serve` verbs, lands in this ADR too.

## References

- Language SDK spec (`task-sdk/docs/lang-sdk-spec.rst`) — `Dag` /
  `TaskHandler` / `bundle` / `register` / `serve` and their per-SDK spellings
- `task-sdk/src/airflow/sdk/execution_time/schema/` — the supervisor schema
  version bundle, the union registry, and the generated `schema.json` snapshot
- [ADR-0003](0003-pure-java-dags.md) — `BundleScanner` / `BuilderProcessor`
  (`@Builder.Dag` / `@Builder.Task` annotation processing, build-time artifact
  inventory)
- [ADR-0004](0004-dag-parsing.md) — coordinator subprocess bridge,
  `DagFileParseRequest` / `DagFileParsingResult`, `can_handle_dag_file`
- [ADR-0006](0006-no-lang-sdk-source-display.md) — no Lang-SDK source display
  for mixed-language Dags
- [ADR-0007](0007-taskflow-across-language-boundary.md) — `arg_bindings` /
  `TaskArgBinding` / `ArgValueSchema`, compared against `TaskHandler` parameter
  declarations in Flow C
- [AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ) — Language SDKs
- [AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) — DagImporter
