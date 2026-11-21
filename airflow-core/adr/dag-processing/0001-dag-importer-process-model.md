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

# ADR-0001: Dag Importer Process Model — Who Owns the Parse Process

## Status

Proposed

## Context

[AIP-85](https://cwiki.apache.org/confluence/x/_Q7OEg) adds `AbstractDagImporter`
(`task-sdk/src/airflow/sdk/importers/`) so the Dag processor can parse sources other than Python.
The interface as it stands says what importing *means* for a format — `list_dag_definitions`,
`might_contain_dag`, `import_definition`, `get_source_code` — and says nothing about *where* the
import runs.

Where it runs is still decided by the Dag processor manager, which forks one parse process per
file. For a file a coordinator's importer claims, the manager starts `LangSDKDagFileProcessorProcess`,
whose child execs the language runtime; for every other file it starts `DagFileProcessorProcess` and
runs a Python parse in it. That is the wrong shape for both directions the AIP opens:

- A format that only has to be **read** — a manifest of already-serialised Dags, say — pays for a
  process it does not need, on every parse of every file.
- A format backed by **another runtime** needs a *different* process — a JVM, a Go binary — not a
  Python parse. The manager supports that today only by recognising one kind of importer, the
  coordinator's, and choosing the process class for it.
  [ADR-0010](../lang-sdk/0010-native-dag-processing.md) records that
  routing as a bridge until importers own their parse process; every further kind of importer would
  need another branch in the manager.

This ADR settles the process model without a layer between the manager and the parse process, and
fixes the handful of interface obligations that follow from it.

Terms: a **definition** is the smallest thing an importer can import on its own. For Python that is
a module; for a zip archive it is a member, not the archive. One definition may yield several Dags.

## Decision

### 1. The importer owns the process, and nothing sits between it and the manager

```
today

  manager loop
     │  one queue entry per FILE
     ▼
  get_claiming_importer(file)               ← the manager special-cases coordinator importers
     ├── claimed:  LangSDKDagFileProcessorProcess → child execs the runtime
     └── otherwise: DagFileProcessorProcess
                      └── _parse_file → DagBag(file) → every Dag in the file, one sys.modules

decided

  manager loop
     │  one queue entry per DEFINITION
     ▼
  importer.submit_import(definition, bundle, context=...) → ImportHandle     never blocks
     ├── fork_import() → parse child           default — one child per definition
     ├── read it here, already finished        opt-in — no author code, bounded work
     └── hand to a runtime it keeps warm       Java / Go — the importer's own process
```

`submit_import` is the extension point for *how the importer's language runs*; `import_definition`
remains the single statement of what importing means, and both the child and an inline import end up
back in it, so there is never a second implementation of the import itself.

`submit_import` returns without waiting for the import. The default forks — `fork_import()`, then
`import_definition` in the child — so an importer written against `import_definition` alone, as every
existing importer is, keeps working unchanged and never runs in the manager. An importer may instead
import inline and return an already-finished handle only if it runs no definition-author code and
its work is bounded, such as reading a manifest of already-serialised Dags. Like a non-blocking
`is_ready`, that is an obligation the framework cannot check.

The manager cannot call `import_definition` directly because its loop is single-threaded and also
services sockets, refreshes bundles, harvests results and enforces deadlines; `import_definition`
blocks for as long as user code takes. `ImportHandle` is the shape it supervises instead:

```
ImportHandle                       what the manager does with it
  is_ready    ──────────────────►  polled every loop pass — must never block
  result      ──────────────────►  read once ready, then bagged and persisted
  start_time  ──────────────────►  compared against the parse timeout
  abort()     ──────────────────►  the deadline passed, the definition left the bundle, or shutdown
  close()     ──────────────────►  exactly once, after the result is taken
```

The interaction across the manager, importer, and execution boundary follows:

```
Manager                            Importer                      Parse child
   │                                  │                                   │
   │── submit_import(def, context) ──►│                                   │
   │                                  │── fork_import() ─────────────────►│ (spawns child)
   │◄── ImportHandle ─────────────────│                                   │
   │                                                                      │
   │── [loop] poll handle.is_ready ──────────────────────────────────────►│ (runs import_definition)
   │                                                                      │
   │◄── is_ready = True ──────────────────────────────────────────────────│
   │── read handle.result & close()
```

The protocol is structural, not a base class: the existing parse subprocess already has this shape,
and an importer wrapping a foreign runtime should not have to inherit from the Task SDK to be
supervised.

`ImportContext` is the other half of the pair and is bounded the same way: it carries state scoped to
a single import, and members are added to it when a caller needs one, not in anticipation. Both are
public surface from the moment this lands, so every member on either is a compatibility obligation.

#### Naming

The interface is not yet released or exported from `airflow.sdk`, so names are still free; they will
not be once it is.

| | chosen | not chosen | why |
|---|---|---|---|
| verb | `import` | `parse`, `process` | Airflow's own language-neutral term — below |
| begins work | `submit_import` | `start_import`, `create_parse` | `start_` implies a paired `stop_`; the handle owns the lifetime, and `submit` is what `concurrent.futures` calls this shape |
| the return | `ImportHandle` | `ParseFuture`, `ParserHandle` | `Future.result()` blocks by default and nothing here may; the handle is over one unit of work, not over a parser |
| poll for it | `is_ready` | `is_finished` | what the existing parse processes already expose and `_collect_results` polls; it means the result can be read — the process exited *and* its output drained — not only that the work finished |
| stop the work | `abort()` | `kill()`, `stop()` | the target may be work inside a runtime other imports are still using, not a process to signal; `stop()` reads as stopping the importer, which is `shut_down()` |
| identify it | *(nothing)* | `pid`, `identifier` | the manager already mints an id per parse (`uuid7()` in `_create_process`) and can keep it next to the handle; a `pid` does not exist for an inline import, nor for work inside a warm runtime |

Only the verb is worth arguing. "Import" looks like a Python borrowing, but in Airflow it is already
the language-neutral word for loading a Dag definition and for failing to: the `import_error` table,
the `/importErrors` API, `airflow dags list-import-errors` and "Dag Import Errors" in the UI all
predate AIP-85, and none of them are about Python's `import` statement. It is also the verb of the
importer interface this extends — `import_definition`, `DagImportError`. "Parse" already names the
layer below it: a coordinator's `parse_dag()` and the Lang-SDK parse protocol
([ADR-0012](../lang-sdk/0012-lang-sdk-parse-protocol.md)) are what a language importer drives once
it has been asked to import. Naming `submit_import` after that layer would blur the two.

#### Alternatives considered

- **A local worker-pool layer between manager and importer**, by analogy with the executor.
  Rejected: on the execution side the first fork is a *distribution boundary* — the workload may
  leave the machine. A local layer crosses no such boundary; it would only add a process hop and a
  second place where process policy lives. Distributing parse work across machines is out of scope
  here. It would decide *where* `submit_import` runs, while the importer still owns the runtime
  there, so nothing in this ADR precludes it.
- **A thread pool in the manager.** Rejected: it contradicts fork safety — a fork wants no threads
  running and no locks held — and it would make thread safety a requirement of every third-party
  importer.
- **Keep the manager forking, and consult the importer only for the work inside the child.** This is
  the model of [ADR-0004](../lang-sdk/0004-dag-parsing.md) and of ADR-0010's target: any language
  runtime is started *inside* a Python parse child. Rejected: it nests a foreign runtime under a
  Python child that must relay every request between the runtime and the manager, and it cannot
  express "no process" at all — which is the point.
- **Keep the manager choosing the process class per kind of importer**, as ADR-0010's interim
  routing does today. Rejected: process policy then lives in the manager, which needs a new branch
  for every kind of importer, and an importer outside airflow-core cannot add one.
- **Import inline by default, forking only when an importer asks.** Rejected: every importer written
  against `import_definition` alone would then run in the manager's loop, blocking it for as long as
  the import takes — including formats such as YAML, whose `uses:` and `run:` build Task SDK Dags and
  run author code.
- **`async`/`await` on the importer interface.** Rejected: it colours the whole interface and drags
  every importer author into an event loop for what is, for most formats, a `read()`.

### 2. The unit of parse work is a Dag definition, not a file

```
bundle/
  daily.py               → 1 definition    daily.py              (1 Dag)
  etl.py                 → 1 definition    etl.py                (3 Dags)
  archive.zip
    ├── a.py             → 1 definition    archive.zip/a.py
    └── b.py             → 1 definition    archive.zip/b.py
                           ^^ the archive importer already yields its members separately;
                              what changes here is that each becomes its own queue entry
  catalog.yaml           → 1 definition    catalog.yaml          (N Dags)
```

A queue entry names one definition, and the location it carries is that definition's
`relative_fileloc` — including for a definition inside a container, where it names the member and
not the container.

Three rules follow, and hold for every importer:

- **Isolation is between definitions, not between containers.** An archive's members are already
  separate imports; queuing the archive as one entry is what puts them back into one process and one
  `sys.modules`. Queuing per definition gives each its own child. The Dags of one definition still
  share its import. The guarantee holds for children from `fork_import`; an importer that keeps a
  runtime warm across definitions isolates them only as far as it documents resetting that runtime.
- **A definition answers for its own freshness and ordering.** Not every definition is a file on
  disk, so the queue asks the definition for its `freshness_token` — which `DagBag` already uses to
  skip unchanged definitions — rather than calling `os.path.getmtime` on a path that may not exist,
  as the manager's `modified_time` sort does today.
- **A callback does not depend on discovery.** A callback entry names the definition and the bundle
  version the callback was requested for, and is resolved against that version, as
  `prepare_callback_bundle` does today — not against the latest listing. If the definition is
  missing from that version, the callback fails with an error naming the definition instead of being
  dropped.

#### Alternatives considered

- **Keep the openable source as the unit and let importers batch internally.** This keeps the queue
  shaped as it is today and metric cardinality flat. Rejected: it makes isolation depend on how a
  deployment happens to package its Dags, and it has no answer at all for a format with no file per
  Dag. The cost of the decision taken is real and is recorded under Consequences.

### 3. Fork mechanics and collector discipline belong to the base class

Forking out of a long-lived process carries a memory contract that is easy to miss: the child shares
the parent's pages until something writes to them, and a garbage collection writes to every object
it inspects. One collection in a freshly forked child therefore copies most of the parent's heap to
achieve nothing. The same effect in Airflow's forked workers is worked through in
[apache/airflow#58143](https://github.com/apache/airflow/discussions/58143); this generalises it to
any importer that forks.

```
manager process                                          parse child
───────────────────────────────────────────────────      ────────────────────────
prepare bundles
warm_importers()       every importer realised, every warm_up() run
gc.collect()           nothing collectable is left to be frozen
gc.freeze()            the shared heap becomes permanent
   │
   ├─ per import, steady state ─────────────────
   │     submit_import()               the default
   │       └── fork_import():  gc.disable()
   │                           spawn_child()  ──fork──►  inherits the collector OFF
   │                           gc.enable()               gc.freeze()  inherited heap permanent
   │                                                     gc.enable()  sees only its own objects
```

`fork_import()` is `final`, so the framework holds this discipline rather than leaving it to importer
authors. It binds every importer that takes its child from `fork_import`, and cannot bind one that
calls `os.fork()` directly inside its own `submit_import` — nothing can, since `submit_import` is the
extension point. Such an importer has left the manager's supervision as well, so the guarantee is
stated narrowly: **the framework holds the discipline for children it hands out.**

The base class does not itself call `os.fork()`. The child is the Dag processor's own machinery —
its request and result models, its comms proxying — so the framework passes a `spawn_child` callable
in through `ImportContext`, and an importer asks for isolation without knowing any of that.

The startup freeze does not cover what the manager allocates afterwards, so a child that simply
re-enabled its collector would still copy those pages at its first collection. The child therefore
freezes first: `gc.freeze()` splices the generation lists into the permanent generation without
touching the objects, so everything inherited becomes permanent and the child's collector only sees
what the child allocates. The parent's `gc.disable()` around the fork is what guarantees no
collection runs in the child before that freeze. Re-enabling is still needed, since the child runs
definition-author code that creates reference cycles. Both steps run in the child entrypoint behind
`spawn_child`, which is the framework's own, before `import_definition` is called: an importer author
never touches the collector.

`spawn_child` is **absent** whenever nobody is supervising child processes: a CLI command, a test,
anything importing definitions directly. Process isolation is therefore something an importer may be
*denied*, not something it can assume — and being denied it silently is the wrong default, because
that caller list includes processes running in server context. `fork_import` takes the importer's
answer rather than choosing for it:

```
fork_import(definition, context, inline_fallback=True)

  spawn_child present            → fork; the discipline applies
  absent, inline_fallback=True   → import inline in the caller's process   (Python today: CLI, tests)
  absent, inline_fallback=False  → raise; the caller learns isolation was unavailable
```

`inline_fallback=True` keeps today's behaviour for the Python importer, which needs to work under
`airflow dags test`. An importer for which isolation is a *safety* property rather than a hygiene
one passes `inline_fallback=False` and gets an error it can surface, instead of quietly running
definition-author code wherever it happens to have been called.

#### Alternatives considered

- **Document the rule for importer authors.** Rejected: an importer author who gets
  it wrong is invisible to the operator, and the damage degrades the entire Dag processor.
  Memory and collector hygiene must be enforced by the framework rather than left to
  third-party documentation.
- **`gc.freeze()`/`unfreeze()` around each fork instead of `disable()`/`enable()`.** Rejected:
  `unfreeze()` has no granularity. It empties the permanent generation wholesale, so a per-import
  `unfreeze()` would discard the startup freeze that the whole scheme depends on.
- **A class-level `requires_isolation` flag on the importer** instead of a per-call argument.
  Rejected: it adds a member to the public interface to express something only `fork_import` ever
  reads, and it cannot vary per definition — a format with both static and executable definitions
  would have to declare the stricter answer for both.

### 4. An importer outlives every import it performs

An importer is built once and kept for the whole Dag processor run. It is therefore free to hold
state that is expensive to create — a warm runtime, a loaded classpath, a language server, a
connection pool — and amortise it across every definition it is given.

This is what makes a one-definition-per-call interface affordable for a language whose startup cost
dwarfs its parse cost: the cost belongs to the importer's *lifetime*, not to the *call*.

The corollary is that an importer must not accumulate per-definition state on itself. Anything
scoped to a single import arrives in an `ImportContext`.

`warm_up()` is where that lifetime state is built: called once, after construction, before any
definition is imported — and, in the Dag processor, before the heap is frozen, so whatever it builds
is paid for once and shared by every child rather than built lazily and copied thereafter:

```python
class AbstractDagImporter(ABC, Generic[DefT]):
    def warm_up(self) -> None:
        """Do one-off setup before any definition is imported and before the heap freezes."""

    def shut_down(self) -> None:
        """Release what warm_up() acquired. Called once; no import follows."""
```

The Dag processor drives this lifecycle around its loop:

```
Manager startup (before_run)
  │
  ├── 1. Discover bundles
  ├── 2. warm_importers()  ──► calls warm_up() on all registered importers
  ├── 3. gc.collect()      ──► sweep transient setup allocations
  └── 4. gc.freeze()       ──► freeze shared memory pages
  ▼
  [ loop: submit_import / poll / harvest ]
  ▼
Manager shutdown (after the loop exits)
  └── shut_down() on every importer that was realised — failures logged, never raised
```

A failing `warm_up()` does not stop the Dag processor booting; the importer is left to fail at its
first real import, where the error has a definition to attach to.

`shut_down()` closes the bracket, and it exists because the state this decision permits is not only
memory: a JVM, a language server and a connection pool are all **operating-system resources with
their own lifetimes**. Without a teardown call there is no defined moment at which a warm runtime
stops, and the importer's children can outlive the Dag processor that started them. It is called
once, on Dag processor shutdown. It must tolerate a `warm_up()` that failed or never ran, and, like
`warm_up()`, a failure in it is logged rather than propagated: shutdown proceeds.

This is deliberately *not* a general teardown protocol — no ordering guarantees between importers, no
re-entry, no restart. It is one call so that an importer holding a process has somewhere to stop it.

#### Alternatives considered

- **A batch API — `import_definitions(list)`.** Rejected: every importer would have to honour it,
  and for Python it re-introduces the shared-process semantics that decision 2 removes. The lifetime
  guarantee buys the same amortisation without changing the shape of the call.
- **Let process exit clean up; no teardown call.** Rejected: it is only true for children that die
  with their parent. A runtime launched detached, a pool holding server-side sessions, or a socket a
  peer is waiting on all outlive an unannounced exit, and the importer has no other hook to notice.

### 5. A definition must be reconstructible in another process

A definition is handed to an importer, and may then have to reappear somewhere with a different
address space: a child that `exec`s instead of forking, or any foreign runtime, which is a different
address space by construction. Definitions therefore serialise to a self-describing envelope — the
definition class plus its own dictionary — and are rebuilt from it on the far side.

```
manager                                     child / runtime
  definition  ──► to_wire()  ──►  { classpath, data }  ──►  from_wire()  ──► definition
```

Definitions travel as **coordinates, not contents**: an archive member sends the archive path and
the member path, not the member's bytes. There is then exactly one reader of the source, in the
process that is going to import it, and no window in which the two disagree.

#### Alternatives considered

- **Pickle the definition.** Rejected: it binds the wire format to Python and to class layout, and a
  foreign runtime cannot read it.
- **Send only a path.** Rejected: it cannot express a member inside a container, nor a definition
  with no path at all.

### 6. A handle's result is already serialised

Dags are written against the Task SDK and must be serialised *in a Task SDK process*, so that the
`airflow.sdk` and `airflow.models` hierarchies never meet. The manager runs in server context. So
`import_definition` returning live `DAG` objects is fine — those objects exist inside the importer's
own process — but `ImportHandle.result` crosses back into the manager, and must not carry them.
That is the boundary this decision fixes: wherever the import runs — a child, a warm runtime, or
later a remote worker — the manager receives the serialised form, and what the importer builds
internally stays its own choice.

```
  producer                     what it does                           ImportHandle.result
  ─────────────────────────────────────────────────────────────────────────────────────────
  forking importer   child: import_definition → DAG objects       ──┐
                            → serialise, in the Task SDK process    │
  static importer    manager: read the source, emit directly      ──┼──►  the serialised form
  foreign runtime    JVM: emit natively, never building a         ──┘
                            Python DAG at all
```

This is also what makes decision 1's inline opt-in safe: an importer that runs no definition-author
code may serialise in the manager, because nothing author-written ever loaded there.

The consequence that matters for AIP-108 is the other one. **The serialised result type is the
foreign-runtime contract.** On the wire it already exists: `DagFileParsingResult` is part of the
Task SDK's versioned supervisor schema, and the Java, Go and TypeScript runtimes emit it today
without building a Python `DAG`. What is missing is the Python side: the model lives in
`airflow.dag_processing.processor`, which an importer distributed outside airflow-core cannot
import. It must move where the Task SDK can name it, or every Python importer outside airflow-core
ultimately depends on airflow-core to produce a result.

`ImportHandle.result` carries the fields `DagFileParsingResult` has today, for one definition:
`fileloc`, `serialized_dags`, `import_errors`, `warnings`, `parsed_definitions` and
`dag_source_codes`. Each serialised Dag travels as the dictionary `DagSerialization` produces, as it
already does on the wire; `LazyDeserializedDAG`, an airflow-core class, stays the manager-side
wrapper around it.

#### Alternatives considered

- **Keep the result as live `DAG` objects and let the manager serialise.** Simplest signature.
  Rejected: it puts Task SDK objects in the server-context process for every importer, and leaves a
  foreign runtime with no expressible result.
- **Forbid inline imports entirely**, so there is only ever a child to serialise in. Rejected: it
  removes the opt-in that lets a read-only format import without a process, one of the reasons for
  decision 1.

### 7. Callbacks stay Python-only for now

`on_failure_callback` / `on_success_callback` are definition-author code. Callbacks for Python Dags
keep running in a Python parse child against a `DagBag`, as `DagFileProcessorProcess` runs them
today, outside the importer interface. A callback for a Dag from a language runtime is not run: no
language SDK runtime supports callbacks yet, and the manager already drops such a request with a
warning (`_log_dropped_lang_sdk_callback`).

#### Alternatives considered

- **A second importer capability** — `execute_callbacks(definition, requests, bundle)`, defaulting to
  unsupported. Deferred, not rejected: running a callback is the language runtime's job, so this is
  the answer once language SDK runtimes support callbacks, and adding a method with an
  "unsupported" default later does not break existing importers.

## Open questions

None. The two raised while drafting are settled: callbacks by decision 7, and runtime launch for
language importers by [ADR-0010](../lang-sdk/0010-native-dag-processing.md) and
[ADR-0012](../lang-sdk/0012-lang-sdk-parse-protocol.md). The importer names its coordinator class
and looks the coordinator up with `get_parsing_coordinator()`, and `SubprocessCoordinator` builds
both the task-execution and the parse command, so there is one launch path. This ADR changes only
where that launch is started from: `submit_import`, instead of the manager's interim routing.

## Consequences

### Positive

- A format that only has to be read can import without a process. Today every format costs one.
- A foreign runtime is expressible: AIP-108's language importers plug into `submit_import` and bring
  their own process model with them. Decision 6 is what makes this true — a runtime that cannot
  construct Task SDK `DAG` objects returns the serialised form instead, and never has to reach a
  Python process to do it.
- One definition's import side effects cannot reach another's in a child from `fork_import`,
  including inside an archive. A `DagBag` given an archive member already imports only that member,
  but one given the archive still imports every member in one process, so this is a Dag processor
  property, not yet a global one.
- The collector discipline holds by construction for any importer that takes its child from
  `fork_import`, rather than by documentation, and the manager's startup freeze survives a full run.
  An importer that spawns its own process by other means opts out of the discipline and of the
  manager's supervision together.

### Negative

- **Metric, log and queue cardinality now track definition count.** A 20-member archive becomes 20
  queue entries, 20 log files and 20 forks where it was one of each. Deployments that ship Dags as
  large archives will see parse-loop throughput fall and per-file metric tags multiply. This is the
  accepted price of decision 2.
- The surface an importer author must understand grows: `submit_import`, `ImportHandle`,
  `ImportContext`, `warm_up`, `shut_down`, the serialised result form and the definition wire format
  — against `import_definition` alone today. Each is public from the moment this lands.
- `submit_import` and `is_ready` are now on the manager's hot path. An importer that blocks in
  either — an inline import that reads too much, a synchronous RPC in a readiness check — stalls the
  parse loop for every other definition. The obligation is on the importer and is not enforceable by
  the framework.

### Neutral

- The archive importer needs no `submit_import` of its own: each member is its own queue entry and
  takes the default fork.
- Priming the secret cache becomes the Python importer's `warm_up`, so a deployment that evicts the
  Python importer no longer primes it. That deployment has no Python Dags reading Variables at
  import time, so the behaviour is coherent rather than accidental.
- AIP-92 is unaffected. The parse child still reaches Airflow only over the Execution API, and the
  manager still holds the only database session; moving process ownership into the importer does not
  add a database caller.
- `DagBag` keeps its other callers — tests, the `airflow dags` CLI, the in-process API. What changes
  is that it is no longer on the Dag processor's parse path.

## References

- [AIP-85: Extendable Dag parsing control](https://cwiki.apache.org/confluence/x/_Q7OEg)
- [ADR-0004](../lang-sdk/0004-dag-parsing.md) — language-specific Dag file processing, and the
  process model this ADR supersedes; its parse request, result and bridge shapes stand
- [ADR-0002](../lang-sdk/0002-workload-execution.md) — workload execution through coordinators; the
  execution-side fork this model deliberately does not mirror
- [ADR-0010](../lang-sdk/0010-native-dag-processing.md) and
  [ADR-0012](../lang-sdk/0012-lang-sdk-parse-protocol.md) — native Dag processing: how an importer
  finds its coordinator, the interim manager routing this ADR replaces, and the parse protocol
- [apache/airflow#74042](https://github.com/apache/airflow/pull/74042),
  [#74035](https://github.com/apache/airflow/pull/74035) and
  [#74043](https://github.com/apache/airflow/pull/74043) — the shipped native Dag processing, the
  first consumer of this interface
- [apache/airflow#58143](https://github.com/apache/airflow/discussions/58143) — preventing
  copy-on-write in forked workers; the collector discipline decision 3 generalises
- AIP-92 — removing database access from the Dag file processor; a parallel workstream this model
  must not obstruct
