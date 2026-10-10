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
`import_definition`, `get_source_code` — and says nothing about *where* the import runs.

Where it runs is still decided by the Dag processor manager, which forks one
`DagFileProcessorProcess` per file and runs a Python parse in it. That is the wrong shape for both
directions the AIP opens:

- A format that only has to be **read** — JSON, YAML, a manifest naming Dags — pays for a process
  it does not need, on every parse of every file.
- A format backed by **another runtime** needs a *different* process — a JVM, a Go binary — not a
  Python fork. Nothing in the current model can express that, which is why Dag parsing was cut
  from AIP-108's scope and [ADR-0004](../lang-sdk/0004-dag-parsing.md) was left "retained for when
  AIP-85 is revisited".

An earlier proposal filled the gap by inserting a layer between the manager and the parse process,
mirroring the executor's worker pool: the manager forks a generic parse worker, which then
dispatches to an importer. This ADR settles the process model without that layer, and fixes the
handful of interface obligations that follow from it.

Terms: a **definition** is the smallest thing an importer can import on its own. For Python that is
a module; for a zip archive it is a member, not the archive. One definition may yield several Dags.

## Decision

### 1. The importer owns the process, and nothing sits between it and the manager

```
today

  manager loop
     │  one queue entry per FILE
     ▼
  fork DagFileProcessorProcess              ← always a Python fork, whatever the format
     └── _parse_file → DagBag(file) → every Dag in the file, one sys.modules

decided

  manager loop
     │  one queue entry per DEFINITION
     ▼
  importer.start_import(definition, bundle, context=...) → ImportHandle
     ├── read it here, already finished        static format — no process at all
     ├── fork_import() → parse child           Python — one child per definition
     └── hand to a runtime it keeps warm       Java / Go — the importer's own process
```

`start_import` is the extension point for *how the importer's language runs*; `import_definition`
remains the single statement of what importing means, and both the inline path and the child end up
back in it, so there is never a second implementation of the import itself.

The default `start_import` imports inline and returns an already-finished handle. A static format
therefore gets no process without asking for one. An importer that executes definition-author code
must override — the manager's own process is not a safe place to run it.

The manager cannot call `import_definition` directly because its loop is single-threaded and also
services sockets, refreshes bundles, harvests results and enforces deadlines; `import_definition`
blocks for as long as user code takes. `ImportHandle` is the shape it supervises instead:

```
ImportHandle                       what the manager does with it
  is_ready    ──────────────────►  polled every loop pass — must never block
  result      ──────────────────►  read once ready, then bagged and persisted
  start_time  ──────────────────►  compared against the parse timeout
  pid         ──────────────────►  diagnostics and log lines only
  kill()      ──────────────────►  the deadline passed
  close()     ──────────────────►  exactly once, after the result is taken
```


The interaction across the manager, importer, and execution boundary follows:

```
Manager                           Importer                       Worker / Subprocess
   │                                 │                                    │
   │── start_import(def, context) ──►│                                    │
   │                                 │── fork_import() ──────────────────►│ (spawns child)
   │◄── ImportHandle ────────────────│                                    │
   │                                                                      │
   │── [loop] poll handle.is_ready ──────────────────────────────────────►│ (runs import_definition)
   │                                                                      │
   │◄── is_ready = True ──────────────────────────────────────────────────│
   │── read handle.result & close()
```

The protocol is structural, not a base class: the existing parse subprocess already has this shape,
and an importer wrapping a foreign runtime should not have to inherit from the Task SDK to be
supervised.

#### Alternatives considered

- **A worker-pool layer between manager and importer**, by analogy with the executor. Rejected: the
  analogy does not hold. On the execution side the first fork is a *distribution boundary* — the
  workload may leave the machine. Parsing has no such boundary; the layer would only add a process
  hop and a second place where process policy lives.
- **A thread pool in the manager.** Rejected: it contradicts fork safety — a fork wants no threads
  running and no locks held — and it would make thread safety a requirement of every third-party
  importer.
- **Keep the manager forking, and consult the importer only for the work inside the child.**
  Rejected: it cannot express "no process" or "a process of a different kind", which is the point.
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
                           ^^ previously ONE entry for the whole archive, with both
                              members imported into a single shared sys.modules
  catalog.yaml           → N definitions   one per Dag the manifest names
```

A queue entry names one definition, and the location it carries is that definition's
`relative_fileloc` — including for a definition inside a container, where it names the member and
not the container.

Two things follow, and are settled here rather than left to each importer:

- **Isolation is a property of the Dag, not of its container.** "One fork per Dag" is otherwise
  true except for archives, where import side effects still leak between members.
- **A definition answers for its own freshness and ordering.** Not every definition is a file on
  disk, so the queue asks the definition for its modification time rather than calling `stat` on a
  path that may not exist.

#### Alternatives considered

- **Keep the openable source as the unit and let importers batch internally.** This preserves
  today's archive behaviour exactly and keeps metric cardinality flat. Rejected: it makes isolation
  depend on how a deployment happens to package its Dags, and it has no answer at all for a format
  with no file per Dag. The cost of the decision taken is real and is recorded under Consequences.

### 3. Fork mechanics and collector discipline belong to the base class

Forking out of a long-lived process carries a memory contract that is easy to miss: the child shares
the parent's pages until something writes to them, and a garbage collection writes to every object
it inspects. One collection in a freshly forked child therefore copies most of the parent's heap to
achieve nothing.

```
manager process                                     parse child
──────────────────────────────────────────────      ────────────────────────
prepare bundles
warm_importers()       every importer realised, every warm_up() run
gc.collect()           nothing collectable is left to be frozen
gc.freeze()            the shared heap becomes permanent
   │
   ├─ per import, steady state ─────────────────
   │     fork_import():   gc.disable()
   │                      spawn_child()  ──fork──►  inherits the collector OFF
   │                      gc.enable()               gc.enable() once its own
   │                                                 setup is done
```

`fork_import()` is `final`. A third-party importer that forgets this degrades the entire Dag
processor and nobody can tell why, so the framework holds the rule rather than documenting it at
importer authors.

The base class does not itself call `os.fork()`. The child is the Dag processor's own machinery —
its request and result models, its comms proxying — so the framework passes a `spawn_child`
callable in through `ImportContext`, and an importer asks for isolation without knowing any of
that. `spawn_child` is **absent** whenever nobody is supervising child processes (a CLI command, a
test, anything importing definitions directly); process isolation is therefore something an importer
may be *denied*, not something it can assume, and `fork_import` absorbs that by falling back to an
inline import.

#### Alternatives considered

- **Document the rule for importer authors.** Rejected: an importer author who gets
  it wrong is invisible to the operator, and the damage degrades the entire Dag processor.
  Memory and collector hygiene must be enforced by the framework rather than left to
  third-party documentation.
- **`gc.freeze()`/`unfreeze()` around each fork instead of `disable()`/`enable()`.** Rejected:
  `unfreeze()` has no granularity. It empties the permanent generation wholesale, so a per-import
  `unfreeze()` would discard the startup freeze that the whole scheme depends on.

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
```

The Dag processor executes this lifecycle in `before_run()` before entering its loop:

```
Manager Startup (before_run)
  │
  ├── 1. Discover bundles
  ├── 2. warm_importers()  ──► calls warm_up() on all registered importers
  ├── 3. gc.collect()      ──► sweep transient setup allocations
  └── 4. gc.freeze()       ──► freeze shared memory pages
```

A failing `warm_up()` does not stop the Dag processor booting; the importer is left to fail at its
first real import, where the error has a definition to attach to.

#### Alternatives considered

- **A batch API — `import_definitions(list)`.** Rejected: every importer would have to honour it,
  and for Python it re-introduces the shared-process semantics that decision 2 removes. The lifetime
  guarantee buys the same amortisation without changing the shape of the call.

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

## Open questions

These are the decisions this ADR deliberately does not take. The decision is cheaper now than after
the extension point is public.

### Q1. What does `ImportHandle.result` carry, and where does serialisation run?

`import_definition` returns live Task SDK `DAG` objects. The reason the parse child exists at all is
that Dags are written against the Task SDK and must be serialised *in a Task SDK process*, so the
`airflow.sdk` and `airflow.models` hierarchies do not collide. The manager runs in server context.
An importer that imports inline therefore serialises in the wrong process.

- **(a) The handle's result is the serialised form.** Forking importers serialise in the child, as
  today; inline importers serialise in the manager. Inline `start_import` is then permitted only for
  importers that execute no definition-author code — a JSON parse does not, so the guarantee that
  matters is preserved. *Recommended.*
- **(b) The result stays `DagImportResult` and the manager serialises.** Simplest signature, but it
  puts SDK `DAG` objects in the server-context process for every importer, inline or not.
- **(c) Forbid inline imports entirely.** Restores a single answer, at the cost of the "a static
  format needs no process" property that motivates decision 1.

### Q2. Who runs callbacks for a non-Python Dag?

`on_failure_callback` / `on_success_callback` are definition-author code, run today inside the parse
child against a `DagBag`. Removing `DagBag` from the parse path leaves them without a home.

- **(a) A second importer capability** — `execute_callbacks(definition, requests, bundle)`,
  defaulting to unsupported, implemented only by the Python importer. Honest about the fact that
  running a callback is a language-runtime job, at the cost of a second method on the interface.
- **(b) Keep callbacks Python-only, outside the importer interface.** Smaller interface; no answer
  for a Java Dag whose failure callback is Java.

The core architectural question: **for a Dag authored in Java, whose runtime runs the
callback?**

### Q3. Java runtime launch — importer, coordinator, or a shared launcher?

This is the question that got ADR-0004 cut. A `JavaDagImporter` and the existing `JavaCoordinator`
both need the jars root, the manifest, the supervisor schema version, and JVM launch.

- **(a) Duplicate the knowledge.** No.
- **(b) A shared runtime launcher both delegate to.** *Recommended.*
- **(c) The importer delegates to the coordinator** — the original sketch, and the resolution that
  justified cutting ADR-0004. [ADR-0008 in PR #71929](https://github.com/apache/airflow/pull/71929)
  takes this route, with the coordinator handing out its own importer.

(b) and (c) are not exclusive; what must not happen is two independent launch paths.

## Consequences

### Positive

- A format that only has to be read costs no process. Today every format costs a fork.
- A foreign runtime is expressible without further core changes: AIP-108's language importers plug
  into `start_import` and bring their own process model with them.
- One Dag's import side effects cannot reach another's, including inside an archive.
- The collector discipline holds for third-party importers by construction rather than by
  documentation, and the manager's startup freeze survives a full run.
- Warming is honest about whether it helps: an importer is told whether a child will inherit this
  process's memory, so it does not grow a heap that no child will ever see.

### Negative

- **Metric, log and queue cardinality now track definition count.** A 20-member archive becomes 20
  queue entries, 20 log files and 20 forks where it was one of each. Deployments that ship Dags as
  large archives will see parse-loop throughput fall and per-file metric tags multiply. This is the
  accepted price of decision 2.
- The surface an importer author must understand grows: `start_import`, `ImportHandle`,
  `ImportContext`, `warm_up`, and the wire format — against `import_definition` alone today.
- `is_ready` is now on the manager's hot path. An importer whose readiness check blocks — a
  synchronous RPC, say — stalls the parse loop for every other definition. The obligation is on the
  importer and is not enforceable by the framework.
- A queued entry whose source has vanished between discovery and import is skipped rather than
  forked, so any callbacks attached to it are dropped rather than raised as a parse failure.

### Neutral

- An archive importer no longer needs a `start_import` of its own on the discovery path: its members
  are queued individually and each routes to the importer that handles the member.
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
- [ADR-0004](../lang-sdk/0004-dag-parsing.md) — language-specific Dag file processing; the coordinator
  bridge cut from AIP-108 scope pending this decision
- [ADR-0002](../lang-sdk/0002-workload-execution.md) — workload execution through coordinators; the
  execution-side fork this model deliberately does not mirror
- [apache/airflow#71929](https://github.com/apache/airflow/pull/71929) — native Dag processing,
  which consumes `AbstractDagImporter` from the coordinator side
- [apache/airflow#58143](https://github.com/apache/airflow/discussions/58143) — preventing
  copy-on-write in forked workers; the collector discipline decision 3 generalises
- AIP-92 — removing database access from the Dag file processor; a parallel workstream this model
  must not obstruct
