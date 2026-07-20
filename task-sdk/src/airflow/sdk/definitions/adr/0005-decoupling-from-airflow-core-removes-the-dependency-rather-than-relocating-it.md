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

# 5. Decoupling from airflow-core removes the dependency rather than relocating it

Date: 2026-07-20

## Status

Accepted

## Context

`adr/0003` establishes that the authoring package must be independent of
airflow-core. Turning that invariant into reality has been a long migration
rather than a single change, and the migration attracts a specific kind of
well-intentioned pull request that has to be refused: one that makes the
dependency *less visible* without making it go away.

The concrete case is SQLAlchemy. The goal for the Task SDK is that it does not
use SQLAlchemy at all — not that it uses it more tidily. A change that removed
`from airflow.utils.session import …` from a task-run path by threading a
caller-supplied `session` argument through instead was closed for exactly this
reason: it *moved* the session usage rather than removing it, and it passed a
**raw session** across the boundary, which is worse than importing the helper.
The import-level symptom disappeared; the architectural coupling got stronger,
because now an ORM object is part of the function's signature and every caller
must produce one.

This is hard to catch in review because the diff looks like progress. The
`check_core_imports_in_sdk` prek hook goes green — it checks imports, and the
import is gone. Line count goes down. The only thing that reveals the problem is
asking what the *runtime* dependency is after the change, which the diff does not
show.

The same pattern appears whenever a shared concern is being extracted: the
tempting move is to pass the airflow-core object in as a parameter, or to accept
it behind `TYPE_CHECKING`, or to look it up lazily inside the function body so no
top-level import exists. All three satisfy the hook. None of them make the
authoring package independently installable and usable, which is the property the
distribution boundary exists to provide.

The supported way to actually remove a dependency is to move the shared piece
into a place both distributions can depend on — the `shared/` distributions,
symlinked into each consumer — rather than to pass it across the seam. That
mechanism exists precisely so that "both sides need this" has an answer other
than "import it from the other side".

## Decision

A change that claims to decouple the authoring package from airflow-core must
remove the runtime dependency, not relocate it. Concretely:

- **State the runtime dependency after the change, not the import list.** A
  decoupling change is judged by what the package needs at run time, not by
  whether a top-level `import` line disappeared.
- **Do not pass airflow-core objects across the boundary to dodge an import.**
  Threading a caller-supplied ORM `session`, connection, or model instance
  through the signature is a stronger coupling than the import it replaced, not a
  weaker one.
- **A lazy or `TYPE_CHECKING`-guarded import of airflow-core is still a
  dependency.** It is acceptable only for a genuine circular-import or
  worker-isolation reason, stated at the site — never as a way to quiet the hook.
- **Shared code goes into a `shared/` distribution**, depended on by both sides,
  rather than being imported from airflow-core or duplicated into the SDK.
- **Partial decoupling is declared as partial.** If a change removes some of a
  dependency, it says what remains and links the tracking issue, rather than
  presenting the seam as closed.

## Consequences

- Decoupling work is slower and lands in fewer, larger steps, because the honest
  version usually requires moving a shared concern into `shared/` first.
- The `check_core_imports_in_sdk` hook stays a *floor*, not a proof. Reviewers
  cannot delegate this judgement to CI, which makes these changes expensive to
  review — deliberately so.
- Some intermediate states are legitimately partial. Naming the remaining
  coupling is required, so the next contributor does not mistake a half-migrated
  seam for a finished one.
- This decision rejects changes that genuinely improve local readability. That
  cost is accepted: a tidier call site is not worth a stronger distribution-level
  coupling.

A change **violates** this decision when it:

- removes an airflow-core import by adding an airflow-core object (session,
  ORM model, connection) to a function signature or constructor in this package;
- converts a top-level airflow-core import into a lazy or function-body import
  without a stated circular-import or isolation reason;
- adds `TYPE_CHECKING`-only airflow-core imports whose runtime counterpart is
  still required for the code to work;
- silences `check_core_imports_in_sdk` with an ignore marker instead of moving
  the shared piece into a `shared/` distribution;
- describes itself as removing a dependency while the package still cannot be
  imported and used without airflow-core installed.

A reviewer should ask, of any decoupling change: after this merges, what does the
Task SDK still need airflow-core for at run time — and is that list shorter than
before, or just written differently?

## Evidence

- #66925 — "Remove airflow-core session and serialization imports from
  `_run_task`": closed with the reasoning that it moved the session usage rather
  than removing it, and passed a raw session through, which is worse than
  importing the session helper from airflow-core.
- #53149 — "Set up process for sharing code between different components": the
  merged mechanism (`shared/` distributions symlinked into consumers) that gives
  "both sides need this" an answer other than a cross-distribution import.
- #53417 — "POC of a symlink-based code sharing approach": the design discussion
  behind that mechanism, including whether one shared library may depend on
  another.
- #65880 — "Add prek checks for cross-distribution import boundaries": extended
  the import guard to the shared distributions themselves, on the same principle
  — the shared libraries must import from `airflow_shared.*` rather than from
  `airflow` or `airflow.sdk`. An earlier attempt (#58825) was closed after the
  discussion established how much groundwork the boundary needed first.
- #55538 — "Remove SDK dependency from `SerializedDAG`" and #55108 — "Decouple
  `NotMapped` exception from Task SDK": merged examples of the supported shape,
  where the dependency is genuinely severed rather than re-expressed.
