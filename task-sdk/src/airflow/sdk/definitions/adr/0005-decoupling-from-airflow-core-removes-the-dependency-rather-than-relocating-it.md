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
airflow-core. Turning that invariant into reality has been a long migration, and
it attracts a specific well-intentioned PR that must be refused: one that makes
the dependency *less visible* without making it go away.

The concrete case is SQLAlchemy. The goal is that the Task SDK does not use it at
all — not more tidily. A change removing `from airflow.utils.session import …`
from a task-run path by threading a caller-supplied `session` through was closed
because it *moved* the usage rather than removing it, and passed a **raw session**
across the boundary — worse than importing the helper, because now an ORM object
is in the signature and every caller must produce one. This is hard to catch: the
`check_core_imports_in_sdk` hook goes green (the import is gone) and line count
drops; only asking what the *runtime* dependency is after the change reveals it.

The same pattern appears whenever a shared concern is extracted — pass the
airflow-core object as a parameter, accept it behind `TYPE_CHECKING`, or look it
up lazily in the function body. All three satisfy the hook; none make the package
independently installable. The supported fix is to move the shared piece into a
`shared/` distribution symlinked into each consumer, so "both sides need this" has
an answer other than "import it from the other side".

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

- Decoupling lands in fewer, larger steps, because the honest version usually
  requires moving a shared concern into `shared/` first.
- The `check_core_imports_in_sdk` hook stays a *floor*, not a proof — reviewers
  cannot delegate this judgement to CI, deliberately.
- Intermediate states are legitimately partial; naming the remaining coupling is
  required so the next contributor does not mistake it for finished.
- Changes that improve local readability are rejected — a tidier call site is not
  worth a stronger distribution-level coupling.

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

- #66925 — remove airflow-core session/serialization imports from `_run_task`;
  closed for moving the session usage and passing a raw session through.
- #53149 — the merged `shared/`-distribution mechanism giving "both sides need
  this" a non-import answer.
- #53417 — the POC design discussion behind that mechanism.
- #65880 — extended the import guard to the shared distributions themselves
  (importing from `airflow_shared.*`); an earlier attempt (#58825) was closed as
  needing groundwork first.
- #55538, #55108 — merged examples of the supported shape, where the dependency is
  genuinely severed rather than re-expressed.
