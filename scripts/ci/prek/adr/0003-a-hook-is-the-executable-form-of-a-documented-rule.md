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

# 3. A hook is the executable form of a documented rule

Date: 2026-07-20

## Status

Accepted

## Decision

- A hook that enforces a convention and the prose that describes that convention
  change in the same pull request. If a hook's rule and the documentation
  disagree, that is a defect in the pull request, not a follow-up.
- A new or behaviour-changing enforcement hook ships with tests under
  `scripts/tests/ci/prek/`, covering at minimum one input that must pass and one
  that must fail. Hooks are run with `prek run <id>`; their tests are run with
  `uv run --project scripts pytest scripts/tests/ -xvs`.
- A hook introduced against pre-existing violations records its baseline through
  the shared `AllowlistManager` in `common_prek_utils.py`, not a bespoke file
  format, and the pull request states the baseline as debt to pay down. The
  allowlist shrinks over time; it does not grow silently.
- Where two artifacts must agree — a version pinned in several places, a
  registry and the code that populates it, a command list and the doc that
  documents it — the hook is written to *check the agreement*, so the invariant
  is enforced rather than merely described.

## Context

Airflow's conventions live in prose — `CLAUDE.md`, the contributing docs, the per-area
`AGENTS.md` files — and prose does not run. A convention in a document alone is followed
by the contributors who read it and silently violated by everyone else, including agents
generating code from a partial view of the repository.

A hook is the version of that rule that executes, which gives it an authority the prose
lacks and creates the failure this decision guards: when the two disagree, the hook wins
in practice while the document keeps teaching something else — a contributor reads the
doc, writes conforming code, and the commit is rejected by a rule nobody told them about.
That is worse than no hook, which at least leaves the document as the single honest source
of truth. The testing requirement follows: a hook runs on every commit, its false
positives block work repository-wide and its false negatives let the rule rot, and the
directory's history is largely hooks that *keep two things in sync* — each only as good as
the test that proves it still fires.

## Consequences

Conventions become enforceable rather than aspirational, and the document explaining a
rule can be trusted to match what the tooling accepts; the tests make a hook safe to
refactor later. The cost is real — adding a hook is a script, its tests, and a doc edit,
typically three files where a contributor expected one.

A change **violates** this decision when it:

- adds or changes an enforcement hook without a corresponding test under
  `scripts/tests/ci/prek/`, or with a test that only exercises the passing case
  and never proves the check fires;
- changes what a hook accepts or rejects while leaving the documented convention
  — in `CLAUDE.md`, the contributing docs, or an area `AGENTS.md` — describing
  the old rule;
- documents a new convention in prose while leaving an existing hook enforcing
  something incompatible with it;
- introduces a baseline or known-violations file in a bespoke format instead of
  the shared `AllowlistManager`, or adds entries to an existing allowlist as a
  way of passing the check rather than fixing the violation;
- duplicates shared helper logic into a new hook script instead of using or
  extending `common_prek_utils.py`, so the two copies can drift apart.

## Evidence

- #68204 — sync `AGENTS.md` commands from the contributing docs via a prek hook, making the doc-to-doc agreement executable.
- #69338 — add a prek hook keeping the Go toolchain version in sync across its several pin sites.
- #68448 — use a pre-commit hook to keep Java versions in sync.
- #63757 — add a pre-commit script checking that the code and the metrics registry stay synced.
- #69327 — consolidate the per-hook allowlist handling into a shared `AllowlistManager`.
- #69057 — move `known_airflow_exceptions.txt` out of `scripts/` into `generated/`, treating the baseline as generated state rather than hand-edited source.
