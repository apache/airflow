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

# 3. No new command for a capability that already exists

Date: 2026-07-20

## Status

Accepted

## Context

The CLI is the most inviting place to contribute: a command is a small,
self-contained, easily-tested diff with an obvious user story. That is why the
largest class of closed-unmerged PRs here is a well-written, well-tested command
that does something the CLI could already do — a cleanup command for a table
`airflow db clean --tables <name> --clean-before-timestamp <ts>` already purges
(the set registered in `airflow/utils/db_cleanup.py`), a pending-schema report
when `airflow db check-migrations -t 0` already exits non-zero on a head mismatch,
a warning explaining a parameter whose real fix is deletion.

None are closed because the code was bad; the cost avoided is *surface*. Every
command is a public contract — it appears in help and the command reference, is
scripted by operators, accretes its own bugs and arguments, and under the CLI
compatibility promise is far harder to withdraw than to add. A second spelling
doubles that cost, adds nothing new, and splits one behaviour across two entry
points that drift. The same refuses a second parameter for one concept: it is
ambiguous, and the fix is to delete the deprecated spelling, not document the
overlap. The rule is stricter here because the redundancy is invisible in the
diff — only someone who knows the generic commands can see it, so it is a
reviewer's burden unless the author discharges it first.

## Decision

**New CLI surface is added only for capability that does not already exist.**

- **Search the generic commands before proposing a specific one.** The `db`,
  `dags`, `tasks`, `variables`, `connections` and `pools` groups already carry broad
  verbs — `clean`, `check-migrations`, `list`, `test`, `export`, `import` — that
  cover most proposals arriving as new commands.
- **A wrapper is not a capability.** If the implementation of the new command is a
  call to an existing command with fixed arguments, the answer is documentation or a
  flag on the existing command, not a new one.
- **Prefer a flag on the existing command to a new command** when the behaviour is a
  narrowing of something already supported — and prefer nothing at all when a
  documented invocation already does it.
- **Do not add a second parameter that expresses an existing concept.** When two
  spellings coexist because one is deprecated, the change is to remove the
  deprecated one, not to improve the warning that explains the overlap.
- **Check for a fix already on `main`, and for an in-flight PR.** Several PRs here
  were closed by their own authors on discovering the behaviour had already landed.
  Note the asymmetry: surface that already exists on `main` makes a change
  redundant, whereas a competing *open* PR does not. Airflow allows parallel work
  and the better PR wins (root `CLAUDE.md`;
  `contributing-docs/04_how_to_contribute.rst`), so an open duplicate is a triage
  comment — "see also #NNNN" — never a ground for closing.
- **New capability belongs on `airflowctl`, not the legacy CLI** — this decision
  sits under ADR 0001, which governs *where* genuinely new surface goes once the
  capability question is settled.
- **An AIP-94 port is not new surface.** A PR that adds an `airflowctl` command
  for a capability the legacy CLI already has is executing the migration
  [ADR 1](0001-legacy-cli-superseded-by-airflowctl.md) mandates. The legacy path
  existing on `main` is the *premise* of such a PR, not an objection to it, and the
  author owes no argument that the legacy path is "insufficient" — the point is
  that it is being retired. The recognisable shape is an `airflowctl` command
  added alongside a `@deprecated_for_airflowctl(...)` marking of the legacy
  command and its registration in `test_command_deprecations.py`; a port that
  lands the `airflowctl` half first is the same migration in two steps. The two
  ADRs compose: ADR 3 asks whether the *capability* is new, ADR 1 asks where it
  lives — a port answers "no" to the first and "airflowctl" to the second, which
  is exactly the intended destination.

## Consequences

- The command set stays small and the reference stays a list of distinct
  capabilities, not aliases.
- Behaviour has one implementation and one place to fix.
- The cost falls on contributors (know the generic commands first) and reviewers
  (say no to working code); making the rule explicit keeps that "no" from reading
  as arbitrary.

A change **violates** this decision when it:

- adds a command whose body is a call to an existing command with fixed arguments;
- adds a command for a table, object, or condition already reachable through a
  generic verb such as `db clean` or `db check-migrations`;
- adds an argument that expresses the same concept as an existing one, rather than
  removing the deprecated spelling;
- adds surface for behaviour that already exists on `main`, without saying why the
  existing path is insufficient — porting an existing legacy-CLI capability to
  `airflowctl` under AIP-94 is the migration ADR 0001 directs, not new surface, and
  is exempt;
- proposes new capability on the legacy `airflow` CLI where ADR 0001 directs it to
  `airflowctl`.

## Evidence

- #68964 — clean tested command to clean expired sessions, closed because
  `db clean --tables session` already purges them. Caveat: `db_cleanup.py`
  registers `session` only under `FabAuthManager` with `fab.session_backend =
  database`; the unconditional entries (`revoked_token`, `log`, `job`, `dag_run`)
  illustrate the rule more cleanly.
- #58584 — `airflow db would-migrate`, closed: `db check-migrations -t 0` already reports the head mismatch.
- #61319 — redirected from rewording a `dags next-execution` warning to removing the deprecated `interval` parameter.
- #62454 — caution, not an example: no command, two approvals, closed after #62626 landed the same validation first — outraced, not declined on scope.
