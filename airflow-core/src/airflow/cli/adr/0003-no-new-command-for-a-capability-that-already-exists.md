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

The CLI is the most inviting place in the codebase to contribute: a command is a
small, self-contained diff with an obvious user story, and the change is easy to
test. That is exactly why the largest single class of closed-unmerged PRs here is
a well-written, well-tested command that does something the CLI could already do.

The pattern is consistent. A cleanup command is proposed for a table that
`airflow db clean --tables <name> --clean-before-timestamp <ts>` already purges —
`revoked_token`, `log`, `deadline` and the rest of the table set registered in
`airflow/utils/db_cleanup.py`. A
command to report pending schema changes is proposed when `airflow db
check-migrations -t 0` already exits non-zero and prints the head mismatch. A
warning is proposed to explain a parameter whose real fix is that the parameter is
deprecated and should be removed rather than described.

Each of these is closed, and none of them are closed because the code was bad. The
cost being avoided is not review effort, it is surface. Every command is a public
contract: it appears in help output and in the command reference, it is scripted
against by operators, it acquires its own bug reports and its own arguments over
time, and — because the CLI carries a compatibility promise — it is far harder to
withdraw than it was to add. A second spelling of an existing capability doubles
that cost while adding nothing a user could not already do, and it splits the
maintenance of one behaviour across two entry points that will drift.

The same reasoning refuses a *second way of saying the same thing* inside one
command. Two parameters that express the same scheduling concept are not twice as
expressive; they are ambiguous, and the resolution is to delete the deprecated one,
not to document the overlap more carefully.

This is a stricter rule here than elsewhere in the codebase precisely because the
alternative is invisible: nothing in a diff shows that the capability already
exists. Only someone who knows the generic commands can see it, which makes it a
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

- The command set stays small enough to hold in your head, and the command
  reference stays a description of distinct capabilities rather than a list of
  aliases.
- Behaviour has one implementation and one place to fix, instead of two entry points
  that drift.
- The cost falls on contributors, who must know the generic commands before they can
  land a specific one — and on reviewers, who must say no to competent, working code.
  Making the rule explicit here is what keeps that "no" from reading as arbitrary.

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

- #68964 — "Add command to clean expired sessions": a clean diff with a unit test,
  closed because expired sessions are already purged by
  `airflow db clean --tables session --clean-before-timestamp <now>`; the command
  would have wrapped exactly that call. Note the condition — `db_cleanup.py`
  registers the `session` table only when the auth manager is `FabAuthManager`
  *and* `fab.session_backend` is `database`, so under `SimpleAuthManager` that
  exact invocation is not available. The unconditional entries (`revoked_token`,
  `log`, `job`, `dag_run`, …) are the cleaner illustration of the rule.
- #58584 — "CLI: Add `airflow db would-migrate` command": closed once reviewers
  pointed at `airflow db check-migrations -t 0`, which already exits non-zero and
  reports the head mismatch between the database and the source tree; the author
  agreed it negated the PR.
- #61319 — clarifying the warning for partition-driven Dags in `dags
  next-execution`: redirected from rewording the warning to removing the deprecated
  `interval` parameter, on the grounds that having two near-identical ways to
  express a schedule is itself the confusion.
- #62454 — mapped-task `map_index` bounds validation in the task CLI. Listed here
  as a caution rather than an example: it added no command, it carried two
  committer approvals, and its author closed it after #62626 landed the same
  validation first. It was outraced, not declined on scope — the rule this ADR
  states did not apply to it.
