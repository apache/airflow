---
triage_review_imbalance:
  area: cli
  criticality: medium
  review_difficulty: medium
  structural_risk_paths:         # commands that mutate core state / cross into critical areas
    - "commands/db_command.py"
    - "commands/triggerer_command.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["bugraoz93", "potiuk", "dheerajturaga", "henry3260"]   # internal signal only — never @-mentioned
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
  # No small_diff override — inherits the central medium ceiling (100 lines / 6 files).
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# CLI — Agent Instructions

The `airflow` command-line interface: argument parsing, command definitions,
and the thin glue that dispatches to core services. Changes here are moderately
critical — a broken command is user-visible and can block operators — but the
blast radius is bounded and the code is mostly self-contained and reviewable
from the diff.

## Why changes here are moderate cost to review

- Command wiring is largely declarative; regressions are usually visible in the
  diff and caught by CLI parser tests.
- The expensive judgement is not in the diff at all: it is whether the capability
  already exists elsewhere in the command set. Nothing in a new command's code
  reveals that `airflow db clean` or `airflow db check-migrations` already does the
  job, so that check falls entirely on someone who knows the whole surface.
- The risk sits at the boundary where a command reaches into core services
  (scheduler, DB, Dag processing) — a change that _only_ touches parsing/output
  is low risk; one that changes what a command _does_ to core state inherits the
  criticality of whatever it calls into.

## Knowledge a reviewer (and a substantial contributor) needs

- The `cli_parser` structure and how commands/args are registered.
- The convention that the test location mirrors the source
  (`cli/cli_parser.py` → `tests/cli/test_cli_parser.py`).
- Where a command crosses into a critical area (e.g. commands that trigger
  scheduling or mutate DB state) — those parts should be reviewed against the
  relevant area's expectations, not just CLI conventions.

## Review criteria

Mined from real review discussion on ~204 merged and 70 closed-unmerged CLI
PRs — the changes reviewers repeatedly required, and the reasons CLI PRs get
turned away. **If you are preparing a change here, treat this as a pre-flight
checklist and fix every applicable item _before_ opening the PR.** Triage
applies the same list: a PR that lands with unmet items is drafted back to its
author with the specific gaps. Ordered roughly by how often reviewers raise
each one.

**Before you even start — is the CLI the right place?**

- [ ] **[transitional — AIP-94 airflowctl migration, in progress as of 2026]**
      The local `airflow` CLI is being superseded by `airflowctl`. Do **not**
      build new features on the legacy CLI, and route CLI data access through
      the `airflowctl` client rather than direct DB / manager access. New
      command logic should call the shared `ctl/commands/…` implementations, not
      duplicate them. **Local-only commands are excluded** — `scheduler`,
      `standalone`, the `db` group and the local execution paths (`dags test`,
      `tasks run`) cannot live in a remote REST client, so a flag on one of them
      is ordinary maintenance, and the existing `provide_session` usage in the
      local-administration commands is the established pattern, not a defect.
      See [`adr/0001`](adr/0001-legacy-cli-superseded-by-airflowctl.md).
      *(Remove/invert this item once the migration lands.)*
- [ ] The capability doesn't already exist under a generic command (e.g.
      `airflow db clean`, `airflow db check-migrations`) — a dedicated command
      that duplicates an existing one gets closed. **This is the single largest
      class of closed CLI PR here**, and the code is usually fine: a command whose
      body is an existing command with fixed arguments is documentation, not
      capability (ADR 0003). Name the exact existing invocation in the PR and say
      why it is insufficient.
- [ ] Check `main` first — several PRs here were closed by their own authors on
      finding the fix already merged. If an in-flight PR does the same thing,
      build on it rather than opening a near-duplicate; its existence is a reason
      to coordinate, not a ground for closing either PR (Airflow allows parallel
      work and the better PR wins).
- [ ] **Don't add a second way to express something that already has one.** Where a
      parameter is deprecated, the accepted fix is to remove it — not to reword the
      warning that explains the overlap.
- [ ] The change isn't a **fallback that papers over a failing path** — a command
      failing against its proper data source is a defect in that source path, and
      fixing it by reading somewhere else is refused (ADR 0004).

**Code the reviewers will require:**

- [ ] Imports at top of file — **no inline / function-body imports** (the
      codebase is actively removing lazy imports; only genuine circular-import
      or worker-isolation cases justify them).
- [ ] Every changed behaviour has a test that **pins the actual data flow**, not
      just that a function was called; mocks use `spec`/`autospec` (a bare
      `MagicMock` that passes on a misspelled method is rejected).
- [ ] No `raise AirflowException` — use a Python built-in or a dedicated
      exception. Validate CLI input at the **argparse layer** (`choices=`) so a
      bad value fails at parse time, not deep in the handler.
- [ ] Duplicated logic between CLI and API (or two branches) is factored into a
      shared helper / base class / mixin so the two can't drift.
- [ ] `-o` / structured-output commands keep stdout machine-parseable — logs and
      warnings go to **stderr**, only the JSON/YAML payload to stdout.
- [ ] Queries don't scale with input size (collapse per-item loops into one
      call); lookups keyed by non-globally-unique fields (e.g. `run_id`) add a
      `dag_id` scoping safeguard.
- [ ] **A Dag command's data source is explicit.** The bare command reads the
      serialized form from the database; an argument naming files or directories is
      what switches it to parsing local sources. No "try the DB, else parse local
      files" path — that makes output depend on whether a local SQLite file happens
      to exist. When a lookup matches several rows (several serialized versions of
      one Dag), take an argument or report all matches; don't pick one silently.
- [ ] **Decoupling work must actually remove the dependency, not relocate it.** In
      task-execution CLI paths, threading a caller-supplied — worse, a raw — ORM
      session through the call is not progress toward Task-SDK isolation; the goal is
      that the SDK side uses no SQLAlchemy at all. Partial decoupling that moves the
      import to the caller gets closed.
- [ ] Action-verb function names; no parameter names that shadow builtins
      (`print`, `input`, …); concise help text; flag names consistent across
      related commands.
- [ ] PR description, config docs, and the actual CLI args are in sync — no
      claims the diff doesn't implement; placeholder commands say so explicitly
      (`raise NotImplementedError`) rather than silently claiming a capability.
- [ ] Root cause fixed, not a fallback/workaround papering over it.
- [ ] User-facing change (new command, changed output/behaviour) carries a
      newsfragment; a command reaching into a critical area (scheduling, DB, Dag
      processing) is also checked against _that_ area's criteria.

> Mined from PR review history; extend as new patterns emerge, and add an
> equivalent `## Review criteria` section to the `AGENTS.md` of every other area
> over time. Re-check the AIP-94 item as the airflowctl migration progresses.

## Expectation for large changes

A new top-level command or a change to what an existing command does to core
state benefits from a short issue describing intent first; pure
parsing/output/help changes generally do not.
