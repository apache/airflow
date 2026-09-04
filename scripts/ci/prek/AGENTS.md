---
triage_review_imbalance:
  area: prek-hooks
  criticality: low               # not runtime code — but a broken hook blocks every contributor's commit
  review_difficulty: medium
  structural_risk_paths:         # shared or repo-wide blast radius — reviewed more carefully
    - "common_prek_utils.py"     # imported by nearly every hook script
    - "ruff_format.py"           # runs against every Python file in the repo
    - "check_imports_in_providers.py"
    - "run_mypy_full_dist_local_venv_or_breeze_in_ci.py"
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["potiuk", "ashb", "gopidesupavan", "amoghrajesh", "jscheffl", "bugraoz93", "jason810496"]  # from the `/scripts/` CODEOWNERS line — internal signal only, never @-mentioned
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
---

 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# scripts/ci/prek/ guidelines

## Overview

This directory contains prek (pre-commit) hook scripts. Shared utilities live in
`common_prek_utils.py` — always check there before duplicating logic.

## Breeze CI image scripts

Some prek scripts require the Breeze CI Docker image to run (e.g. `mypy-providers`, OpenAPI
spec generation, provider validation). These scripts use the `run_command_via_breeze_run`
helper from `common_prek_utils.py` to execute commands inside the container. Non-provider
mypy hooks (`mypy-airflow-core`, `mypy-task-sdk`, `mypy-shared-<dist>`, etc.) run locally via
`run_mypy_full_dist_local_venv_or_breeze_in_ci.py`, which builds a dedicated virtualenv per hook under `.build/mypy-venvs/`
using `uv sync --frozen --project <X> --group mypy` — no Breeze image needed.

When adding a new breeze-dependent hook:

1. Import and use `run_command_via_breeze_run` from `common_prek_utils` — do not shell out
   to `breeze` directly.
2. Register the hook at the **end** of the relevant `.pre-commit-config.yaml` file (breeze
   hooks are slow and should run after fast, local checks).

## Adding new hooks

- Scripts must be Python (not bash).
- Use helpers from `common_prek_utils.py` for path constants, console output, and breeze
  execution.
- Register the script in the appropriate `.pre-commit-config.yaml` (`/.pre-commit-config.yaml`
  for repo-wide hooks, `/airflow-core/.pre-commit-config.yaml` for core-specific hooks, or a
  provider-level config).

## Before opening a PR here — authoring-agent guard

**This is a low-criticality area — nothing here ships to users — but it has an
unusually wide _local_ blast radius.** Every hook registered in a
`.pre-commit-config.yaml` runs on every contributor's machine on every commit,
and again in CI. A hook that is too slow, too broadly scoped, or wrong in a way
that only shows up on someone else's platform does not break production; it
breaks everyone's ability to commit, which costs the project more time than the
hook saves.

So the bar here is not architectural standing — it is **empirical care**:

- Run the hook you changed against the **whole repo** (`prek run <id> --all-files`),
  not just your two touched files, and note how long it took.
- If you added a hook, be able to say _why_ its `files` pattern is the narrowest
  one that still catches the rule, and what the false-positive story is.
- If the hook needs the Breeze image or the network, expect to justify that —
  those are the two things that make a hook feel broken to contributors.

You do not need deep Airflow-internals experience to contribute here, and a
small, well-tested hook is a genuinely good first contribution. What gets a PR
drafted back is an untested hook, an over-broad pattern, or a rule that
contradicts what the docs say (see `## Review criteria`).

## Review criteria

Mined from real review discussion across ~319 commits touching this directory
and ~114 closed-unmerged pull requests touching it (30 with substantive
discussion) — the changes reviewers repeatedly required, and the reasons hook
PRs get turned away. **If you are preparing a change here, treat this as a pre-flight checklist
and fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered roughly by how often reviewers raise each one.

**Scoping and speed (the defining concern here):**

- [ ] **The `files:` pattern is as narrow as the rule allows**, and `exclude:` is
      used where a subtree is genuinely out of scope. A hook declared with a bare
      `files: \.py$` when it only ever inspects `providers/` taxes every commit in
      the repo for nothing.
- [ ] **The hook does the minimum work per invocation.** Prefer `pass_filenames: true`
      and act only on the passed files; a hook that rescans the whole tree on every
      commit needs a reason. Where a full scan is unavoidable, gate the expensive
      part behind an "inputs unchanged" short-circuit.
- [ ] **Slow or image-dependent hooks are registered at the _end_ of the config**
      (see `## Breeze CI image scripts`), so fast local checks fail first, and are
      moved to `stages: ['manual']` if they are too slow for every commit.
- [ ] **No new Breeze-image dependency unless the check genuinely cannot run
      locally** — use `run_command_via_breeze_run` from `common_prek_utils.py` if it
      must, never a direct shell-out to `breeze`.

**Determinism and failure messages:**

- [ ] **The failure message names the hook and says exactly what to change.** A
      contributor seeing the hook fail should not have to open the script to learn
      what it wants. Include the offending file and line where the check is
      per-file.
- [ ] **Auto-fix where the fix is unambiguous** (formatters, generated-file
      regeneration, sorted lists) and let the hook fail after writing, per the
      prek convention — do not just report a diff the contributor must apply by hand.
- [ ] **No network access from a hook that runs on every commit.** Network calls
      make the hook fail offline, on planes, and against rate limits; if a check
      truly needs the network, it belongs in `stages: ['manual']` or CI.
- [ ] **No dependence on local environment state** — resolve paths from
      `AIRFLOW_ROOT_PATH` and friends in `common_prek_utils.py`, not from the
      current working directory; do not read from a cache that can go stale
      relative to the working tree.
- [ ] **Pin what needs pinning** — `additional_dependencies`, `language_version`,
      and third-party repo `rev:` are frozen so the hook does not change behaviour
      under a contributor without a commit.

**Rule, docs, and tests move together:**

- [ ] **A new or changed enforcement hook has a test under `scripts/tests/ci/prek/`**
      covering both a passing and a failing input (`uv run --project scripts pytest
      scripts/tests/ -xvs`). A hook is code that runs on everyone's machine; it gets
      tested like code.
- [ ] **The documented convention is updated in the same PR.** If the hook enforces
      a rule described in `CLAUDE.md`, `contributing-docs/08_static_code_checks.rst`,
      or an area `AGENTS.md`, the prose and the hook must agree — a hook whose rule
      contradicts the docs is worse than no hook.
- [ ] **Allowlist / known-violation files shrink, never grow silently.** When
      introducing a hook against existing violations, the baseline file is
      explicitly acknowledged in the PR description as debt to pay down, and uses
      the shared `AllowlistManager` rather than a bespoke format.
- [ ] **A hook is the last mechanism to reach for, not the first.** If
      `.gitignore`, a config setting, or a one-line `entry` that simply fails on a
      matching path does the job, that is the change — a Python script for it gets
      pushed back (#46459). Where a rule bans something, the failure message must
      also say _why_ it is banned, not only that it is.
- [ ] **A hook that forbids something lands with the tree already clean and
      scoped to where the rule holds.** A hook banning `session.query` stalled on
      both counts: it had to exclude `providers/`, where the rule does not apply,
      and every existing violation had to be removed in the same change for the
      hook to pass at all (#45714). Budget for the cleanup before proposing the
      hook.
- [ ] **Detect drift against a committed artifact, not against another commit.**
      CI checks out a single commit, so a hook cannot compare the working tree to
      the previous revision. Where a check needs a baseline, generate it into a
      file that is committed and diffed — the shape that replaced a proposed
      compat-API check built on checking out the prior version (#44913), and the
      shape the `common.sql` API guard took when its first version was closed in
      favour of generated stubs shipped with the package (#27946 → #27962).
- [ ] **Shared logic goes into `common_prek_utils.py`** rather than a third copy
      that will drift — but a change to that file is reviewed as a change to every
      hook that imports it.
- [ ] **The hook fails when it stops covering its target.** A hook whose `files:`
      pattern or hardcoded path no longer matches anything must exit non-zero,
      not report success on an empty file list — a provider-metadata hook stayed
      green for months after the providers moved directory (`#57276`,
      `#57283`). See `adr/0004-...`.

**Version bumps, backports and scope:**

- [ ] **Hook tool versions are bumped by the automated `upgrade important CI
      environment` flow**, not by hand; hand-written bumps race the automated
      one and get closed as superseded (`#58305`, `#57361`).
- [ ] **A tool bump ships the output it changes.** Upgrading a formatter or doc
      generator means running it across the repository and committing the result
      in the same PR (`#65221`) — otherwise the next contributor's commit fails
      on a diff they did not cause.
- [ ] **Hook changes land on `main` and are not backported** to release branches
      (`#64325`); see `dev/adr/0004-...`.
- [ ] **Run the hook you changed before pushing.** PRs here are closed on
      failing static checks more often than on design objections (`#69535`,
      `#61853`).
- [ ] **Agent skills, prompts and IDE-context tooling are not hooks.** Proposals
      to make prek enforce agent workflow state have been declined; that work
      belongs in the separate skills tooling (`#63661`, `#63162`).

**Code quality reviewers consistently require:**

- [ ] **Scripts are Python, not bash**, use the shared path constants and console
      helpers, and carry the Apache license header (a hook enforces this).
- [ ] **Action-verb function names**; imports at module top; no heavy work at
      import time (it is paid on every commit).
- [ ] **The hook is registered in exactly one config** at the right level — repo-wide
      versus distribution-level — and its `id` matches how contributors will invoke
      it (`prek run <id>`).

> Mined from the commit and review history of this directory. The closed-unmerged
> sample now reaches back to 2022, when this directory was `scripts/ci/pre_commit/`
> and hooks ran under `pre-commit`; its dominant pattern — a change being closed
> and reopened from a branch in the apache repository so the then-separate
> image-build workflow would execute the modified script — no longer applies, since
> that workflow and its `pull_request_target` trigger have been removed. Treat
> pre-2025 evidence about how a hook change is _validated_ with that in mind; the
> rules about how a hook is _written_ carry over. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

### What these documents are currently good for

A validation pass ran this area's ADRs against the live open-PR queue and found
**zero firings across 3 PRs**. The sample is small, and that is part of the
point: hook changes arrive rarely and are usually written by people who already
know the conventions. These documents currently function as _review guidance and
onboarding context_, not as a mechanical gate. Do not sharpen the rules until
they catch something — inventing a trigger would produce false positives against
merged work and destroy the signal this measurement carries.

## Expectation for large changes

A single new hook does not need prior discussion — write it, test it, open the
PR. **Discuss first** when the change is structural: editing
`common_prek_utils.py` in a way every hook feels, introducing a new hook
_category_ or language runtime, changing which stage hooks run at, or adding a
check that will fail on existing code across many distributions. Those change
the commit experience for every contributor, and the trade-off is better settled
in an issue or on the dev list than in review.
