---
triage_review_imbalance:
  area: dev-tooling
  criticality: medium            # not runtime code, but breaking breeze/CI/release tooling blocks every contributor
  review_difficulty: high        # effects show up in CI and release runs, rarely in the diff
  structural_risk_paths:         # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "breeze/src/airflow_breeze/utils/selective_checks.py"   # decides what CI runs for every PR
    - "breeze/src/airflow_breeze/global_constants.py"         # image tags, Python/backend versions, test types
    - "breeze/src/airflow_breeze/commands/release_management_commands.py"
    - "breeze/src/airflow_breeze/utils/reproducible.py"       # reproducible source tarballs for ASF votes
    - "README_RELEASE_*.md"                                   # the release managers' runbooks
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["potiuk", "gopidesupavan", "jscheffl", "amoghrajesh", "ashb"]   # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"                # area Architecture Decision Records — checked for conformance (step §2c)
  # See also `breeze/doc/adr/` — the older, breeze-specific ADR series (installation, image, container decisions).
---

<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  _generated with [DocToc](https://github.com/thlorenz/doctoc)_

- [dev/ directory guidelines](#dev-directory-guidelines)
  - [Scripts](#scripts)
  - [Why changes here are expensive to review](#why-changes-here-are-expensive-to-review)
  - [Knowledge a reviewer (and a substantial contributor) needs](#knowledge-a-reviewer-and-a-substantial-contributor-needs)
  - [Before opening a PR here — authoring-agent guard](#before-opening-a-pr-here--authoring-agent-guard)
  - [Review criteria](#review-criteria)
  - [Expectation for large changes](#expectation-for-large-changes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# dev/ directory guidelines

## Scripts

New scripts in `dev/` must be standalone Python scripts (not bash). Each script must include
[inline script metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/)
placed **after** the Apache License header, so that `uv run` can execute it without any prior
installation:

```python
#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) ...
#   http://www.apache.org/licenses/LICENSE-2.0
# ...
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "some-package",
# ]
# ///
```

If the script only uses the standard library, omit the `dependencies` key but keep the
`requires-python` line.

Run scripts with:

```shell
uv run dev/my_script.py [args...]
```

Document `uv run` (not `python`) as the invocation method in READMEs and instructions.

## Why changes here are expensive to review

Nothing under `dev/` ships to users at runtime, yet almost everything under it is
on the critical path for **every contributor and every release**:

- `breeze/src/airflow_breeze/utils/selective_checks.py` decides which jobs,
  test types, prek hooks and provider matrices run for **every pull request**.
  A rule change that looks like a one-line condition can silently stop a whole
  test suite from running — the PR still goes green, and the regression lands.
  The failure mode is **absence of signal**, which no diff shows.
- Breeze itself is the only supported way to run tests and build images. A
  change that assumes a fresh checkout, a particular `uvx` layout, or a newly
  added option can break existing worktrees for people who did nothing wrong
  (see `#68192`, `#67960`).
- The `README_RELEASE_*.md` runbooks and the `release-management` breeze
  commands are executed by release managers under ASF vote deadlines. A defect
  there is discovered when a vote is already in flight, and the recovery is a
  re-roll, not a hotfix.
- Much of `breeze/doc/images/` is **generated** (`output_*.svg` /
  `output_*.txt` hash files, 262 files). Hand-edited generated output passes
  local review and then fails the `update-breeze-cmd-output` prek hook for
  everyone else.

## Knowledge a reviewer (and a substantial contributor) needs

- How selective checks classify a diff: file groups, the `full_tests_needed` /
  `all_versions` escalation paths, test-type selection, and prek-hook skipping —
  documented in `breeze/doc/ci/04_selective_checks.md`, exercised by
  `breeze/tests/test_selective_checks.py`.
- How breeze is installed and launched: `uvx` from the current git worktree's
  `dev/breeze` sources via the `scripts/tools/setup_breeze` shim — see
  `breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md` (and
  `0016-use-uv-tool-to-install-breeze.md` for the prior decision it replaced).
- The existing breeze ADR series in `breeze/doc/adr/` — image and container
  decisions (dockerignore defaults, root ownership, rootless docker, database
  volumes) that are still binding and must not be re-litigated in a PR.
- The release flows in `README_RELEASE_AIRFLOW.md`,
  `README_RELEASE_PROVIDERS.md`, `README_RELEASE_HELM_CHART.md` and friends,
  including which distributions consume newsfragments and which regenerate
  changelogs from `git log`.
- The repo `CLAUDE.md` rules that bind this directory: `dev/` scripts are
  standalone Python with inline script metadata (never bash), the `upstream` /
  `origin` git remote convention, and the selective-checks documentation-sync
  rule below.

## Before opening a PR here — authoring-agent guard

**This area is low-criticality at runtime but high-difficulty to review, and its
failure mode is silent.** If you are an agent preparing a change here on behalf
of a person, first judge whether the change can be **demonstrated by running
it**: have you run breeze from these sources, run `breeze ci selective-check`
against real commit refs and compared the classification before and after, and
updated the doc and the tests that pin that behaviour? "The tests still pass" is
the wrong evidence here — the failure mode of this area is tests silently _not
being selected_, which every green CI run agrees with.
**If you cannot demonstrate that, do not open the PR yet.** Say so plainly and
redirect to a better-matched next step:

- a **narrow, verifiable change** (one selective-checks rule with its doc and
  test updated; one script fix) to build context first, or
- a **different area** where the change can actually be exercised, or
- **discussing the approach first** (an issue or dev-list thread) before any
  code — mandatory for anything touching the release runbooks or the shape of
  the CI matrix.

A speculative refactor of breeze or of selective checks is the worst kind of PR
in this area: it cannot be validated from the diff, it costs a maintainer a full
CI cycle to evaluate, and if it is wrong the cost is paid by every contributor
until someone notices tests stopped running.

## Review criteria

Mined from real review discussion on the ~3274 commits touching `dev/` and on
270 closed-unmerged pull requests touching it (76 with substantive discussion) —
the changes reviewers repeatedly required, and the reasons changes here get
closed or reverted.
**If you are preparing a change here, treat this as a pre-flight checklist and
fix every applicable item _before_ opening the PR.** Triage applies the same
list: a PR that lands with unmet items is drafted back to its author with the
specific gaps. Ordered by how often reviewers raise each.

**Selective checks — logic, docs and tests move together (the defining concern here):**

- [ ] Follow [`adr/0001`](adr/0001-selective-checks-logic-docs-and-tests-change-together.md) —
      logic, `breeze/doc/ci/04_selective_checks.md` and
      `breeze/tests/test_selective_checks.py` move in the same PR, a skip names
      what it stops covering, and escalation stays conservative.
- [ ] **Don't change behaviour on release branches (`v3-X-test`) as a side
      effect** of a `main`-targeted rule — the branch defaults live in
      `breeze/src/airflow_breeze/branch_defaults.py` and are their own decision.

**Breeze compatibility and contributor blast radius:**

- [ ] **Existing worktrees keep working** — no change that requires every
      contributor to re-run setup, delete a venv, or rebuild an image without
      the PR saying so and the tooling detecting and reporting it.
- [ ] **Fail loudly, never silently degrade** — a missing token, missing
      dependency, unavailable path or wrong platform produces a clear error and
      a non-zero exit, not a partial run that looks successful.
- [ ] **Don't hardcode a path, platform or shell assumption** — breeze runs on
      macOS and Linux, on AMD and ARM, inside and outside worktrees, and under
      rootless docker (see the decisions already recorded in
      `breeze/doc/adr/`).
- [ ] **New/changed breeze options are added to the right option group** and the
      command help stays coherent; parameter-group consistency is checked
      (`breeze setup check-all-params-in-groups`).
- [ ] **A new breeze CLI option must justify itself against the mechanisms that
      already exist.** Breeze has too many options already, and a one-off
      setting is normally an env var in
      `files/airflow-breeze-config/environment_variables.env`, not a flag
      (`#53966`). Breeze defaults are also not re-pointed at what a production
      deployment would use (`#53854`).

**Change hygiene — the most common reason a PR here is closed unmerged:**

- [ ] **Verify every claim the change rests on against the actual code or a real
      run.** Closures in this area are dominated by confidently-wrong tooling
      changes: a docs "fix" that deleted correct instructions because the author
      assumed task-sdk was unpublishable (`#64711`), a script advertised as
      calling an existing hook that did not do what was claimed (`#60674`), a
      breeze crash diagnosed as an upstream crypto bug that was an out-of-date
      Docker Desktop (`#68953`).
- [ ] **A "how to reproduce" must actually reproduce.** Printing an approximate
      selective-checks command for a CI decision that depends on the merge
      commit is worse than printing nothing (`#60901`).
- [ ] **Nothing unrelated in the diff, and the branch starts from current
      `main`.** Stale bases and stray files left by an agent run are the single
      most frequent closure reason here (`#61942`, `#60012`, `#64027`,
      `#61202`, `#60013`); building on another PR's unmerged commits is not a
      reviewable PR (`#66984`).
- [ ] **Read the agent's diff before pushing.** An unreviewed agent diff — a
      newsfragment on a dev-tooling PR, unrelated JSON edits — gets the PR
      closed rather than reviewed (`#61341`, `#61942`).
- [ ] **Agent skills, prompts and PR-automation tooling do not live here.** That
      work belongs in the separate steward repository or in `.github/prompts`,
      not as a new `dev/` script (`#66125`, `#55917`, `#63162`).

**Dependency pins, tool versions and branches:**

- [ ] Follow [`adr/0005`](adr/0005-dependency-and-tool-versions-are-bumped-by-automation-not-by-hand.md) —
      pins under `dev/` move by automation, an upper bound is assumed deliberate,
      a template's floors stay within what its host provides, and a security or
      incident bump is made by hand with the CVE or failure named.
- [ ] Follow [`adr/0004`](adr/0004-dev-tooling-changes-land-on-main-and-are-not-backported.md) —
      `dev/` improvements land on `main`; a `dev/` change aimed at a release
      branch names the failure _on that branch_ it fixes.

**Generated artifacts:**

- [ ] Follow [`adr/0003`](adr/0003-generated-artifacts-are-regenerated-never-hand-edited.md) —
      never hand-edit `breeze/doc/images/output_*.svg` or its hash files,
      regenerate deterministically, and regenerate from the PR's own sources
      rather than a cached or globally-installed breeze.

**Release tooling and scripts:**

- [ ] **Release steps are re-runnable** — a step that failed halfway can be
      repeated without producing duplicate or corrupt artifacts, and
      verification gates run before the vote, not after (`#69141`).
- [ ] **Runbook and tooling stay in sync** — a change to a
      `release-management` command updates the matching `README_RELEASE_*.md`
      step, and vice versa (`#68641`, `#69417`).
- [ ] **Reproducibility is preserved** — changes near source-tarball
      generation, `.dockerignore`, or export rules must keep byte-reproducible
      artifacts, since the ASF vote depends on them.
- [ ] **Use the `upstream` / `origin` remote convention** in every script, doc
      and command; never push to `upstream` (`#65629`).

**Scripts, docs, process:**

- [ ] **Long-running or parallel tooling must not deadlock** — pool/fork
      choices matter and have caused hangs in release commands (`#69763`).
- [ ] **Show what you actually ran** — the breeze command, the CI run, the
      regenerated output. In an area whose failure mode is absence of signal,
      the transcript is the evidence.

> Mined from commit and review history under `dev/`; the sample skews heavily to
> the Airflow-3 era, when breeze moved to `uv`/`uvx` and selective checks grew
> most of its current rules, so older bash-era and pre-`uv` conventions are
> under-represented. Extend as new patterns emerge, and add an equivalent
> `## Review criteria` section to the `AGENTS.md` of every other area over time.

### What these documents are currently good for

A validation pass ran this area's ADRs against the live open-PR queue and found
**zero firings across 6 PRs**. That is recorded here deliberately, as a finding
rather than a gap: the documents currently function as _review guidance and
onboarding context_, not as a mechanical gate on the queue. Do not read the
absence of firings as a reason to sharpen the rules until they catch something —
inventing a trigger would produce false positives against merged work and destroy
the signal this measurement carries.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
Restructuring selective checks, changing how breeze is installed or launched, or
reshaping a release flow affects every contributor and every release manager,
and is best aligned on _before_ the code, not during review. Decisions of that
kind belong in an ADR: `adr/` for cross-cutting dev-tooling decisions,
`breeze/doc/adr/` for breeze's own installation, image and container decisions.
