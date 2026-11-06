<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`pr-management-quick-merge` configuration (template)](#pr-management-quick-merge-configuration-template)
  - [Thresholds](#thresholds)
  - [Real-CI patterns](#real-ci-patterns)
  - [Path globs](#path-globs)
    - [`tier_a_allow_globs` — documentation / text only (highest confidence)](#tier_a_allow_globs--documentation--text-only-highest-confidence)
    - [`tier_b_allow_globs` — low-risk code (test / example only)](#tier_b_allow_globs--low-risk-code-test--example-only)
    - [`deny_globs` — absolute disqualifiers (consequential areas; one match drops the PR even at one line)](#deny_globs--absolute-disqualifiers-consequential-areas-one-match-drops-the-pr-even-at-one-line)
  - [Merge-command template](#merge-command-template)
  - [Approve action](#approve-action)
  - [Note on automated merge (Agentic Autonomous)](#note-on-automated-merge-agentic-autonomous)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# `pr-management-quick-merge` configuration (template)

Per-project configuration for the
[`pr-management-quick-merge`](../../skills/pr-management-quick-merge/SKILL.md)
skill. This is the **`_template` default**; new adopters copy it into their own
`<project-config>/pr-management-quick-merge-config.md` and tune the thresholds
and path globs for their repository layout.

The default globs below are shaped for a Python monorepo; an
adopter with a different layout replaces them wholesale. When a field is absent,
the skill falls back to the default noted in its row.

## Thresholds

| Key | Default | Meaning |
|---|---|---|
| `max_churn` | `20` | Maximum `additions + deletions` for a quick-merge candidate. Pure deletions count. |
| `max_files` | `3` | Maximum number of changed files. |
| `default_tiers` | `A,B` | Which tiers are surfaced when the run does not pass `tier:`. |

## Real-CI patterns

`real_ci_patterns` is **read from the shared
[`<project-config>/pr-management-config.md`](pr-management-config.md)** — do not
duplicate it here. The skill uses it for the
[Real-CI guard](../../skills/pr-management-triage/classify-and-act.md#real-ci-guard)
in gate G2 so a SUCCESS rollup that comes only from bot checks
(`Mergeable`/`DCO`/`boring-cyborg`) is not mistaken for green CI.

## Path globs

Matched against repo-relative POSIX paths. Deny is evaluated first and wins
(see [`candidate-rules.md` Path matching](../../skills/pr-management-quick-merge/candidate-rules.md#path-matching)).

### `tier_a_allow_globs` — documentation / text only (highest confidence)

```text
**/*.rst
**/*.md
**/docs/**
docs/**
**/newsfragments/**
**/changelog.rst
**/i18n/**
**/locales/**
**/*.po
spelling_wordlist.txt
```

### `tier_b_allow_globs` — low-risk code (test / example only)

```text
**/tests/**
**/test_*.py
**/*_test.py
**/examples/**
**/example_*/**
```

### `deny_globs` — absolute disqualifiers (consequential areas; one match drops the PR even at one line)

```text
**/migrations/**
**/versions/**
**/alembic*/**
pyproject.toml
**/pyproject.toml
uv.lock
setup.cfg
**/requirements*.txt
.github/**
**/Dockerfile*
scripts/ci/**
**/security/**
**/auth*/**
**/jwt*/**
# Core application code — Apache Airflow security-sensitive module paths.
airflow-core/src/airflow/jobs/**
airflow-core/src/airflow/models/**
airflow-core/src/airflow/executors/**
airflow-core/src/airflow/api_fastapi/**
airflow-core/src/airflow/serialization/**
task-sdk/src/airflow/sdk/execution_time/**
# Repository / GitHub settings (branch protection, merge buttons, PR cap)
.asf.yaml
```

Tune `deny_globs` toward over-inclusion: a false deny just means a PR waits for
`pr-management-code-review`; a false allow means a maintainer may merge a
core/security/build change after only a skim. When unsure, add the path here.

## Merge-command template

| Key | Default | Meaning |
|---|---|---|
| `merge_command_template` | `gh pr merge <N> --squash --repo apache/airflow` | The **copy-paste command the skill prints** next to each candidate for the maintainer to run *themselves*. The skill never executes it — it is presentation only (see [`SKILL.md` Golden rule 1](../../skills/pr-management-quick-merge/SKILL.md#golden-rules)). Set the merge method (`--squash` / `--merge` / `--rebase`) to your project's convention. Airflow: `.asf.yaml` enables squash merges only. |

## Approve action

The skill's one permitted mutation is an APPROVE review, submitted only on the
maintainer's explicit per-PR confirmation (see
[`SKILL.md` Step 3b](../../skills/pr-management-quick-merge/SKILL.md#step-3b--optional-approve-action)).

| Key | Default | Meaning |
|---|---|---|
| `enable_approve` | `true` | Whether the `[A]pprove NN` action is offered. Set `false` to make the skill purely read-only (surface-only, no approvals). |
| `approve_requires_diff_view` | `true` | When `true`, `[A]pprove NN` is rejected unless the maintainer ran `[V]iew diff` for that PR earlier in the session. Keep `true` — approving the maintainer's own review act should follow actually reading the diff. |
| `approve_body` | *(empty)* | Optional text posted as the APPROVE review body. **Leave empty** for a bare approve (no agent-drafted prose, no attribution footer needed). If set, the text is an agent-drafted GitHub message and the skill appends the `Drafted-by:` attribution footer per [`AGENTS.md`](../../AGENTS.md) automatically. |

The approve adds exactly one approving review (the maintainer's). It never uses
`--admin` or any branch-protection bypass: if the repo requires more than one
approval, one approve will not unblock the merge, and the skill says so rather
than implying the PR is mergeable.

## Note on automated merge (Agentic Autonomous)

This skill **surfaces** candidates; it does not merge. Automated merge — even
narrowly-scoped and per-PR-confirmed — is the framework's `mode:Autonomous`, off until
the Triage/Mentoring/Drafting modes have a two-quarter track record (see
[`docs/labels-and-capabilities.md`](../../docs/labels-and-capabilities.md)).
There is therefore **no `enable_merge` knob** in this config: the capability is
gated at the framework level, not per adopter. When the gate lifts, the merge
action will ship as a separate, explicitly Mode-D-labelled change with its own
config and safety protocol.
