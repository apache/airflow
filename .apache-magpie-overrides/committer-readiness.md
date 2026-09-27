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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — committer-readiness configuration](#apache-airflow--committer-readiness-configuration)
  - [Assessment window](#assessment-window)
  - [Committer thresholds](#committer-thresholds)
  - [PMC thresholds](#pmc-thresholds)
  - [Project-specific notes *(optional)*](#project-specific-notes-optional)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — committer-readiness configuration

Per-project thresholds for the
[`contributor-to-committer`](../../skills/contributor-to-committer/SKILL.md)
readiness tracker. Copy into your `<project-config>/` directory and
replace every TODO.

**Thresholds are optional.** If this file does not declare thresholds,
the skill falls back to `contributor-nomination-config.md` thresholds,
or asks the maintainer at run time. Only declare thresholds here if
your PMC has agreed on explicit criteria — they vary across projects
and there are no meaningful universal defaults.

**This file is separate from `contributor-nomination-config.md`** so
that the readiness tracker and the nomination brief can be tuned
independently. If your project uses the same bar for both, you can
set this file's thresholds to the same values and keep a single place
to update them.

---

## Assessment window

| Key | Value | Notes |
|---|---|---|
| `assessment_window_months` | `6` | How many months of activity to assess. 6 is common; slower-moving projects may prefer 12. |

---

## Committer thresholds

Calibrate against recent successful nominations on your project, not
against framework defaults. The numbers below are a low bar for a
mid-size active project.

| Dimension | Default (low bar) | Project value | Notes |
|---|---|---|---|
| `prs_merged` | `5` | `40` | Merged PRs — the clearest signal of sustained code contribution |
| `reviews_total` | `3` | `20` | Total review acts — shows engagement with others' work |
| `reviews_substantive` | `2` | `3` | Reviews with real inline feedback (≥ 3 comments or > 50 char body) |
| `issues_filed` | `0` | `2` | Set to 0 to treat as non-required; many valid tracks don't involve filing issues |
| `threads_commented` | `5` | `35` | PR/issue comment threads — basic community presence |
| `area_breadth` | `0` | `5` | Distinct `area:*` labels across merged PRs; 0 = no breadth requirement |

---

## PMC thresholds

PMC membership requires demonstrated community leadership beyond code.
Raise these well above the committer bar for any project that treats
PMC as a senior track.

| Dimension | Default (low bar) | Project value | Notes |
|---|---|---|---|
| `prs_merged` | `10` | `40` | |
| `reviews_total` | `8` | `100` | PMC members are expected to help evaluate others' work |
| `reviews_substantive` | `4` | `8` | |
| `issues_filed` | `0` | `2` | |
| `threads_commented` | `10` | `150` | |
| `area_breadth` | `2` | `8` | PMC members typically span multiple project areas |

---

## Automated and low-signal contributions

How the readiness tracker discounts visibly automated or low-signal GitHub activity.
The full definition — detection heuristics, aggregation, and how the brief reports raw and adjusted counts — is [`automated-contributions.md`](https://github.com/apache/magpie/blob/main/skills/contributor-nomination/automated-contributions.md).

The discount is a signal for the humans reading the brief, never an automatic disqualification.
Using AI tools, and disclosing that use, is not penalised; only restatement, content maintainers pushed back on, and work closed after that pushback are discounted.

Each key is resolved from this file first, then from `contributor-nomination-config.md`, then from the default below.

| Key | Default | Project value | Notes |
|---|---|---|---|
| `automated_contribution_weight` | `0.25` | | Weight (0–1) of a merged or open PR, issue, review or comment that drew maintainer pushback as looking generated, unreviewed, restating, fabricated, or unwanted |
| `restatement_comment_weight` | `0` | | Weight (0–1) of a comment or review body that only restates the description, earlier comments, or the diff |
| `closed_after_pushback_weight` | `0` | | Weight (0–1) of a PR or issue closed unmerged after that pushback; `0` removes it from every metric |
| `automated_pushback_phrases` | empty | | Extra phrases your maintainers use when pushing back, added to the generic list |

Set all three weights to `1` to turn the arithmetic off; flagged items are still listed in the brief.

### Project expectations for AI-assisted contributions

List the documents in which your project states what it expects from AI-assisted and automated contributions — a generative-AI contribution policy, PR guidelines, a review or triage guide.
Use paths relative to the repository root, or `https://` URLs, optionally with a `#section` anchor.

```yaml
automated_contribution_expectations:
  - contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions
  - contributing-docs/25_maintainer_pr_triage.md#why-the-first-pass-is-automated
  - contributing-docs/25_maintainer_pr_triage.md#for-contributors
```

When the list is present, the skill reads each document, judges contributions against it first, and cites the document and section each flagged item conflicts with.
When it is empty or none of the documents can be read, the skill falls back to the framework's generic heuristics and says so in the brief.
The skill does not go looking for policy documents this list does not name.

---

## Project-specific notes *(optional)*

Free text surfaced at the top of every readiness brief. Use for norms
the maintainer should see — e.g. multi-repo projects, non-GitHub
contribution tracks that are particularly valued, or cultural notes
about how the PMC calibrates nominations.

```text
Source: COMMITTERS.rst ("Guidelines to become an Airflow Committer" and
"Guidelines for promoting Committers to Airflow PMC"). There is no strict
numeric protocol; the PMC weighs combined contributions across areas.
- The numeric thresholds are a floor for surfacing candidates, never a
  decision rule: activity volume alone does not make a candidate. Breadth
  across areas, dev list participation, release testing and sustained
  activity weigh as much as the counts.
- Committer prerequisites: consistent contribution over at least three
  months; visibility on the dev list, Slack or GitHub issues/discussions
  (including non-binding votes and testing release candidates); helping
  other contributors (reviews, constructive feedback); contributions to
  community health.
- Non-code paths count: it is possible to become a committer (and PMC
  member) without changing code, but only with visible presence in the
  community channels. Such exceptions are rare.
- Areas the PMC looks at: Airflow Core, Task SDK, airflowctl, API, Docker
  image, Helm chart, dev tools (Breeze / CI), providers, security team work,
  issue triage, documentation.
- PMC: committer for at least 3 months; currently active; consistent
  voting on release candidates for at least the past 3 release cycles;
  AIP engagement; reviews and merges; community involvement.
- Airflow spans one main repository (apache/airflow) plus apache/airflow-site
  and apache/airflow-client-* repos; check activity across them.
```
