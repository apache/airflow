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

- [Apache Airflow — pr-management-triage comment templates](#apache-airflow--pr-management-triage-comment-templates)
  - [Project-specific URLs](#project-specific-urls)
  - [Quality-criteria marker string](#quality-criteria-marker-string)
  - [AI-attribution footer](#ai-attribution-footer)
  - [Violations bullet format](#violations-bullet-format)
  - [Template bodies](#template-bodies)
    - [`request-author-confirmation`](#request-author-confirmation)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage comment templates

This file is the **per-project comment-body library** for the
[`pr-management-triage`](../../.claude/skills/pr-management-triage/SKILL.md) skill.
It supplies the Apache-Airflow-specific values the framework
needs to render its default template bodies — project-specific
URLs, the AI-attribution footer wording, and the violations
bullet format — plus the one body that intentionally diverges
from the framework default
([`request-author-confirmation`](#request-author-confirmation)).

The framework's
[`comment-templates.md`](../../.claude/skills/pr-management-triage/comment-templates.md)
ships the default bodies for every other template; the skill
reads this file for the URLs / wording and renders the
framework defaults with them.

New adopters should copy this file into their own
`<project-config>/pr-management-triage-comment-templates.md`
and replace every Airflow-specific URL and wording with their
project's equivalents.

## Project-specific URLs

| Placeholder | Project value |
|---|---|
| `<quality_criteria_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria` |
| `<two_stage_triage_rationale_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/25_maintainer_pr_triage.md#why-the-first-pass-is-automated` |
| `<project_display_name>` | `Apache Airflow` |
| `<merge_conflicts_rebase_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/10_working_with_git.rst` |
| `<static_checks_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/08_static_code_checks.rst` |
| `<testing_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/09_testing.rst` |
| `<docs_building_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/11_documentation_building.rst` |
| `<helm_tests_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/testing/helm_unit_tests.rst` |
| `<k8s_tests_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/testing/k8s_tests.rst` |
| `<provider_testing_url>` | `https://github.com/apache/airflow/blob/main/contributing-docs/12_provider_distributions.rst` |
| `<project_communication_channel>` | `Airflow Slack` |
| `<project_communication_url>` | `https://s.apache-airflow-slack.io` |

## Quality-criteria marker string

The framework uses a literal string to detect already-triaged PRs
(searches the PR body and comments for it). **Do not paraphrase**:
the same exact string must appear verbatim in every triage comment
the skill posts, and the `pr-management-stats` skill uses the same
marker for "is this PR triaged" detection.

| Concept | Value |
|---|---|
| Triage-marker visible link text | `Pull Request quality criteria` |

## AI-attribution footer

The verbatim block appended to every contributor-facing comment.
Customise the **wording** for the project but keep the
**structure** (italicised meta-block, link to two-stage-triage
rationale).

```markdown
---

_Note: This comment was drafted by an AI-assisted triage tool and may contain mistakes. Once you have addressed the points above, an Apache Airflow maintainer — a real person — will take the next look at your PR. We use this [two-stage triage process](https://github.com/apache/airflow/blob/main/contributing-docs/25_maintainer_pr_triage.md#why-the-first-pass-is-automated) so that our maintainers' limited time is spent where it matters most: the conversation with you._
```

## Violations bullet format

`<violations>` in the [template bodies](#template-bodies) below
expands to a bullet list — one bullet per category of failing
check or other violation. For this project, the bullet uses the
**bare-category form**:

```
- :x: **<category>**. See [docs](<doc_link>).
```

- `:x:` for severity `error`, `:warning:` for severity `warning`.
- `<category>` and `<doc_link>` are looked up in
  [`pr-management-triage-ci-check-map.md`](pr-management-triage-ci-check-map.md)
  (one bullet per **category**, regardless of how many individual
  check names matched it).

### Do not list individual failing job names

This overrides the framework default. The triage comment must
**not** enumerate the failing check names underneath the
category (e.g. avoid `:x: **Kubernetes tests** — Failing:
Kubernetes tests / K8S System:LocalExecutor-3.10-v1.30.13-false,
Kubernetes tests / K8S System:KubernetesExecutor-3.10-...,
(+1 more). See docs.`).

The same applies to the per-category remediation snippets the
framework's default rendering may add ("Run `prek run …`
locally and fix anything that flags." etc.) — drop them. The
linked doc has the steps; the bullet's job is to point at the
**category** and the **doc URL**, nothing more.

GitHub's Checks tab already shows the failing job names and
re-running steps; repeating them in the triage comment adds
noise without adding signal and pushes the comment past the
size where contributors actually read it. Multiple violations
in different categories produce multiple bullets in the same
list; multiple failing checks in the same category produce a
single bullet.

### Non-CI violations with a useful inline payload

A few violations carry a short payload that is genuinely useful
in the bullet itself (number of unresolved threads, count of
flagged PRs by the author, branch-behind-by-N). For those, the
bullet may append the payload inline after the category:

```
- :x: **<category>**: <short payload>. See [docs](<doc_link>).
```

Examples permitted by this rule:

- `- :x: **Unresolved review comments**: 3 thread(s). See [docs](…).`
- `- :x: **Multiple flagged PRs**: <flagged_count> of your PRs are currently flagged for quality issues. Please focus on those before opening new ones.`
  (already present verbatim in the [`close`](#close) template
  body below — kept as-is.)

The payload must be **one short clause**, not a list of
job names. If you find yourself listing three or more items in
the payload, the rule above applies — drop them and let the
doc link do the work.

## Template bodies

The framework's [`comment-templates.md`](../../.claude/skills/pr-management-triage/comment-templates.md)
provides default bodies for every triage template, with
project-specific URLs and wording resolved via the
[Project-specific URLs](#project-specific-urls) table above
and the [AI-attribution footer](#ai-attribution-footer)
section. This section contains only the project-specific
**body variants** where Apache Airflow diverges from the
framework default.

If a template name is not listed here, the skill uses the
framework default rendered with the URLs / placeholders from
the sections above.

### `request-author-confirmation`

The body **must** include the literal marker string
`ready for maintainer review confirmation` verbatim — the
framework's
[`viewer_confirmation_request_present`](../.claude/skills/pr-management-triage/classify-and-act.md#viewer_confirmation_request_present)
precondition searches for that exact text. Do not paraphrase
that string when adapting the rest of the body.

```markdown
@<author> — There are <N> unresolved review thread(s) on this PR, and you have engaged with each one (post-review commits and/or in-thread replies). Could you confirm whether you believe the feedback is fully addressed and the PR is ready for maintainer review confirmation?

If yes, reply here (a short "yes / ready" is fine) and an Apache Airflow maintainer will pick the PR up from the review queue on the next sweep.

If you are still working on a thread, please reply with what is outstanding so the threads stay unresolved on purpose.

<ai_attribution_footer>
```
