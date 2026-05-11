<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — pr-management-triage comment templates](#apache-airflow--pr-management-triage-comment-templates)
  - [Project-specific URLs](#project-specific-urls)
  - [Quality-criteria marker string](#quality-criteria-marker-string)
  - [AI-attribution footer](#ai-attribution-footer)
  - [Template bodies](#template-bodies)
    - [`draft`](#draft)
    - [`comment-only`](#comment-only)
    - [`close`](#close)
    - [`review-nudge` (author-primary)](#review-nudge-author-primary)
    - [`review-nudge` (reviewer-re-review)](#review-nudge-reviewer-re-review)
    - [`reviewer-ping` (author-primary)](#reviewer-ping-author-primary)
    - [`reviewer-ping` (reviewer-re-review)](#reviewer-ping-reviewer-re-review)
    - [`mark-ready-with-ping`](#mark-ready-with-ping)
    - [`stale-draft-close` (triaged)](#stale-draft-close-triaged)
    - [`stale-draft-close` (untriaged)](#stale-draft-close-untriaged)
    - [`inactive-to-draft`](#inactive-to-draft)
    - [`stale-workflow-approval`](#stale-workflow-approval)
    - [`suspicious-changes` (no AI footer)](#suspicious-changes-no-ai-footer)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage comment templates

This file is the **per-project comment-body library** for the
[`pr-management-triage`](../../.claude/skills/pr-management-triage/SKILL.md) skill.
It contains the concrete templates used by the Apache Airflow
project.  New adopters should copy this file into their own
`<project-config>/pr-management-triage-comment-templates.md` and
replace every Airflow-specific URL and wording with their
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

## Template bodies

The framework's [`comment-templates.md`](../../.claude/skills/pr-management-triage/comment-templates.md)
documents the structural contract for each template (must-include
sections, ordering, footer rules).  This section contains the
actual bodies for the Apache Airflow project.

### `draft`

```markdown
@<author> Converting to **draft** — this PR doesn't yet meet our [Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).

<violations>

<rebase_note_if_needed>

See the linked criteria for how to fix each item, then mark the PR "Ready for review". This is **not** a rejection — just an invitation to bring the PR up to standard. No rush.

<ai_attribution_footer>
```

### `comment-only`

```markdown
@<author> A few things need addressing before review — see our [Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).

<violations>

<rebase_note_if_needed>

No rush.

<ai_attribution_footer>
```

### `close`

```markdown
@<author> Closing — this PR has multiple violations of our [Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).

<violations>
- :x: **Multiple flagged PRs**: <flagged_count> of your PRs are currently flagged for quality issues. Please focus on those before opening new ones.

This is **not** a rejection — you're welcome to open a new PR addressing the issues above. No rush.

<ai_attribution_footer>
```

### `review-nudge` (author-primary)

```markdown
@<author> — This PR has new commits since the last review requesting changes from <reviewers>. Could you address the outstanding review comments and either push a fix or reply in each thread explaining why the feedback doesn't apply? Once the threads are resolved please mark the PR as "Ready for review" and re-request review. Thanks!

<ai_attribution_footer>
```

### `review-nudge` (reviewer-re-review)

```markdown
@<author> <reviewers> — This PR has new commits since the last review requesting changes, and the diff looks like it addresses the feedback (see <thread-links>). @<reviewers>, could you take another look when you have a chance to confirm? Thanks!

<ai_attribution_footer>
```

### `reviewer-ping` (author-primary)

```markdown
@<author> — There are <N> unresolved review thread(s) on this PR from <reviewers>. Could you either push a fix or reply in each thread explaining why the feedback doesn't apply? Once you believe the feedback is addressed, mark the thread as resolved so the reviewer isn't re-pinged needlessly. Thanks!

<ai_attribution_footer>
```

### `reviewer-ping` (reviewer-re-review)

```markdown
<reviewers> — @<author> appears to have addressed your review feedback (see the linked threads and the commits pushed since). Could you confirm and resolve the threads if you agree? Thanks!

@<author>, if any of the threads still need work on your side, please reply in-line and push a fix.

<ai_attribution_footer>
```

### `mark-ready-with-ping`

```markdown
@<author> — Your unresolved review thread(s) from <reviewers> appear to have been addressed (post-review commits and/or in-thread replies on every thread, with the latest commit pushed after the most recent thread). I've added the `ready for maintainer review` label so the PR re-enters the maintainer review queue.

<reviewers> — could you take another look when you have a chance? If you agree the feedback was addressed, please mark the threads as resolved so the queue signal stays accurate. If a thread still needs work, please reply in-line — @<author> will follow up.

<ai_attribution_footer>
```

### `stale-draft-close` (triaged)

```markdown
@<author> This draft PR has been inactive for <days_since_triage> days since the last triage comment and no response from the author. Closing to keep the queue clean.

You are welcome to reopen this PR when you resume work, or to open a new one addressing the issues previously raised. There is no rush — take your time.

<ai_attribution_footer>
```

### `stale-draft-close` (untriaged)

```markdown
@<author> This draft PR has had no activity for <weeks_since_activity> weeks. Closing to keep the queue clean.

You are welcome to reopen and continue when you're ready. If you'd like to pick it back up, please rebase onto the current `<base>` branch first.

<ai_attribution_footer>
```

### `inactive-to-draft`

```markdown
@<author> This PR has had no activity for <weeks_since_activity> weeks. Converting to draft to signal that maintainer review is paused until you resume work.

When you're ready to continue, please rebase onto the current `<base>` branch, address any newly-appearing CI failures, and mark the PR as "Ready for review" again. There is no rush.

<ai_attribution_footer>
```

### `stale-workflow-approval`

```markdown
@<author> This PR has been awaiting workflow approval with no activity for <weeks_since_activity> weeks. Converting to draft so it doesn't block the first-time-contributor review queue.

When you're ready to continue, please push a new commit (which will re-request workflow approval) and mark the PR as "Ready for review" again. There is no rush.

<ai_attribution_footer>
```

### `suspicious-changes` (no AI footer)

```markdown
This PR has been closed because of suspicious changes detected in it or in another PR by the same author. If you believe this is in error, please contact the Airflow maintainers on the [Airflow Slack](https://s.apache-airflow-slack.io).
```
