<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Comment templates

Every comment the skill posts comes from this file. The
templates exist to keep the tone consistent across the project
and to make the `Pull Request quality criteria` marker show up
in the same place on every triage comment so
[`classify.md#c4-already_triaged`](classify.md) can find them.

Placeholders:

- `<author>` — PR author's GitHub login (without `@` — the
  template adds it)
- `<violations>` — the rendered violations list (see
  [`#violations-rendering`](#violations-rendering))
- `<base>` — PR base branch name (`main`, `v3-1-test`, …)
- `<commits_behind>` — integer
- `<flagged_count>` — number of currently-flagged PRs by this
  author (for the `close` template)
- `<reviewers>` — space-separated `@login` mentions
- `<days_since_triage>` — integer, for the stale-draft close
  comment

All templates use the canonical link to the quality-criteria
document:

```
[Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria)
```

Do not paraphrase this link — the literal text "Pull Request
quality criteria" is the triage-comment marker the skill
searches for when classifying already-triaged PRs. Changing the
anchor text breaks the re-triage skip logic.

---

## Draft comment

*(`draft` — convert-to-draft comment)*

Used when the action is `draft` (see
[`actions.md#draft`](actions.md)).

```markdown
@<author> This PR has been converted to **draft** because it does not yet meet our [Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).

**Issues found:**
<violations>

<rebase_note_if_needed>

**What to do next:**
<what_to_do_next>

Converting a PR to draft is **not** a rejection — it is an invitation to bring the PR up to the project's standards so that maintainer review time is spent productively. There is no rush — take your time and work at your own pace. We appreciate your contribution and are happy to wait for updates. If you have questions, feel free to ask on the [Airflow Slack](https://s.apache.org/airflow-slack).
```

`<rebase_note_if_needed>` is present **only** when
`commits_behind > 50`:

```markdown
> **Note:** Your branch is **<commits_behind> commits behind `<base>`**. Some check failures may be caused by changes in the base branch rather than by your PR. Please rebase your branch and push again to get up-to-date CI results.
```

`<what_to_do_next>` is the "what to do next" block read from
the [Converting to draft
section](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst)
of the contributing docs. If unavailable at runtime, fall back
to:

```markdown
- Fix each issue listed above.
- Make sure static checks pass locally (`prek run --from-ref <base> --stage pre-commit`).
- Mark the PR as "Ready for review" when you're done.
```

---

## Comment only

*(`comment-only` — post-without-drafting comment)*

Used when the action is `comment` for a `deterministic_flag`
classification.

```markdown
@<author> This PR has a few issues that need to be addressed before it can be reviewed — please see our [Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).

**Issues found:**
<violations>

<rebase_note_if_needed>

**What to do next:**
<what_to_do_next>

There is no rush — take your time and work at your own pace. We appreciate your contribution and are happy to wait for updates. If you have questions, feel free to ask on the [Airflow Slack](https://s.apache.org/airflow-slack).
```

Identical body to the `draft` variant minus the "converted to
draft" opener. The classification marker ("Pull Request quality
criteria" link text) is still present — re-triage logic
recognises both.

---

## Close

*(`close` — close-with-comment)*

Used when the action is `close` (deterministic flags, author
has >3 flagged PRs) — see
[`actions.md#close`](actions.md).

```markdown
@<author> This PR has been **closed because of multiple quality violations** — it does not meet our [Pull Request quality criteria](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).

**Issues found:**
<violations>
- :x: **Multiple flagged PRs**: You currently have **<flagged_count> PRs** flagged for quality issues in this repository. We recommend focusing on improving your existing PRs before opening new ones.

You are welcome to open a new PR that addresses the issues listed above. There is no rush — take your time and work at your own pace. If you have questions, feel free to ask on the [Airflow Slack](https://s.apache.org/airflow-slack).
```

The "Multiple flagged PRs" line is appended to the violations
list before rendering — do not re-render `<violations>` without
it. If `flagged_count <= 3` (which shouldn't happen on this
template per suggested-actions rules), render the close
comment without this extra line.

---

## Review nudge

*(`review-nudge` — stale `CHANGES_REQUESTED` ping)*

Used when the action is `ping` on a `stale_review`
classification. There are two sub-templates based on whether
the author already pinged the reviewer.

### The author already pinged the reviewer

```markdown
@<author> <reviewers> — This PR has new commits since the last review requesting changes, and it looks like the author has followed up. Could you take another look when you have a chance to see if the review comments have been addressed? Thanks!
```

### The author has not pinged the reviewer

```markdown
@<author> — This PR has new commits since the last review requesting changes from <reviewers>. If you believe you've addressed the feedback, please ping the reviewer(s) to request a re-review. Thanks!
```

If both sub-states apply for different reviewers (some pinged,
some not), concatenate the two bodies with a blank line
between.

---

## Reviewer ping

*(`reviewer-ping` — unresolved-review-thread ping)*

Used when the action is `ping` on a `deterministic_flag`
classification triggered by unresolved review threads (i.e.
the reviewer commented but the thread stayed unresolved and the
author may have responded).

```markdown
<reviewers> — Could you please check whether your review feedback on this PR has been addressed? @<author> appears to have responded to your comments. @<author>, do you believe the reviewer's concerns have been resolved?

If the concerns are resolved, please resolve the conversation threads. Thank you!
```

This differs from `review-nudge` by being scoped to individual
comment threads rather than a `CHANGES_REQUESTED` review.

---

## Stale draft close

*(`stale-draft-close` — stale draft closing comment)*

Used by the stale-sweep flow when a draft PR's triage comment
is older than 7 days with no author reply (see
[`stale-sweeps.md#stale-draft`](stale-sweeps.md)).

```markdown
@<author> This draft PR has been inactive for <days_since_triage> days since the last triage comment and no response from the author. Closing to keep the queue clean.

You are welcome to reopen this PR when you resume work, or to open a new one addressing the issues previously raised. There is no rush — take your time.
```

### Untriaged-draft variant

Used for drafts that were never triaged but have gone 3+ weeks
with no activity:

```markdown
@<author> This draft PR has had no activity for <weeks_since_activity> weeks. Closing to keep the queue clean.

You are welcome to reopen and continue when you're ready. If you'd like to pick it back up, please rebase onto the current `<base>` branch first.
```

---

## Inactive to draft

*(`inactive-to-draft` — convert inactive non-draft to draft)*

Used by the stale-sweep flow when an open (non-draft) PR has
had no activity for 4+ weeks.

```markdown
@<author> This PR has had no activity for <weeks_since_activity> weeks. Converting to draft to signal that maintainer review is paused until you resume work.

When you're ready to continue, please rebase onto the current `<base>` branch, address any newly-appearing CI failures, and mark the PR as "Ready for review" again. There is no rush.
```

No label is added — the conversion itself is the signal.

---

## Stale workflow approval

*(`stale-workflow-approval` — convert stale WF-approval to draft)*

Used by the stale-sweep flow when a PR awaiting workflow
approval has had no activity for 4+ weeks.

```markdown
@<author> This PR has been awaiting workflow approval with no activity for <weeks_since_activity> weeks. Converting to draft so it doesn't block the first-time-contributor review queue.

When you're ready to continue, please push a new commit (which will re-request workflow approval) and mark the PR as "Ready for review" again. There is no rush.
```

---

## Suspicious changes

*(`suspicious-changes` — flag-as-suspicious comment)*

Used by the `flag-suspicious` action when a first-time-
contributor workflow-approval PR shows tampering indicators
(see [`workflow-approval.md#what-counts-as-suspicious`](workflow-approval.md)).

Posted on every currently-open PR by the flagged author as
part of the per-author sweep — keep it short and non-accusatory.

```markdown
This PR has been closed because of suspicious changes detected in it or in another PR by the same author. If you believe this is in error, please contact the Airflow maintainers on the [Airflow Slack](https://s.apache.org/airflow-slack).
```

Do **not** enumerate which patterns triggered the flag in the
comment — that's operational detail that belongs in the
maintainer-side session summary, not in a message to the
contributor.

---

## Violations rendering

`<violations>` in the templates above expands to a bullet list,
one bullet per violation returned by the classifier. Each
bullet has the form:

```
- :x: **<category>**: <explanation> <details>
```

- `:x:` for severity `error`, `:warning:` for severity `warning`.
- `<category>` — short category name, e.g.
  `Merge conflicts`, `mypy (type checking)`,
  `Unresolved review comments`.
- `<explanation>` — one sentence stating what's wrong
  (e.g. *"Failing: mypy-airflow-core, mypy-providers"*).
- `<details>` — remediation guidance with a doc link (e.g.
  *"Run `prek run mypy-airflow-core --all-files` locally to
  reproduce. See [mypy checks docs](…)"*).

The category / explanation / details triples come from
`assess_pr_checks` / `assess_pr_conflicts` /
`assess_pr_unresolved_comments`-equivalent logic — this skill
reproduces those deterministic assessments without the LLM
layer. The canonical categories are:

| Category | Signal | Remediation pattern |
|---|---|---|
| `Merge conflicts` | `mergeable == CONFLICTING` | `git fetch upstream <base> && git rebase upstream/<base>`, resolve, push |
| `Failing CI checks` (fallback) | `checks_state == FAILURE`, no failed names available | `prek run --from-ref <base> --stage pre-commit`; docs links to static-checks + testing |
| `Pre-commit / static checks` | failed check name matches `static checks`, `pre-commit`, `prek` | `prek run --from-ref <base>` |
| `Ruff (linting / formatting)` | `ruff` | `prek run ruff --from-ref <base>` + `prek run ruff-format --from-ref <base>` |
| `mypy (type checking)` | `mypy-*` | `prek --stage manual mypy-<hook> --all-files` for each failing hook |
| `Unit tests` | `unit test`, `test-` | `breeze run pytest <path> -xvs` |
| `Build docs` | `docs`, `spellcheck-docs`, `build-docs` | `breeze build-docs` |
| `Helm tests` | `helm` | `breeze k8s run-complete-tests` |
| `Kubernetes tests` | `k8s`, `kubernetes` | "See the K8s testing documentation." |
| `Image build` | `build ci image`, `build prod image`, `ci-image`, `prod-image` | "Check Dockerfiles and dependencies." |
| `Provider tests` | `provider` | `breeze run pytest <provider-test-path> -xvs` |
| `Other failing CI checks` | anything uncategorised | `prek run --from-ref <base>` |
| `Unresolved review comments` | `unresolved_threads > 0` | "Review and resolve all inline comments. Click 'Resolve conversation' after addressing feedback." |

When a category has multiple matching failed check names,
list the first 5 and summarise the rest as `(+N more)`.

---

## Tone rules

- **No emoji in the body text.** The severity icons `:x:` and
  `:warning:` are the only "emoji" allowed, and only because
  GitHub renders them inline and they're informative.
- **No scare-quoted words.** Don't write *"This PR has 'issues'"*.
- **Always include the no-rush line** in `draft`, `comment-only`,
  and `close` — contributors who see triage output feel
  time-pressure by default; the explicit de-pressurisation is
  part of the contract.
- **Mentions: `@<author>` gets one mention per comment, at the
  top.** Further mentions beyond the first are noise — they
  all hit the same notification anyway.
- **Sign-off: none.** Don't add "Thanks," or the maintainer's
  name. The comment comes from the triage tool and reads as
  such; signing it adds noise and invites replies directed at
  the wrong human.
