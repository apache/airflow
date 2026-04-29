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

## AI-attribution footer

**Every contributor-facing template below ends with this
footer.** It calibrates the contributor's trust in the comment
(AI-drafted, may be wrong), reassures them that a human
maintainer is the real gate, and links to the documented
rationale for the two-stage process so the message is not just
a disclaimer but a pointer to the project's policy.

`<ai_attribution_footer>` expands to exactly:

```markdown
---

_Note: This comment was drafted by an AI-assisted triage tool and may contain mistakes. Once you have addressed the points above, an Apache Airflow maintainer — a real person — will take the next look at your PR. We use this [two-stage triage process](https://github.com/apache/airflow/blob/main/contributing-docs/25_maintainer_pr_triage.md#why-the-first-pass-is-automated) so that our maintainers' limited time is spent where it matters most: the conversation with you._
```

Rules for the footer:

- **Always include it** on every contributor-facing comment the
  skill posts — `draft`, `comment-only`, `close`,
  `review-nudge`, `reviewer-ping`, `mark-ready-with-ping`,
  `stale-draft-close`, `inactive-to-draft`,
  `stale-workflow-approval`. The only exception is the
  `suspicious-changes` template, which is short, operationally
  sensitive, and already directs the contributor to maintainers
  on Slack — adding the footer there would dilute the signal.
- **Do not paraphrase it.** Post the block verbatim. If the
  wording needs to change, update this section and propagate —
  do not drift per-template.
- **Keep the link to the rationale anchor** (`#why-the-first-
  pass-is-automated`). That section of the contributing doc is
  where the project explains why the first pass is automated
  and why that frees maintainers' time for human conversation.
  Changing the anchor text breaks the link.
- **Place it after all other body content, before any trailing
  blank lines.** The horizontal rule (`---`) separates it from
  the body so GitHub renders it as a clear footer.
- **The footer is italicised in one block** to read as meta-
  commentary rather than part of the primary message.

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

<ai_attribution_footer>
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

<ai_attribution_footer>
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

<ai_attribution_footer>
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
classification.

**Strongly prefer pinging the author** to address the
outstanding feedback. The skill may only flip to pinging the
reviewer when it has **inspected the review thread + the
commits pushed after the review** and judged that the feedback
was already addressed but never re-reviewed. Defaulting to the
reviewer-nudge variant without that inspection is noisy — it
burns a maintainer's attention on a PR whose owner hasn't
actually done the work yet.

### Default — author-primary nudge *(use unless the inspection below says otherwise)*

```markdown
@<author> — This PR has new commits since the last review requesting changes from <reviewers>. Could you address the outstanding review comments and either push a fix or reply in each thread explaining why the feedback doesn't apply? Once the threads are resolved please mark the PR as "Ready for review" and re-request review. Thanks!

<ai_attribution_footer>
```

### Reviewer-re-review nudge — only when the inspection shows the feedback has been addressed

```markdown
@<author> <reviewers> — This PR has new commits since the last review requesting changes, and the diff looks like it addresses the feedback (see <thread-links>). @<reviewers>, could you take another look when you have a chance to confirm? Thanks!

<ai_attribution_footer>
```

### How to decide which variant to use

Before drafting, fetch the post-review diff and the conversation
on each thread:

1. `gh api repos/apache/airflow/pulls/<N>/reviews/<review_id>/comments --jq '.'`
   to see the reviewer's line-level comments.
2. `gh pr diff <N> --repo apache/airflow` limited to the files
   the reviewer commented on.
3. Author replies in-thread (`reviewThreads.nodes.comments.nodes`
   from the batch query) where the author responded after the
   review.

Flip to the reviewer-re-review variant **only when all** of the
following are true:

- Every inline comment the reviewer left has either a code
  change in the post-review diff at or near the commented line,
  **or** an author reply in-thread explaining the
  intentional deviation.
- The thread-level replies read as "done" / "fixed" / "pushed a
  commit", not as "can you clarify" / "I disagree".
- At least one commit was pushed after the review's
  `submittedAt` timestamp.

Otherwise, stay with the author-primary nudge — the ball is in
the author's court and the reviewer should not be re-summoned.

If multiple reviewers are stale and only some have had their
feedback addressed, **use the default (author-primary) variant**
and list all reviewers in the mention — one less noisy message
is preferable to two split ones, and the author gets one
coherent to-do list.

---

## Reviewer ping

*(`reviewer-ping` — unresolved-review-thread ping)*

Used when the action is `ping` on a `deterministic_flag`
classification triggered by unresolved review threads (i.e.
the reviewer commented but the thread stayed unresolved and the
author may have responded).

**Strongly prefer pinging the author** to resolve the
outstanding threads. The skill may only flip to pinging the
reviewer when the same inspection protocol from
[`#review-nudge`](#review-nudge) above has confirmed that the
feedback was addressed and the threads just need a re-look to
be resolved.

### Default — author-primary nudge *(use unless the inspection below says otherwise)*

```markdown
@<author> — There are <N> unresolved review thread(s) on this PR from <reviewers>. Could you either push a fix or reply in each thread explaining why the feedback doesn't apply? Once you believe the feedback is addressed, mark the thread as resolved so the reviewer isn't re-pinged needlessly. Thanks!

<ai_attribution_footer>
```

### Reviewer-re-review nudge — only when the inspection shows the feedback has been addressed

```markdown
<reviewers> — @<author> appears to have addressed your review feedback (see the linked threads and the commits pushed since). Could you confirm and resolve the threads if you agree? Thanks!

@<author>, if any of the threads still need work on your side, please reply in-line and push a fix.

<ai_attribution_footer>
```

The decision rule is the same as `review-nudge`: go with the
author-primary nudge by default; only use the reviewer-re-review
variant after an explicit inspection confirms the comments have
been addressed in a post-review commit or resolved with an
in-thread reply.

---

## Mark ready with ping

*(`mark-ready-with-ping` — promote-and-invite-reviewers comment)*

Used when the action is `mark-ready-with-ping` (see
[`actions.md#mark-ready-with-ping`](actions.md)). The PR's
only outstanding signal is unresolved review threads, the
[`unresolved_threads_only_likely_addressed`](classify.md#sub-flag-unresolved_threads_only_likely_addressed)
heuristic fired, and we are promoting the PR to
`ready for maintainer review` while inviting the original
reviewer(s) to confirm the resolution.

```markdown
@<author> — Your unresolved review thread(s) from <reviewers> appear to have been addressed (post-review commits and/or in-thread replies on every thread, with the latest commit pushed after the most recent thread). I've added the `ready for maintainer review` label so the PR re-enters the maintainer review queue.

<reviewers> — could you take another look when you have a chance? If you agree the feedback was addressed, please mark the threads as resolved so the queue signal stays accurate. If a thread still needs work, please reply in-line — @<author> will follow up.

<ai_attribution_footer>
```

Notes on the body:

- **`@<author>` is mentioned once at the top.** They get one
  notification with the rationale (so they understand why the
  label appeared) and a clear ask if a thread comes back open.
- **Every reviewer with an unresolved thread is `@`-mentioned
  once** in the second paragraph. They get the prompt that
  matters for them — "please re-look and resolve the threads if
  you agree".
- **No "no rush" line.** Unlike the `draft` / `comment-only`
  templates, this one is announcing forward motion (PR
  promoted), not asking the author to fix something — the
  decompression line would read as out of place.
- **No "thanks!"** — per the global tone rules, sign-offs are
  noise.
- **The `<ai_attribution_footer>` still applies** so the
  reviewer knows the promotion is AI-drafted; if the
  heuristic was wrong they have a clear cue to push back
  rather than assuming a maintainer made the call.

If the heuristic was wrong (the reviewer disagrees that the
threads are addressed), the reviewer can re-open a thread,
remove the label, or comment back — the PR re-enters the
unresolved-threads triage path on the next sweep.

---

## Stale draft close

*(`stale-draft-close` — stale draft closing comment)*

Used by the stale-sweep flow when a draft PR's triage comment
is older than 7 days with no author reply (see
[`stale-sweeps.md#stale-draft`](stale-sweeps.md)).

```markdown
@<author> This draft PR has been inactive for <days_since_triage> days since the last triage comment and no response from the author. Closing to keep the queue clean.

You are welcome to reopen this PR when you resume work, or to open a new one addressing the issues previously raised. There is no rush — take your time.

<ai_attribution_footer>
```

### Untriaged-draft variant

Used for drafts that were never triaged but have gone 3+ weeks
with no activity:

```markdown
@<author> This draft PR has had no activity for <weeks_since_activity> weeks. Closing to keep the queue clean.

You are welcome to reopen and continue when you're ready. If you'd like to pick it back up, please rebase onto the current `<base>` branch first.

<ai_attribution_footer>
```

---

## Inactive to draft

*(`inactive-to-draft` — convert inactive non-draft to draft)*

Used by the stale-sweep flow when an open (non-draft) PR has
had no activity for 4+ weeks.

```markdown
@<author> This PR has had no activity for <weeks_since_activity> weeks. Converting to draft to signal that maintainer review is paused until you resume work.

When you're ready to continue, please rebase onto the current `<base>` branch, address any newly-appearing CI failures, and mark the PR as "Ready for review" again. There is no rush.

<ai_attribution_footer>
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

<ai_attribution_footer>
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

Do **not** append the `<ai_attribution_footer>` here. This
template is intentionally terse and already directs the
contributor to maintainers on Slack if the flag was in error —
adding the "an AI may have gotten this wrong" footer on a
suspicious-changes close would dilute the signal and give a
bad-faith actor a footnote to argue with.

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
| `Unaddressed Copilot review` | classification `stale_copilot_review` — unresolved review thread by a `copilot*[bot]` login older than 7 days with no author reply | "GitHub Copilot posted automated review comments on this PR that have been sitting unaddressed for more than a week. **Some of the Copilot suggestions may be irrelevant or incorrect** — that is expected. However it is the author's responsibility to go through each thread and either apply the fix, reply in-thread with a short explanation of why the suggestion does not apply, or resolve the thread if the feedback is no longer relevant. **Once you believe a thread is resolved — whether by pushing a fix or by explaining why the suggestion doesn't apply — please mark it as resolved yourself by clicking the 'Resolve conversation' button at the bottom of the thread.** Reviewers don't auto-close their own threads, so leaving threads unresolved (even when addressed) signals "still waiting on the author" and blocks the PR. Please walk through the threads: <thread_urls>." |
| `Unresolved review comments` | `unresolved_threads > 0` | "There are unresolved review threads on this PR. For each one: either apply the suggestion in a follow-up commit, or reply in-thread with a brief explanation of why the feedback doesn't apply, or resolve the thread if the feedback is no longer relevant. **Once you believe a thread is resolved — whether by pushing a fix or by explaining why the suggestion doesn't apply — it is the author's responsibility to mark it as resolved by clicking the 'Resolve conversation' button at the bottom of the thread.** Reviewers don't auto-close their own threads, so unresolved threads read as 'still waiting on the author' and block the PR from moving forward." |

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
- **Always include the `<ai_attribution_footer>`** on every
  contributor-facing template (the only exception is
  `suspicious-changes`; see the note there). The footer
  calibrates trust in the AI-drafted message and links to the
  project's documented two-stage-triage rationale.
- **Mentions: `@<author>` gets one mention per comment, at the
  top.** Further mentions beyond the first are noise — they
  all hit the same notification anyway.
- **Sign-off: none.** Don't add "Thanks," or the maintainer's
  name. The comment comes from the triage tool and reads as
  such; signing it adds noise and invites replies directed at
  the wrong human.
