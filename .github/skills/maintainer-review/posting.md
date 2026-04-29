<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Posting reviews — `gh pr review` recipes and templates

This file is the canonical reference for how the skill turns the
combined findings list (after [`review-flow.md`](review-flow.md)
Step 5 / 6) into an actual GitHub review submission, and the
verbatim review-body templates the skill uses.

---

## Disposition

The disposition is one of three GitHub review submissions:

| Disposition | `gh pr review` flag | When |
|---|---|---|
| `APPROVE` | `--approve` | green CI, no unresolved threads, no maintainer conflicts (Golden rule 7), zero `blocking`/`major` findings, only `nit`/`minor` left, all author questions answered |
| `REQUEST_CHANGES` | `--request-changes` | ≥ 1 `blocking`, OR ≥ 2 `major`, OR `major` + unanswered author question, OR a finding the maintainer wants to gate the merge on |
| `COMMENT` | `--comment` | everything else: mixed `minor` findings, CI pending, threads open, maintainer wants observations without gating |

Auto-pick uses these rules and shows reasoning to the
maintainer (Step 6 of [`review-flow.md`](review-flow.md)).
Maintainer can override with `[A]`/`[R]`/`[C]`.

Golden rule 7 (`SKILL.md`) downgrades any auto-`APPROVE` if
unresolved threads / pending other-maintainer reviews exist.
Golden rule 8 downgrades any auto-`APPROVE` if CI is failing.

---

## `gh pr review` invocation

### Approve

```bash
gh pr review <N> --repo <repo> --approve --body "$(cat <<'EOF'
[review body here]
EOF
)"
```

### Request changes

```bash
gh pr review <N> --repo <repo> --request-changes --body "$(cat <<'EOF'
[review body here]
EOF
)"
```

### Comment

```bash
gh pr review <N> --repo <repo> --comment --body "$(cat <<'EOF'
[review body here]
EOF
)"
```

The skill always uses **here-doc body passing** (never `--body
"$STRING"` with quotes) to avoid shell-escape mishaps with PR
content that may contain backticks, dollar signs, or quotes.

### Self-review guard

GitHub rejects `gh pr review` from the PR's own author. The
skill checks `gh pr view <N> --json author --jq .author.login`
against `gh api user --jq .login` before posting. On match:

> *PR #N is authored by `<viewer>`. GitHub doesn't allow
> self-review. Skipping.*

…and moves to the next PR.

### Inline / line-level comments — default on, maintainer picks

For every finding with a `file:line` anchor, the skill **always
proposes an inline review comment** by default. Inline
comments sit next to the offending line in the PR's "Files
changed" view, where the contributor encounters them in
context; a body-only `file.py:142` reference goes stale the
moment the line moves and forces the contributor to scroll back
and forth. The skill draws the inline comments from the same
findings list that backs the body, so nothing has to be
authored twice.

After the disposition pick (Step 6 of [`review-flow.md`](review-flow.md))
and before the final body is composed, the skill renders a
**picker** listing every drafted inline comment with an index
and a checkbox-style enabled flag:

```text
Proposed inline comments (all enabled by default):

  [x] 1. providers/foo/hook.py:142 — major
        > Imports inside function bodies should move to the top.
  [x] 2. providers/foo/hook.py:189 — minor
        > `ti.operator` could be None here; either guard
        >  explicitly or skip the metric.
  [x] 3. providers/foo/tests/test_hook.py:33 — nit
        > AGENTS.md asks for `spec=`/`autospec=` when mocking.

Pick which to post:
  [A]ll              (default — keep all inline)
  [N]one             (post body-only; findings fold into "Smaller observations")
  [<list>]           comma-separated indices to keep, e.g. `1,3`
  [<-list>]          comma-separated indices to drop, e.g. `-2,-3`
  [E <i>]            edit comment <i>'s body before posting
  [Q]uit
```

Default is `[A]ll`. Picking is one prompt — the maintainer is
not asked to confirm every comment individually, only the
subset they want. Comments the maintainer drops do not vanish:
their substance folds into the body's *Smaller observations*
block so the review still says everything it would have said,
just in fewer places.

The picker is skipped automatically when the findings list is
empty (an `APPROVE` with zero anchored findings); for
pure-body reviews the legacy `gh pr review` path runs.

Behind the scenes the skill submits a single
`addPullRequestReview` mutation carrying the picked-in
comments:

```graphql
mutation AddPullRequestReview(
  $pullRequestId: ID!,
  $event: PullRequestReviewEvent!,
  $body: String,
  $comments: [DraftPullRequestReviewComment!]!
) {
  addPullRequestReview(input: {
    pullRequestId: $pullRequestId,
    event: $event,
    body: $body,
    comments: $comments
  }) {
    pullRequestReview { id }
  }
}
```

Each `comments[]` entry carries `path`, `position` (the diff
position, not the file line), and `body`. The skill computes
diff position from the cached unified diff captured at Step 2.

#### Stale positions

Inline-comment positions are valid only against the SHA that
was diffed. If the SHA-recheck at Step 8 fires (the contributor
pushed during review), inline positions are stale and the
mutation will be rejected by GitHub. The skill surfaces the
drift:

> *PR pushed since I drafted. Inline positions stale.
> `[R]efresh` (re-run Steps 2–7 against the new SHA — usually a
> few seconds), `[B]ody-only-now` (post the existing draft as
> body-only), `[Q]uit`.*

Default is `[R]efresh`. `[B]ody-only-now` is a one-PR override;
it does not flip the default off for the rest of the session.

#### Disabling inline globally for a session

A maintainer who knows they want body-only reviews this
session can pass `inline:off` (alias `body-only`) at invocation
time. The picker is then skipped on every PR and reviews go
through `gh pr review` directly. This is rarely the right
default; the skill announces the choice once at session start
so it isn't forgotten halfway through a queue.

---

## Review body — template structure

A review body has up to four sections, in this order. Sections
with no content are omitted (don't render an empty
"Smaller observations" header).

```markdown
[summary line]

[blocking findings — if any]

[major findings — if any]

[smaller observations — minor + nit]

[ai_attribution_footer]
```

### Summary line

One sentence that names the disposition's reason. Examples:

- `APPROVE`: *"LGTM — clean N+1 fix with regression test, CI
  green."*
- `REQUEST_CHANGES`: *"Found 1 blocking issue (potential SQL
  injection in `where` clause) that needs to land before this
  can merge."*
- `COMMENT`: *"Approach looks reasonable; a few observations
  inline that I'd like resolved before merging — none
  blocking."*

The summary line is **never** boilerplate. It's the one piece
of the review body the contributor reads first; it has to
say something specific.

### Blocking findings

For each `blocking` finding:

````markdown
### Blocking — [short rule name] (`file.py:142`)

> [verbatim quote of the rule from .github/instructions/code-review.instructions.md or AGENTS.md]

```text
[5–10 lines of context from the diff, with a `# ←` arrow at the offending line]
```

[1–3 sentences explaining why this is blocking, with a
concrete suggestion. If the suggestion is small enough,
include a GitHub `suggestion` block:]

```suggestion
[the proposed replacement]
```
````

### Major findings

Same shape as blocking, header `### [short rule name]`. Drop
the "Blocking — " prefix. No `suggestion` block unless the
suggestion fits in <10 lines.

### Smaller observations

Minor + nit findings folded together as a bulleted list:

```markdown
### Smaller observations

- `file.py:89` — *narrating comment* (`# Add the item to the
  list` before `list.append(item)`). Drop the comment; the
  code already says what it does.
- `tests/test_foo.py:42` — `@pytest.fixture` is `autouse=True`
  but never `yield`-only; converting to `return` would be
  clearer (style nit, not blocking).
- `tests/test_bar.py:115` — `Mock()` without `spec`. AGENTS.md
  asks for `spec`/`autospec` when mocking.
```

Group by file when there are >5 observations on the same file.

### AI-attribution footer

Every review body ends with the verbatim block below. Do not
paraphrase, do not omit. The variant differs slightly by
disposition (the contributor-facing tone shifts from
"a maintainer will follow up with merge" on `APPROVE` to
"a maintainer will follow up after you address the points" on
the others).

#### `<ai_attribution_footer>` for `APPROVE`

```markdown
---

> *This review was drafted by an AI-assisted tool and
> confirmed by an Apache Airflow maintainer. The maintainer
> approving this PR has read the findings and signed off. If
> something feels off, please reply on the PR and a maintainer
> will follow up.*
>
> *More on how Apache Airflow handles maintainer review:*
> [contributing-docs/05_pull_requests.rst](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst).
```

#### `<ai_attribution_footer>` for `REQUEST_CHANGES`

```markdown
---

> *This review was drafted by an AI-assisted tool and
> confirmed by an Apache Airflow maintainer. After you've
> addressed the points above and pushed an update, an Apache
> Airflow maintainer — a real person — will take the next look
> at the PR. The findings cite the project's review criteria;
> if you think one of them is mis-applied, please reply on the
> PR and a maintainer will weigh in.*
>
> *More on how Apache Airflow handles maintainer review:*
> [contributing-docs/05_pull_requests.rst](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst).
```

#### `<ai_attribution_footer>` for `COMMENT`

```markdown
---

> *This review was drafted by an AI-assisted tool and
> confirmed by an Apache Airflow maintainer. The findings
> below are observations, not blockers; an Apache Airflow
> maintainer — a real person — will take the next look at the
> PR. If you think a finding is mis-applied, please reply on
> the PR and a maintainer will weigh in.*
>
> *More on how Apache Airflow handles maintainer review:*
> [contributing-docs/05_pull_requests.rst](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst).
```

---

## Adversarial-reviewer attribution

When a finding came from the adversarial reviewer, mark it
inline:

```markdown
### Blocking — Race condition on lock release (`scheduler.py:312`)

[…]

*Flagged by the adversarial reviewer; cross-checked.*
```

When two reviewers landed on the same finding:

```markdown
*Flagged by both the primary and adversarial reviewers.*
```

This makes the contributor's mental model accurate — they're
not arguing with one tool; they're arguing with two
independently-trained reviewers and a human maintainer who
agreed.

---

## Confirm-before-post

The maintainer's harness-level instructions (`AGENTS.md`,
`~/.claude/CLAUDE.md`) typically include a "confirm before
sending" rule for any message authored on their behalf. The
post step is **always** preceded by:

> *Drafted review (disposition: `<DISP>`):*
>
> ```markdown
> [full body here]
> ```
>
> *Post as-is, or want any edits?*

Wait for explicit confirmation (`yes`, `post`, `go ahead`, or
similar). If the maintainer replies with edits, **re-render
the new body and re-confirm** — earlier `yes` only covers the
exact text it approved.

---

## Per-tone overrides

If the maintainer's harness-level instructions (`AGENTS.md`,
`~/.claude/CLAUDE.md`) define **per-contributor tone overrides**
— e.g. one contributor expects a sharper register, another
gets a more measured tone — the **summary line** and body
wording for the affected PR shift accordingly. The findings
themselves don't change; the framing does.

If a tone override applies, surface it before the maintainer
confirms the body:

> *Tone override active for `<author>` per harness instructions
> (`<override-summary>`). Drafted body reflects that — please
> double-check.*

---

## `dry-run` mode

When the `dry-run` selector is in effect (see
[`selectors.md`](selectors.md)), the post step is replaced with:

> *Dry-run mode: would post `<DISP>` review to PR #N. Move on?
> `[Y]es` (default), `[E]dit`, `[S]kip`, `[Q]uit`.*

`gh pr review` is **not invoked**. The session summary lists
the would-have-been dispositions and counts.
