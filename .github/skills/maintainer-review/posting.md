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

### Inline / line-level comments

`gh pr review` does not accept line-level comments via flags.
For findings the maintainer wants posted as **line-anchored**
comments (rather than only in the review body), the skill uses
the GraphQL `addPullRequestReview` mutation with a `comments`
array. The mutation is heavier than `gh pr review`, so the
skill only invokes it when the maintainer explicitly opts in
during Step 7 (Compose review body):

> *Post the file:line findings as inline comments anchored to
> the diff? `[I]nline`, `[B]ody-only` (default), `[E]dit`,
> `[Q]uit`.*

`[B]ody-only` is the default because line-anchored comments
require the diff position to still be valid; if the
contributor pushes a fixup right before posting, the line
positions can drift and GitHub rejects the mutation.
Body-only references (`file.py:142`) survive.

When `[I]nline` is chosen, the mutation:

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

…with `comments` populated from the findings list, each entry
carrying `path`, `position` (the diff position, not the file
line), and `body`. The skill computes diff position from the
cached unified diff captured at Step 2.

If the SHA-recheck at Step 8 fires (the contributor pushed
during review), the diff positions are stale; the skill warns
and falls back to `[B]ody-only` automatically:

> *PR pushed since I drafted. Inline positions stale; falling
> back to body-only references. Re-fetch and retry inline?
> `[R]efresh`, `[B]ody-only-now`, `[Q]uit`.*

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

When a finding came from the adversarial reviewer (Codex),
mark it inline:

```markdown
### Blocking — Race condition on lock release (`scheduler.py:312`)

[…]

*Flagged by Codex; cross-checked.*
```

When two reviewers landed on the same finding:

```markdown
*Flagged by both Claude and Codex.*
```

This makes the contributor's mental model accurate — they're
not arguing with one tool; they're arguing with two
independently-trained reviewers and a human maintainer who
agreed.

---

## Confirm-before-post

Per the user's personal `~/.claude/CLAUDE.md` rule on
confirmation before sending messages on the maintainer's
behalf, the post step is **always** preceded by:

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

The user's personal `~/.claude/CLAUDE.md` defines tone overrides
for specific contributors (e.g. `Sebb` → polite-but-firm,
`Jim Jagielski` → sharper edge with light humour). When the PR
author or commenter matches one of those names, the **summary
line** and the body wording shift accordingly. The findings
themselves don't change; the framing does.

If a tone override applies, surface it before the maintainer
confirms the body:

> *Tone override active for `<author>` per personal CLAUDE.md
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
