<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Selectors — resolving the working PR list

The skill takes a selector string and produces a list of PR
numbers to review, in order. This file is the canonical reference
for how each selector resolves and the GraphQL / `gh` query that
backs it.

---

## Default — `review-requested-for-me`

When the maintainer invokes the skill with no selector, the
default is "open PRs where review is requested from me, the
authenticated user, individually (not via team)." This maps to
GitHub's `review-requested:<viewer>` search qualifier.

```bash
viewer=$(gh api user --jq .login)
gh search prs \
  --repo <repo> \
  --state open \
  --review-requested "$viewer" \
  --sort updated \
  --order desc \
  --limit 50 \
  --json number,title,author,labels,statusCheckRollup,reviewDecision,updatedAt,isDraft,baseRefName
```

Output is filtered post-fetch to drop drafts (drafts shouldn't
collect reviews; if the maintainer wants to review a draft they
pass `pr:<N>` explicitly).

---

## `pr:<N>` — single PR

```bash
gh pr view <N> --repo <repo> \
  --json number,title,author,authorAssociation,labels,statusCheckRollup,reviewDecision,reviewRequests,reviews,isDraft,baseRefName,body,headRefOid,changedFiles,additions,deletions
```

`pr:<N>` bypasses every other filter — including `collab:`. The
maintainer asked for this specific PR; the skill reviews it.

---

## `area:<LBL>` — filter by area label

Supports literal labels (`area:scheduler`) and wildcards
(`area:provider*`, `provider:amazon` — note that some labels use
the `provider:` prefix instead of `area:`). The wildcard match
is post-fetch (GitHub Search API doesn't expand wildcards on
labels), so the skill fetches with `--review-requested` first and
then filters the results in-memory:

```python
# pseudocode
def matches_area(pr_labels: list[str], area_pattern: str) -> bool:
    if "*" in area_pattern:
        prefix = area_pattern.rstrip("*")
        return any(lbl.startswith(prefix) for lbl in pr_labels)
    return area_pattern in pr_labels
```

Composes with the default review-requested selector. If the
maintainer wants the area filter without the review-requested
constraint, they combine `area:<LBL> ready` (see below).

---

## `collab:true` / `collab:false` — author collaborator status

Filters by the GitHub **author association** of the PR author on
this repo. The author association is in the GraphQL response as
`author { ... } authorAssociation`:

| `authorAssociation` | Meaning | `collab:true` | `collab:false` |
|---|---|---|---|
| `OWNER` | Repo owner | match | skip |
| `MEMBER` | Org member | match | skip |
| `COLLABORATOR` | Direct collaborator | match | skip |
| `CONTRIBUTOR` | Has contributed before, not a collaborator | skip | match |
| `FIRST_TIME_CONTRIBUTOR` | First contribution | skip | match |
| `NONE` | No prior association | skip | match |
| `MANNEQUIN` | Placeholder ghost user | skip | match |

The filter is applied post-fetch.

Without `collab:`, the skill includes both groups but **prints a
chip** in the per-PR headline: `[external]` for non-collab
authors, no chip for collab authors. The chip is a UI cue, not
a filter — the maintainer often wants to review external PRs
with extra care, but does not necessarily want to filter
collaborator PRs out.

---

## `team:<NAME>` — team review-request

When the maintainer wants the team queue, not just their own
direct review-requests. Resolves via GitHub's
`team-review-requested:<org>/<team>` qualifier:

```bash
gh search prs \
  --repo <repo> \
  --state open \
  --team-review-requested "<org>/<NAME>" \
  --sort updated --order desc \
  --limit 50
```

Useful for committers who have multiple team-level review
requests across `apache/airflow` (e.g. `apache/airflow-providers-amazon`,
`apache/airflow-providers-google`).

---

## `ready` — the curated triage queue

Sources from the `ready for maintainer review` label, regardless
of who is on the request list. Useful when the maintainer wants
to dip into the broader pool of PRs that triage has already
deemed ready.

```bash
gh pr list \
  --repo <repo> \
  --state open \
  --label "ready for maintainer review" \
  --json number,title,author,authorAssociation,labels,statusCheckRollup,reviewDecision,updatedAt,isDraft,baseRefName,reviewRequests \
  --limit 100
```

Often combined with `area:<LBL>` to scope. Without `area:` it's
typically too broad for a single sitting; warn the maintainer if
the result count exceeds 30.

---

## `repo:<owner>/<name>` — repo override

Replaces `<repo>` in every query above. The default is
`apache/airflow`. Other Apache-side repos (`apache/airflow-site`,
`apache/airflow-client-python`) are valid; the skill warns if
the repo lacks the `area:*` label convention (see
[`prerequisites.md#repo-override`](prerequisites.md)).

---

## `max:<N>` — cap session length

Trims the working list to the first `<N>` PRs after all other
filters apply. Useful for time-boxing ("I have an hour; show me
the top 5"). Default is unlimited (i.e. as many as the selector
returns, up to the page size of 50).

---

## `dry-run` — never post

The skill drafts every review but refuses to call `gh pr review`.
Useful for running the skill against a queue to sanity-check
findings without committing to anything.

When `dry-run` is in effect, the per-PR confirmation prompt
becomes:

> *Dry-run mode: I would post the above review with disposition
> `<disposition>`. Move on? `[Y]es`, `[E]dit`, `[S]kip`,
> `[Q]uit`.*

…and the post step is replaced with a no-op + message:

> *(dry-run: not posted)*

---

## `no-adversarial` — skip second-reviewer step

Disables the per-PR proposal to invoke the locally-configured
adversarial reviewer (e.g. Codex). The skill announces this
once at session start and does not raise it per PR.

---

## Composition rules

Selectors compose by AND. `area:scheduler collab:false max:3`
means the **first 3** PRs that are `area:scheduler` **and**
authored by a non-collaborator **and** have review requested from
the viewer (the implicit default — unless `team:` or `ready` is
also passed).

The single-PR selector `pr:<N>` does not compose — it is a
direct override.

---

## When the result is empty

Print the resolved selector, the count (0), and exit:

> *Resolved selector: `area:scheduler collab:false`,
> review-requested-for `<viewer>` on `apache/airflow`.*
> *Match count: 0. Nothing to review — exiting.*

Do not silently fall back to a wider selector.
