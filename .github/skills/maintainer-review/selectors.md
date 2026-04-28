<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Selectors — resolving the working PR list

The skill takes a selector string and produces a list of PR
numbers to review, in order. This file is the canonical reference
for how each selector resolves and the GraphQL / `gh` query that
backs it.

---

## Default — "my reviews"

When the maintainer invokes the skill with no selector, the
default working list — referred to throughout the docs as
**"my reviews"** — is the **union** of five signals on
`<viewer>` (the authenticated maintainer):

1. **Review-requested** — open PRs where review is requested
   from `<viewer>`, individually (not via team). Maps to
   GitHub's `review-requested:<viewer>` search qualifier.
2. **Touching files I've recently modified** — open PRs that
   change at least one file in the maintainer's "active set":
   files from `<viewer>`'s open PRs and files `<viewer>` has
   authored commits to on the base branch in the past
   `<since>` (default `30d`). See
   [`touching-mine`](#touching-mine--prs-that-touch-files-ive-been-working-on)
   below for the active-set computation.
3. **CODEOWNER of modified files** — open PRs that change at
   least one file the repo's
   [`CODEOWNERS`](https://github.com/apache/airflow/blob/main/.github/CODEOWNERS)
   assigns to `<viewer>` (directly, or via a team `<viewer>`
   belongs to). See
   [`codeowner`](#codeowner--prs-that-touch-files-i-own) below.
4. **Mentioned by author or another maintainer** — open PRs
   where `<viewer>`'s `@login` appears in the PR body, a
   PR-conversation comment, an inline review comment, or a
   commit message. See
   [`mentioned`](#mentioned--prs-that--name-me) below.
5. **Previously reviewed by me (real reviews only)** — open
   PRs that already have a `gh pr review`-submitted review
   from `<viewer>` (state `APPROVED`, `CHANGES_REQUESTED`, or
   `COMMENTED`). **Triage comments do not count** — the
   `pr-triage` skill's PR-conversation comments live in
   `comments[]`, never in `reviews[]`, so they are excluded
   automatically. See
   [`reviewed-before`](#reviewed-before--prs-i-already-reviewed)
   below.

The union is computed post-fetch — all five signals run,
results are deduplicated by PR number, and the union is sorted
by most-recently-updated. Each PR in the working list carries
**match-reason chip(s)** so the maintainer sees *why* it
landed there: any of `[review-requested]`,
`[touches: <path>]`, `[codeowner: <path>]`,
`[mentioned-in: <where>]`, `[reviewed-before: <when>]`, or
several chips on the same line when multiple signals fire on
one PR — there is no special collapsing. Chips are rendered
in the per-PR headline at Step 1 of
[`review-flow.md`](review-flow.md).

The review-requested half:

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

Each of the other four halves is documented in its own section
below.

Output is filtered post-fetch to drop drafts (drafts shouldn't
collect reviews; if the maintainer wants to review a draft
they pass `pr:<N>` explicitly).

If the maintainer wants only some of the halves, pass any of
`requested-only`, `mine-only`, `codeowner-only`,
`mentioned-only`, `reviewed-before-only` (each of these drops
the four others). Or compose negatives:
`no-touching-mine no-mentioned` keeps the rest of the union but
suppresses those two signals.

---

## `touching-mine` — PRs that touch files I've been working on

A PR can be highly relevant to the maintainer even when GitHub
hasn't requested their review on it: a contributor opened a PR
that edits the same file the maintainer just refactored, or
that conflicts with an open branch the maintainer hasn't pushed
yet. This signal surfaces those PRs.

### Defining "files I've been working on"

The skill builds an **active set** of file paths from two
sources, computed once at session start and cached:

1. **Open PRs by `<viewer>`** — every file path touched by every
   open PR the viewer has authored on `<repo>`.
2. **Recent commits on the base branch** — every file path the
   viewer has authored a commit to on `upstream/<base>` in the
   past `<since>` (default `30d`).

The active-set computation:

```bash
viewer=$(gh api user --jq .login)
since="${SINCE:-30 days ago}"   # default; overridable via since:<window>

# 1) Files in open PRs authored by viewer:
viewer_open_prs=$(gh pr list --repo <repo> --author "$viewer" \
  --state open --json number --jq '.[].number')

mine_via_open_prs=$(for n in $viewer_open_prs; do
  gh pr view "$n" --repo <repo> --json files --jq '.files[].path'
done | sort -u)

# 2) Files in recent main-branch commits authored by viewer:
mine_via_main=$(git log \
  --author="$viewer" \
  --since="$since" \
  --pretty=tformat: \
  --name-only \
  upstream/<base> -- | sort -u | grep -v '^$')

active_set=$(printf '%s\n%s\n' "$mine_via_open_prs" "$mine_via_main" \
  | sort -u | grep -v '^$')
```

`<base>` defaults to `main`. The `git log --author="$viewer"`
match is by name *or* email — git's matcher is permissive, so
any maintainer whose `git config user.email` matches their
GitHub-side email will be picked up. If the active set comes
back empty, announce it once at session start (so the
maintainer knows the touching-mine half contributed nothing) and
fall back to review-requested only.

### Matching open PRs against the active set

Open PRs (excluding drafts and PRs authored by `<viewer>` —
self-review is rejected by GitHub anyway) are scanned for any
file in the active set. The scan uses GraphQL aliased queries
to fetch changed-file paths for many PRs in one call:

```graphql
query OpenPRFiles($repo_owner: String!, $repo_name: String!, $cursor: String) {
  repository(owner: $repo_owner, name: $repo_name) {
    pullRequests(states: OPEN, first: 50, after: $cursor,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        author { login }
        isDraft
        files(first: 100) {
          nodes { path }
        }
      }
    }
  }
}
```

Pagination stops when either:

- `hasNextPage` is false, or
- the page's most-recently-updated PR is older than the
  active-set's `<since>` window (older PRs that touch active
  files are usually stale and not worth surfacing).

For each PR, intersect `files[].path` with the active set; if
non-empty, add the PR to the working list with the **first
match path** as the chip text (e.g. `[touches: airflow-core/src/airflow/jobs/scheduler_job_runner.py]`).
For >1 match, the chip says `[touches: <first-path> +N more]`.

### Tuning

| Selector | Effect |
|---|---|
| `since:<window>` | Set the recency window for the main-branch source. Examples: `since:7d`, `since:2w`, `since:90d`. Default `30d`. |
| `mine-only` | Use the touching-mine signal alone (drops every other half of the default union). |
| `requested-only` | Use only the review-requested signal (drops every other half). |
| `no-touching-mine` | Drop just the touching-mine half; keep the rest of the union. |

### Compose with `area:`, `collab:`, `max:`

`touching-mine` is union-with-default, so it composes with the
post-fetch filters (`area:`, `collab:`) the same way: the
filters apply to the union, not to each half independently.

`area:scheduler` filters out any PR — review-requested or
touching-mine — that doesn't carry `area:scheduler`. If the
maintainer wants area to *exclude* their touching-mine signal
(e.g. they want only review-requested PRs in the scheduler
area, not every PR touching their files), they pass
`area:scheduler requested-only`.

---

## `codeowner` — PRs that touch files I own

Independent of whether the maintainer has been *editing* a
file recently, GitHub's `CODEOWNERS` declares which
maintainer (or maintainer-team) is responsible for which
paths. Even a maintainer who hasn't touched a file in months
should see PRs that mutate code they own — that is what the
ownership signal is for.

### Computing ownership

The repo's `.github/CODEOWNERS` (or `CODEOWNERS` at the repo
root) maps glob patterns to one or more `@user` /
`@org/team` entries. The skill parses it once at session
start and resolves `<viewer>`'s ownership set:

1. **Direct ownership** — patterns whose owners list contains
   `@<viewer>`.
2. **Team ownership** — patterns whose owners list contains
   `@<org>/<team>` for any team `<viewer>` is a member of.
   Team membership is fetched via `gh api
   orgs/<org>/members/<viewer>` per team listed in the file
   (cheap and cached for the session).

The output is a **patterns-owned-by-viewer** list. For each
candidate PR, the skill matches the PR's `files[].path`
against those patterns; on any match the PR joins the working
list with the chip `[codeowner: <first-matched-path>]`.

`CODEOWNERS` later entries override earlier ones for the
same path, matching GitHub's own resolution rule. The skill's
matcher mirrors that — the *last* matching pattern wins, so a
later `*` line that doesn't name `<viewer>` correctly removes
ownership.

### When `CODEOWNERS` is missing

If the repo has no `CODEOWNERS` file, the skill announces
once:

> *No `CODEOWNERS` in `<repo>`. The codeowner signal is
> contributing nothing this session.*

…and proceeds with the rest of the union.

### Tuning

| Selector | Effect |
|---|---|
| `codeowner-only` | Use only this signal (drops every other half). |
| `no-codeowner` | Drop just the codeowner half; keep the rest. |

---

## `mentioned` — PRs that @-name me

Some PRs land on a maintainer's plate because they're
explicitly called out in the PR conversation rather than via
review-request or ownership: an author writing *"@viewer can
you sanity-check the migration logic?"* in the PR body, or
another maintainer asking *"@viewer this is your code path —
agree?"* in a thread.

### What "mentioned" means here

`<viewer>` is considered **mentioned on a PR** if any of these
text bodies on the live PR contain the literal `@<viewer>`
(case-insensitive, word-bounded):

- the PR body,
- any **PR-conversation** comment (the `comments` connection),
- any **review** body or **inline review comment** body (the
  `reviews` connection),
- any **commit message** in the PR's commit list.

The match is on the literal `@<viewer>` token, not arbitrary
substring — so `@viewer-bot` or `email@viewer.com` does not
trigger.

### Query

```graphql
query MentionedOnPR(
  $repo_owner: String!, $repo_name: String!
) {
  repository(owner: $repo_owner, name: $repo_name) {
    pullRequests(states: OPEN, first: 50,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number title author { login } isDraft updatedAt
        bodyText
        comments(first: 50) { nodes { bodyText } }
        reviews(first: 50)  { nodes { bodyText } }
        commits(first: 50)  { nodes { commit { message } } }
      }
    }
  }
}
```

The skill scans `bodyText / comments[].bodyText /
reviews[].bodyText / commits[].commit.message` for the
`@<viewer>` token; any hit adds the PR with chip
`[mentioned-in: body|comment|review|commit]` (the first
matching location wins for the chip text).

### Tuning

| Selector | Effect |
|---|---|
| `mentioned-only` | Use only this signal (drops every other half). |
| `no-mentioned` | Drop just the mentioned half; keep the rest. |

---

## `reviewed-before` — PRs I already reviewed

If `<viewer>` already submitted a review on a PR (via
`gh pr review`, regardless of `APPROVE` /
`CHANGES_REQUESTED` / `COMMENT` outcome), the contributor has
likely pushed updates and the maintainer is the natural
person to take a follow-up look. This signal surfaces those
PRs so the maintainer doesn't lose track of their own
in-flight reviews.

### What counts as "reviewed" — and what does not

A PR is **reviewed-before by `<viewer>`** if its `reviews[]`
array contains any entry with `author.login == <viewer>`,
regardless of `state`.

**Triage comments do NOT count.** The `pr-triage` skill posts
its notes via `gh pr comment`, which lands in the PR's
`comments` array (the GitHub *issue-comment* endpoint). Those
never appear in `reviews`. So the reviewed-before filter
cleanly distinguishes *"I actually reviewed the code on this
PR"* from *"I tagged it during morning triage"* — only the
former counts.

### Query

```graphql
query ReviewedBefore(
  $repo_owner: String!, $repo_name: String!, $viewer: String!
) {
  repository(owner: $repo_owner, name: $repo_name) {
    pullRequests(states: OPEN, first: 50,
                 orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number isDraft updatedAt headRefOid
        reviews(first: 50, author: $viewer) {
          nodes { state submittedAt commit { oid } }
        }
      }
    }
  }
}
```

For PRs with a non-empty `reviews[]` from `<viewer>`, the
chip is `[reviewed-before: <relative-time>]` (e.g.
`[reviewed-before: 4 days ago]`), pulled from the latest
`submittedAt`.

### Auto-skip already-handled re-reviews

If `<viewer>`'s most recent `state == APPROVED` was submitted
against the PR's current head SHA (no new commits since), the
re-review is redundant — there is nothing new to read. The
skill auto-skips with reason `prior-approval-current-sha`
(see
[`review-flow.md` § Edge cases](review-flow.md#viewer-already-approved-in-a-prior-session)
for the SHA-comparison logic).

### Tuning

| Selector | Effect |
|---|---|
| `reviewed-before-only` | Use only this signal (drops every other half). |
| `no-reviewed-before` | Drop just this half; keep the rest. |

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

## `with-reviewer:<command>` — name an adversarial reviewer

Names the slash command the skill should propose at Step 5 of
[`review-flow.md`](review-flow.md) for second-read coverage.
The skill never fires the command itself — it asks the
maintainer to type it. See
[`adversarial.md`](adversarial.md) for the full mechanics and
why the assistant proposes but does not invoke.

Example:

```text
/maintainer-review with-reviewer:/some-plugin:adversarial-review
```

If `with-reviewer:` is not passed, the skill checks the
maintainer's agent-instructions file (project-scope
`AGENTS.md`, harness-specific `CLAUDE.md`) for a "Review
preferences" entry naming a default reviewer — see
[`prerequisites.md#2`](prerequisites.md). If none is
configured, Step 5 is announced as a no-op and skipped.

---

## `no-adversarial` — skip second-reviewer step

Disables the per-PR proposal to invoke the configured
adversarial reviewer for this session. The skill announces
this once at session start and does not raise it per PR.
Useful when the maintainer wants speed and is comfortable with
single-reviewer coverage on a known-low-risk batch.

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
