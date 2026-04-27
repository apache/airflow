<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Fetch and batch

Every rate-limit problem this skill is going to have, if it has
one, will come from making too many small queries. This file
documents the single batched GraphQL shape the skill uses for
every page of PRs, the prefetch plan for the *next* page, and the
session-scoped cache that prevents re-fetching across groups.

---

## The one query that matters — PR-list + rollup enrichment

On entering Step 1, issue **one** GraphQL query that pulls the
page of PRs, the rollup state for each PR's check run, the
mergeable state, the unresolved-thread count, the commits-behind
count, the latest review per reviewer, and the last handful of
comments.

### Template

```graphql
query(
  $searchQuery: String!,
  $batchSize: Int!,
  $cursor: String
) {
  search(query: $searchQuery, type: ISSUE, first: $batchSize, after: $cursor) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number
        title
        url
        createdAt
        updatedAt
        id                     # node_id — needed for mutations
        isDraft
        mergeable              # MERGEABLE / CONFLICTING / UNKNOWN
        baseRefName
        author { login }
        authorAssociation      # OWNER / MEMBER / COLLABORATOR / CONTRIBUTOR / NONE / FIRST_TIME_CONTRIBUTOR / ...
        labels(first: 30) { nodes { name } }
        commits(last: 1) {
          nodes {
            commit {
              oid
              committedDate
              statusCheckRollup {
                state          # SUCCESS / FAILURE / PENDING / ERROR
                contexts(first: 50) {
                  nodes {
                    __typename
                    ... on CheckRun     { name conclusion status }
                    ... on StatusContext { context state }
                  }
                }
              }
            }
          }
        }
        reviewThreads(first: 30) {
          totalCount
          nodes {
            isResolved
            # `first: 5` rather than `first: 1`: the
            # `mark-ready-with-ping` heuristic in
            # [`suggested-actions.md`](suggested-actions.md) needs
            # to see whether the PR author has replied in-thread
            # after the reviewer's first comment. 5 is the smallest
            # window that catches the typical "reviewer comment →
            # author reply" exchange without blowing GraphQL
            # complexity (30 threads × 5 comments × 20 PRs = 3000
            # nodes, well under the ceiling that `first: 10` here
            # would push us toward).
            comments(first: 5) {
              nodes {
                author { login }
                authorAssociation
                url
                bodyText
                createdAt
              }
            }
          }
        }
        latestReviews(first: 20) {
          nodes {
            state
            author { login }
            authorAssociation
            submittedAt
          }
        }
        comments(last: 10) {
          nodes {
            author { login }
            authorAssociation
            createdAt
            bodyText
          }
        }
        baseRef {
          target {
            ... on Commit { history(first: 1) { totalCount } }
          }
        }
      }
    }
  }
}
```

`$searchQuery` is built from the selector — see
[`#search-query-construction`](#search-query-construction)
below. The typical production shape for the default sweep on
`apache/airflow` is:

```
is:pr is:open repo:apache/airflow
-label:"ready for maintainer review"
sort:updated-asc
```

### Why this shape

Every field above is consumed by Step 2 (classify) or Step 3
(suggest action). Nothing here is speculative:

- `mergeable` → conflict detection
- `statusCheckRollup` → CI pass/fail + failing-check names for
  grace-period and "static-checks-only" logic
- `reviewThreads` (unresolved + reviewer login) → unresolved-
  thread count + ping targets
- `latestReviews` → stale `CHANGES_REQUESTED` detection and
  `has_collaborator_review` flag (extended grace period)
- `comments(last: 10)` → "already triaged" detection (viewer's
  prior triage comment), "author responded after triage"
  detection, and the active-maintainer-conversation pre-filter
  (recent collaborator comment + maintainer-to-maintainer
  `@`-ping detection — see
  [`classify.md#5-active-maintainer-conversation-on-the-pr`](classify.md))
  — which is why the comment node carries `authorAssociation`
  and `bodyText` in addition to author login
- `baseRef.target.history.totalCount` → commits-behind anchor
  (combined with PR head commit count; see
  [`#commits-behind`](#commits-behind))

### Batch size

Default `$batchSize = 20`. Empirically, requesting 50 aliased
`PullRequest` objects — each expanding `contexts(first: 30)` +
`reviewThreads(first: 10)` + `latestReviews(first: 5)` +
`comments(last: 5)` — trips GitHub's GraphQL complexity ceiling
on `apache/airflow` and returns a generic error page ("Unicorn")
instead of JSON. 20 reliably comes back with `cost=3` against
the rate-limit budget. The inner `first:` arguments are the
dominant factor; if you need to widen them, *lower* the outer
batch size first — never raise above 25 without measuring.

### `gh` invocation

```bash
gh api graphql -F searchQuery="$SEARCH" -F batchSize=20 -F cursor="$CURSOR" \
  --field query=@fetch-prs.graphql
```

Use `-F` for integers/strings and `--field query=@file` (read
from a file) for the literal query. The `@file` form is less
fragile than `-f query="$(cat file)"` when the query contains
`$variable` references — the shell would otherwise try to
expand them. Capture the JSON output and parse it with `jq` or
`python3 -c 'import json; ...'` rather than re-querying.

---

## Search-query construction

Translate the selector into the GitHub search query:

| Selector | Query fragment |
|---|---|
| default | `is:pr is:open repo:<repo> sort:updated-asc` |
| `pr:<N>` | `repo:<repo> <N>` *(no need for `is:pr` — the number is unique)* — but prefer the non-search `pullRequest(number: N)` field, it's cheaper |
| `label:<LBL>` (exact) | `label:"<LBL>"` |
| `label:<PAT*>` (wildcard) | omit from search; filter client-side against the returned `labels.nodes.name` list |
| `-label:<LBL>` | `-label:"<LBL>"` |
| `author:<LOGIN>` | `author:<LOGIN>` |
| `review-for-me` | `review-requested:@me` (or `user-review-requested:@me`) |
| `created:>=YYYY-MM-DD` | `created:>=YYYY-MM-DD` |
| `updated:>=YYYY-MM-DD` | `updated:>=YYYY-MM-DD` |

GitHub caps `sort:` at the search level and respects
`updated-asc` (oldest-updated first), which is what you want:
stale PRs surface first, hot-changing PRs hit GitHub's own
cache anyway and surface on a later run.

Always include `is:pr is:open` and the target `repo:<repo>`
explicitly. Never rely on implicit repo context — the skill must
behave identically when invoked from outside a checkout.

For a single-PR invocation (`pr:<N>`), skip `search` entirely
and use the direct `repository(owner, name) { pullRequest(number: N) { ... } }`
query with the same selection set — it's one GraphQL call
regardless and doesn't burn a search-API budget slot (search is
rate-limited more tightly: 30 per minute).

---

## Commits-behind

GitHub's GraphQL doesn't expose "commits behind" directly. The
cheapest approximation is `compareCommits` via the REST API,
but that's one call per PR. Instead, fetch the commits-behind
for the entire page in **one** aliased GraphQL call:

```graphql
query($owner: String!, $repo: String!) {
  repository(owner: $owner, name: $repo) {
    pr123: pullRequest(number: 123) {
      baseRef { target { ... on Commit { oid history(first: 1) { totalCount } } } }
      headRef { target { ... on Commit { oid history(first: 1) { totalCount } } } }
      # ... repeated for every PR on the page
    }
  }
}
```

This returns totals for the base and head branches — subtract
head-merge-base total from base total to get commits-behind.
Group these aliases into chunks of **20 PRs per aliased query**
(GitHub's alias-per-query cap is around 50, but 20 keeps you
well clear of the complexity budget).

Stash the result on each PR record and use it in both grace-
period logic and comment rendering (e.g. "your branch is 73
commits behind `main`"). Do not call `gh pr view` per PR — that
is the anti-pattern this file exists to prevent.

---

## Mandatory: `action_required` run index per page

Before classification runs, fetch one REST call per page:

```bash
gh api "repos/<owner>/<repo>/actions/runs?event=pull_request&status=action_required&per_page=100"
```

This lists **every** workflow run across the repo that is
awaiting maintainer approval. Index the response by `head_sha`;
any PR on the current page whose head SHA appears in the index
is `pending_workflow_approval` (see
[`classify.md#c1-pending_workflow_approval`](classify.md)).

Why this is mandatory, not "fallback":

- `statusCheckRollup.state` aggregates only **completed**
  check-runs. When a first-time-contributor PR has its real CI
  held in `action_required`, the rollup reports SUCCESS based on
  fast bot checks (`Mergeable`, `WIP`, `DCO`, `boring-cyborg`)
  that run unconditionally.
- Empirically on `apache/airflow`, 17 first-time-contributor
  PRs in a single sweep reported `rollup.state == SUCCESS` while
  every real CI workflow was in `action_required`. Trusting the
  rollup classified them all as `passing`.
- The REST call returns ≤ 100 runs at a time and paginates
  cheaply — a single extra round-trip per page is well under the
  rate-limit budget and closes the whole class of false-
  positives.

Walk all pages of `actions/runs` (or at least the first 3, which
covers any reasonable repo-level backlog) and keep the union as
a per-page index. Invalidate the index before fetching the next
PR page — approval state changes fast.

The REST call is the primary signal. The rollup + "real CI
pattern" guard from
[`classify.md#verifying-real-ci-ran`](classify.md) is the
belt-and-braces second check that protects against a rare case
where the REST call misses a freshly-created run.

---

## Optional: failed-job log snippets (deferred)

When a PR is pulled out of a `draft` group for individual
review, and only then, fetch short log snippets from the failed
jobs to help the maintainer decide:

```bash
gh api repos/<owner>/<repo>/commits/<head_sha>/check-runs?status=failed
# pick the failed run IDs, then:
gh api repos/<owner>/<repo>/actions/jobs/<job_id>/logs   # plain text
```

Cap the snippet at 30 lines per job and 5 jobs per PR. This is
the only per-PR call the skill makes in the non-batch path, and
it's gated on "the maintainer is actually looking at this one".
Cache the snippet keyed by `(pr_number, head_sha)` in the
session cache.

---

## Prefetch plan

The interaction loop (see [`interaction-loop.md`](interaction-loop.md))
presents one group of PRs at a time. *While* a group is on
screen — i.e. inside the same tool-call turn as the
presentation — fire the next enrichment call in parallel so the
next group is already warm by the time the maintainer decides.

Concretely, two parallel GraphQL calls per interaction turn:

| Call A (current turn's result display) | Call B (prefetched) |
|---|---|
| PR-list + rollup query for **current** page | PR-list + rollup query for **next** page (using `endCursor` from page 1) |
| *or* log-snippet fetch for the current `draft` group | *or* diff preview fetch for the next `approve-workflow` group |

Parallelism is a must, not an option — serialising the prefetch
behind the maintainer's decision doubles end-to-end latency for
every group. Use the tool harness's parallel tool call feature
(issue two Bash tool calls in the same response).

If the maintainer is likely to quit (this is the last group),
skip the prefetch — it's wasted budget. Heuristic: if
`has_next_page` is false and there's no larger pending work,
don't prefetch.

---

## Session cache

`/tmp/pr-triage-cache-<repo-slug>.json` stores intermediate
results so that re-invocations inside a working session skip
anything that isn't needed. Schema:

```json
{
  "viewer": {"login": "potiuk", "permission": "MAINTAIN"},
  "label_ids": {
    "ready for maintainer review": "LA_kwDO..==",
    "closed because of multiple quality violations": "LA_kwDO..==",
    "suspicious changes detected": "LA_kwDO..=="
  },
  "prs": {
    "12345": {
      "head_sha": "abc123...",
      "classification": "deterministic_flag",
      "suggested_action": "rebase",
      "action_taken": "rebase",
      "action_at": "2026-04-22T09:17:03Z"
    }
  },
  "recent_main_failures": {
    "fetched_at": "2026-04-22T08:00:00Z",
    "failing_check_names": ["Helm tests (1.29)", "..."]
  }
}
```

### Invalidation

- An entry's `head_sha` must match the head SHA returned by the
  current fetch — if it doesn't, the contributor pushed since
  and the entry is stale. Drop it and re-classify.
- The `recent_main_failures` block is valid for 4 hours; after
  that, re-fetch via the canary/main-branch failure query
  (see below).
- The whole cache is discardable — losing it only costs one
  extra enrichment round.

### Writing discipline

Write the cache once per group completion, not once per PR.
Writing on every single mutation creates burst disk churn and
— more importantly — makes it easy to leave the cache in an
inconsistent state if the session is interrupted mid-group.
On `Ctrl-C`, flush once on the way out.

---

## Recent main-branch failures (for "is this failure systemic?")

The `suggested_action` computation needs to know which checks are
currently failing across main-branch PRs, so that a PR whose
only failures match the main-branch failures gets suggested for
`rerun` rather than `draft`. Fetch this **once** per session
(cache for 4 hours):

```graphql
query($owner: String!, $repo: String!) {
  repository(owner: $owner, name: $repo) {
    pullRequests(states: MERGED, orderBy: {field: UPDATED_AT, direction: DESC}, first: 10) {
      nodes {
        number
        mergedAt
        commits(last: 1) {
          nodes {
            commit {
              oid
              statusCheckRollup {
                contexts(first: 50) {
                  nodes {
                    __typename
                    ... on CheckRun { name conclusion }
                    ... on StatusContext { context state }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

Collect the set of `CheckRun.name` values where `conclusion` is
`FAILURE` or `TIMED_OUT` across those 10 recently-merged PRs.
Any failure appearing in ≥2 of them is "systemic". Store the
resulting set in the session cache as `recent_main_failures`.

---

## What not to do

- **Do not call `gh pr view <N>`** in a loop. Each invocation is
  a separate API call; 50 of them = 50 round-trips and 50
  rate-limit points. Use the aliased batch query instead.
- **Do not issue a GraphQL query inside a Python generator** or
  other lazy structure that might hide the call count. All PR
  fetches happen in a small set of named calls — count them and
  keep the count down.
- **Do not prefetch the *next-next* page** just because you can.
  One page ahead is the right depth; two is wasted budget for
  a session the maintainer will usually end within 1–3 pages.
- **Do not sleep after rate-limit errors.** If a query 403s on
  `X-RateLimit-Remaining: 0`, stop immediately, surface the
  budget info to the maintainer, and let them decide whether to
  pause or retry. Sleeping and retrying in-skill just masks the
  root cause (you are almost certainly mis-batching).
