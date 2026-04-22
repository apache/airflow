<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Fetch

Two GraphQL shapes drive the whole skill: one for the currently-open PRs, one for the closed/merged PRs that were triaged within the cutoff window. Both are paginated with `after: <cursor>` and must follow the "one query per batch" rule from [`SKILL.md`](SKILL.md#golden-rules).

---

## Open PRs

Fetch every open PR on `<repo>` in pages of 50. This feeds Table 2 (Triaged still-open) and also supplies the denominators for the legend/context line.

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
        url
        createdAt
        isDraft
        author { login }
        authorAssociation
        labels(first: 30) { nodes { name } }
        commits(last: 1) {
          nodes {
            commit {
              oid
              committedDate
            }
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
      }
    }
  }
}
```

### `searchQuery`

```
is:pr is:open repo:<repo> sort:created-asc
```

Sort is `created-asc` (oldest PR first) so the age-bucket counts accumulate deterministically — same PR always lands in the same row in a re-run. `is:pr` filters out issues in the same search.

### `gh` invocation

```bash
gh api graphql \
  -F searchQuery="is:pr is:open repo:<repo> sort:created-asc" \
  -F batchSize=50 \
  -F cursor="$CURSOR" \
  --field query=@/tmp/pr-stats-open.graphql
```

### Batch size

50 is the default. Empirically the open-PR selection set (no rollup, no review threads) stays well under GraphQL's complexity ceiling at 50. If a rare response returns `"errors": [{"type": "MAX_NODE_LIMIT_EXCEEDED", ...}]`, drop to 25 and retry — but that's a fallback, not a default.

---

## Closed / merged triaged PRs (since cutoff)

Table 1 needs PRs that were closed or merged since the cutoff date AND had a triage comment posted at some point in their lifetime. Use GitHub's search with an `-is:open` filter plus a `closed:>=<cutoff>` date predicate, then client-side scan the `comments` subfield for the `Pull Request quality criteria` marker.

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
        url
        closedAt
        mergedAt
        state
        merged
        isDraft
        author { login }
        authorAssociation
        labels(first: 30) { nodes { name } }
        comments(last: 25) {
          nodes {
            author { login }
            authorAssociation
            createdAt
            bodyText
          }
        }
      }
    }
  }
}
```

Notice `comments(last: 25)` — higher than the 10 used for open PRs because a triaged PR that was then closed will often have extra follow-up comments; we still need to find the original triage marker. If the marker isn't in the last 25 comments for a given PR, drop that PR from Table 1 (it wasn't triaged by the bot/viewer convention).

### `searchQuery`

```
is:pr -is:open repo:<repo> closed:>=<cutoff> sort:updated-desc
```

`-is:open` matches both `closed` and `merged` states. `closed:>=` is GitHub's search qualifier for closed/merged date. `sort:updated-desc` keeps the most recent final actions at the top (so Ctrl-C'ing a long pagination returns the freshest portion).

### `gh` invocation

```bash
gh api graphql \
  -F searchQuery="is:pr -is:open repo:<repo> closed:>=2026-03-11 sort:updated-desc" \
  -F batchSize=50 \
  -F cursor="$CURSOR" \
  --field query=@/tmp/pr-stats-closed.graphql
```

### Cutoff default

If the maintainer doesn't pass `since:<date>`, default to six weeks ago:

```bash
cutoff=$(date -u -d "-42 days" +%Y-%m-%d)
```

Six weeks covers ~a sprint-and-a-half, which is long enough to smooth out day-to-day variation in closures without being so far back that the numbers lose meaning.

---

## Paginating

Both queries follow the same pattern:

```bash
cursor=null
while : ; do
  out=$(gh api graphql -F cursor="$cursor" …)
  # append out.data.search.nodes to the accumulator
  hasNext=$(echo "$out" | jq -r '.data.search.pageInfo.hasNextPage')
  cursor=$(echo  "$out" | jq -r '.data.search.pageInfo.endCursor')
  [ "$hasNext" = "true" ] || break
done
```

Two safety valves:

- Cap the accumulator at 2000 PRs per query. If the repo really has that many open PRs, the maintainer needs a narrower selector (`label:area:scheduler`, for example) — surface the cap and stop.
- If a single page returns fewer nodes than `batchSize` *and* `hasNextPage == true`, keep going — GitHub sometimes returns short pages legitimately. Never break on a short page alone.

---

## Why no `statusCheckRollup` / `mergeable` / `reviewThreads`

`pr-triage` needs all three for classification; `pr-stats` does not. Dropping them keeps the query complexity well below GitHub's per-page ceiling, which is how we can safely run `batchSize=50` here versus `20` in `pr-triage`. If a future stats column ever needs one of those fields, raise only that query's complexity — don't pull them into the default shape "just in case".

---

## Scratch cache

`/tmp/pr-stats-cache-<repo-slug>.json` (where slug is `<owner>__<name>`):

```json
{
  "viewer": {"login": "potiuk"},
  "fetched_at": "2026-04-23T10:00:00Z",
  "open_prs": { "12345": {"head_sha": "…", "classification": "triaged_waiting"} },
  "closed_since_cutoff": {
    "cutoff": "2026-03-11",
    "fetched_at": "2026-04-23T10:00:00Z",
    "prs": { "12300": {"state": "MERGED", "responded_before_close": true, "areas": ["scheduler"]} }
  }
}
```

### Invalidation

- The open-PRs block is valid while `fetched_at` is within the last 15 minutes. After that it's re-fetched (queue state changes fast — a sweep in the other window could have moved 50 PRs).
- The closed-since block is keyed by `cutoff` — if the maintainer supplies a different cutoff, fetch fresh.
- `clear-cache` on invocation drops the whole file.

### Writing discipline

Write once at the end of the full run, not after each page. A half-written cache from a Ctrl-C mid-paginate is harder to reason about than a missing cache.
