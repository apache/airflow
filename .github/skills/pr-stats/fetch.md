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
            body
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
            body
          }
        }
      }
    }
  }
}
```

Notice `comments(last: 25)` — higher than the 10 used for open PRs because a triaged PR that was then closed will often have extra follow-up comments; we still need to find the original triage marker. If the marker isn't in the last 25 comments for a given PR, drop that PR from Table 1 (it wasn't triaged by the bot/viewer convention).

### Why `body`, not `bodyText`, for the comment fetch

GitHub's GraphQL exposes two fields for a comment:

- `bodyText` — plain-text rendering; HTML comments (`<!-- … -->`) are **stripped**.
- `body` — raw Markdown as stored; HTML comments are preserved.

The now-removed `breeze pr auto-triage` command posted *staleness-close* comments with the marker embedded as an HTML comment, and those comments are still present on PRs that were triaged before the command was removed:

```markdown
This pull request has had no activity from the author for over 4 weeks.
@<author>, you are welcome to reopen this PR when you are ready to continue working on it. Thank you for your contribution!

<!-- Pull Request quality criteria -->
```

In this case the visible body contains no "Pull Request quality criteria" text at all — the only marker is the HTML comment at the bottom. Running the same marker match against `bodyText` misses these entirely. A spot-check on a 40-PR sample from `apache/airflow` found ~10% of triaged-marker comments were HTML-comment-only: invisible to a `bodyText`-based search.

**Always use `body`, not `bodyText`.** The marker detection is a simple substring search for `Pull Request quality criteria` against the raw body — it matches both:

- the visible `[Pull Request quality criteria](https://…)` link that the `pr-triage` skill (and the removed breeze violations-triage) emits, and
- the `<!-- Pull Request quality criteria -->` HTML comment that the removed breeze staleness-triage embedded as a hidden marker (still present on legacy triaged PRs).

Raw bodies are slightly noisier (Markdown formatting characters) but the marker string is distinctive enough that false positives are not a concern on `apache/airflow`.

### Known limitation — GitHub search-index lag for closed-since counts

Two GitHub-search behaviours conspire to make Table 1 hard to get right:

1. **`issueCount` for closed/merged searches is heavily under-reported.**
   Empirically on `apache/airflow` (2026-04), the GraphQL `search` query
   `is:pr is:merged repo:apache/airflow merged:>=2026-04-01` returns
   `issueCount: 37` while the REST `/pulls?state=closed` endpoint shows
   ~30 PRs merged in the last 24 hours alone. The search index appears to
   rebuild slowly (daily-ish) and results are capped in a way that
   `issueCount` doesn't reflect. **Never use the search `issueCount` as a
   denominator** — it's a biased sample.
2. **Full-text search for `"Pull Request quality criteria"` is even worse.**
   The same `apache/airflow` repo shows only 164 PRs when searched by the
   marker string, despite that exact phrase being posted on dozens of PRs
   per day by the triage skill. Free-text indexing clearly lags.

The **default Table 1 path is the hybrid REST + aliased-GraphQL fetch**, not the free-text search. The search variant is kept only as a `fast-closed` opt-in for maintainers who want a quick approximation and accept the undercount.

### Hybrid path (default)

Two stages:

1. **Enumerate** the closed / merged set since cutoff using the REST `/pulls?state=closed&sort=updated&direction=desc` endpoint. Paginate until the oldest PR on a page has `closed_at < cutoff`, then stop. Filter out bot authors (`*[bot]`, `dependabot`, `github-actions`) at this stage — bots never carry the triage marker. Budget: about one REST call per 100 PRs (~20 calls for a 6-week `apache/airflow` window yielding ~2000 closed-since PRs, ~1600 after bot filter).

2. **Fetch comments in aliased batches** of **30 PRs per GraphQL call**. Each alias queries one PR with `comments(last: 25) { nodes { author { login } authorAssociation createdAt body } }`. A single query with 30 aliases stays well inside GitHub's complexity budget; larger batch sizes occasionally hit `MAX_NODE_LIMIT_EXCEEDED`. Budget: ~54 GraphQL calls for 1600 PRs; end-to-end around 60 seconds on a warm token.

```graphql
query {
  repository(owner:"apache",name:"airflow") {
    pr63407: pullRequest(number:63407) {
      number author{login} authorAssociation closedAt mergedAt state merged
      labels(first:30){nodes{name}}
      comments(last:25){nodes{author{login} authorAssociation createdAt body}}
    }
    pr63914: pullRequest(number:63914) { … }
    # … 30 aliases per query
  }
}
```

For each returned PR, apply the same marker check as [`classify.md`](classify.md) (`Pull Request quality criteria` substring in raw `body`, author in `OWNER/MEMBER/COLLABORATOR`). Record `responded_before_close` when the author has a comment after the triage marker and on or before `closedAt`.

Empirical delta on `apache/airflow`, cutoff 2026-03-11:

| Path | Triaged+closed PRs found |
|---|---|
| Free-text search (`fast-closed`) | 28 — heavily under-reported |
| Hybrid (default) | **204** — 7.3× higher, actual count |

### `fast-closed` opt-in

When the maintainer explicitly asks for a quick approximation (`fast-closed` flag, or in a low-budget context), fall back to the search-based path:

```
is:pr -is:open repo:<repo> closed:>=<cutoff> sort:updated-desc
```

The fast path must print a clear caveat above Table 1: *"fast-closed mode: Table 1 uses the free-text search index which currently undercounts older triaged+merged PRs on `apache/airflow`. Re-run without `fast-closed` for accurate numbers."*

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

## Parsing the response — Python 3.14 strict mode

Python 3.14's `json` module raises `JSONDecodeError: Invalid control
character` on strings containing raw control characters (tabs,
newlines, `\xHH` escapes that don't map to valid JSON escapes).
GitHub's API returns comment `bodyText` fields that routinely contain
such characters, so a naive `json.load(stdin)` fails.

Use one of the following, whichever matches the pipeline:

- `json.load(fp, strict=False)` — relaxes the control-character check;
  the parser accepts raw tabs / newlines inside strings.
- `gh api ... --jq '.'` — `gh` piping through its built-in `jq` handles
  the data without going through Python's strict JSON parser.
- Save the response to a file first and re-read: `gh api ... > /tmp/x.json && python3 -c "import json; d=json.load(open('/tmp/x.json'), strict=False)"`. Avoids shell-escape interactions that can mangle the stream.

Do **not** try to clean the JSON by replacing `\n` → `\\n` before
parsing — the replacement misses genuine escape sequences elsewhere
in the payload and silently corrupts bodies. Use `strict=False`.

On REST responses that contain a PR body with an invalid JSON escape
sequence (e.g. `\z`, `\e`), even `strict=False` fails with
`Invalid \escape`. These are rare but possible — fall through to
`gh api --jq` for those calls.

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
