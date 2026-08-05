<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Plan: make the Airflow UI respect the log continuation token

**Status:** design / not yet implemented
**Context:** follow-up to the streaming task-log series (`#69299`, `#69300`, and the
"Skip log accumulator disk spill when reading finished task logs" commit on this branch).

## Problem

The React UI ignores the log continuation token entirely. `useLogs`
(`airflow-core/src/airflow/ui/src/queries/useLogs.tsx`) calls
`useTaskInstanceServiceGetLog` with only
`{accept, dagId, dagRunId, mapIndex, taskId, tryNumber}` — **no `token`** — and on
every `refetchInterval` tick React Query discards the previous data and re-fetches the
**entire log from line 0**. It reads only `data.content`; the returned
`continuation_token` (JSON body) / `Airflow-Continuation-Token` (NDJSON header) is never
consumed. Client-side `truncateData` slices to the last `limit` lines only *after*
pulling everything.

Cost model today: for a task that runs across N polls while its log grows to L lines, the
server does **O(L × N)** work — read + `_interleave_logs` merge + serialize + ship the
whole log every tick — and the browser re-parses the whole log every tick.

## What already exists (no backend change needed)

- Endpoint `GET .../logs/{try_number}` (`api_fastapi/core_api/routes/public/log.py`)
  already accepts `token: str | None`, decodes it back into `metadata` (carrying
  `log_pos`), and returns the next token **only while the log is unfinished**:
  - `application/json` → `continuation_token` in the body.
  - `application/x-ndjson` (current UI default) → `Airflow-Continuation-Token` **response
    header**.
- Generated client `GetLogData` already has `token?: string | null`, and `getLog`
  forwards it.

So the entire change is on the React side: capture the returned token, send it back, and
**append** deltas instead of **replacing** the whole buffer.

## Design

### A. Where to read the token

- **Recommended: switch the UI request to `accept: "application/json"`** so the token
  arrives in the body (`continuation_token`) — the generated client already deserializes
  the body, so no header plumbing is required.
- Keeping NDJSON streaming would require extending the generated request layer to surface
  the `Airflow-Continuation-Token` **response header**, which the client currently
  discards. More work; defer.

### B. Accumulate instead of replace

Convert `useLogs` from a replace-query to an accumulate-query (a `useInfiniteQuery`, or a
manual reducer):

- `queryFn` calls `getLog({ ..., token: pageParam })`.
- `getNextPageParam` returns the response `continuation_token`; when it is `null`/absent
  (`end_of_log`), return `undefined` so React Query stops paging.
- Concatenate `content` across pages into one buffer and feed **that** buffer to
  `parseLogs`, `truncateData`, search, and download — all already operate on a flat
  content array.
- Drive polling with `refetchInterval` only while `hasNextPage && isStatePending(state)`,
  replacing the current "refetch everything while pending" behavior.

### C. Reset on try-number / restart

Reset the accumulated buffer + token when `tryNumber` changes or the TI transitions to a
fresh run, so stale lines are not appended. Key the infinite query on `tryNumber`.

### D. Keep "download full log" honest

Download currently relies on `fetchedData` being the whole log. With incremental fetch,
either reuse the accumulated buffer, or for download specifically issue a one-shot
`fullContent: true` request (the endpoint already supports `full_content`).

No backend change required — this is a `useLogs.tsx` refactor plus (optionally, only for
the NDJSON path) response-header exposure in the request layer.

## Does it reduce API-server pressure?

**Yes — per-request cost during the running-task window drops from O(L) to O(Δ)** (only
new lines since `log_pos`); total server work over the task life becomes **O(L) instead of
O(L × N)**. Concretely it cuts:

- **Response payload / egress** — biggest, clearest win; only the delta ships each tick.
- **CPU** — JSON/structlog serialization and the `_interleave_logs` merge scale with lines
  emitted, so they shrink to the delta.
- **Browser cost** — only new lines are parsed/rendered each tick instead of the whole log.

### Caveats (be realistic)

1. **Request count is unchanged** — still one poll per interval per open log view. Token
   use lowers per-request cost, not the number of requests. Cutting request count needs
   server-push (SSE/streaming), a separate effort.
2. **Savings apply only while the task runs.** Once finished the UI already stops polling,
   and the accumulator-bypass commit on this branch already streams terminal reads without
   the accumulator.
3. **Log-backend reads may shrink less than egress.** For remote object stores that cannot
   seek, the handler still pulls the object and `islice`s past `log_pos`, so backend I/O
   improves less than payload size; local files fare better.
4. **Shifts running-task reads onto the accumulator path** (which computes `log_pos`) —
   inherent to producing a token, bounded by `HEAP_DUMP_SIZE`. This is the opposite
   optimization from the finished-task fast path in the last commit.

**Net:** a solid efficiency win (egress + CPU) for deployments with many users watching
long-running tasks with large logs. It is *not* a remedy for "too many concurrent
pollers" — real streaming is the lever there; token-based incremental fetch is the lever
for eliminating wasted re-transfer.
