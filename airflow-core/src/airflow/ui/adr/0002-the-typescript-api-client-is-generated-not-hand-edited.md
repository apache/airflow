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

# 2. The TypeScript API client is generated from the OpenAPI spec, not hand-edited

Date: 2026-07-20

## Status

Accepted

## Context

The UI does not hand-write its API layer. `pnpm codegen` runs `openapi-merge-cli`
(combining the public v2 spec and the private UI spec into `openapi.merged.json`)
and then `openapi-rq`, which emits typed request functions and React Query hooks
into `openapi-gen/`. Those modules are what `src/queries/` wraps. So `openapi-gen/`
is *derived output that happens to be committed* — committed so a checkout builds
without a running API server, but overwritten wholesale by the next run. The repo
already excludes it from `pnpm format` and the formatting prek hooks, because tools
that rewrite it create diffs the generator discards.

The property this buys is *parity*: the types the UI compiles against are the types
the server publishes, checked by `tsc` at build time. A field the server dropped
becomes a compile error, not a production `undefined`. That survives only if the
pipeline is respected both ways. Editing `openapi-gen/` makes the committed client a
fork of the server's contract, silently reverted next regeneration. Reaching for
data the contract does not offer — a hand-rolled request, a client-side
reconstruction, a cast through `unknown` — hides the requirement from the API's own
reviewers; the correct sequence is change the API server, regenerate, then write the
component. Genuine client-layer defects (what `openapi-rq` gets wrong for Airflow
shapes, such as path parameters containing `/`) are fixed where generation is
configured or where the client is wrapped, so the fix survives regeneration.

## Decision

The generated client is treated as generated:

- **`openapi-gen/` is never hand-edited.** It is produced by `pnpm codegen` and
  regenerated whenever the API surface changes; the committed copy is kept in
  sync with the spec it was generated from.
- **A UI change that needs new data is an API change first.** Add or amend the
  endpoint and its datamodel on the server, regenerate the client, then build the
  UI on the regenerated types.
- **Components call the API through `openapi-gen/queries` wrapped in
  `src/queries/`**, not through bespoke `axios` calls, so every call inherits the
  generated types along with the shared auth, retry, and error behaviour.
- **Client-layer defects are fixed in the generation configuration or in the
  wrapping layer**, never as a patch to generated files.
- **Formatters and lint autofixers stay out of `openapi-gen/`**, since any change
  they make is discarded on the next run.

## Consequences

- The UI and the server contract stay in parity, enforced by `tsc` at build time
  rather than leaving shape mismatches to surface in production.
- Regenerating is cheap and always safe, because nothing of value lives only in the
  generated directory.
- Features that need new data are gated on API-server review — slower, but the
  requirement stays visible to the people who own the published contract, and the
  data reaches every client rather than only the UI.
- A diff touching `openapi-gen/` is either a regeneration matching a spec change in
  the same PR, or a mistake.

A change *violates* this decision when it:

- edits a file under `openapi-gen/` by hand — to add, retype, or loosen a field,
  or to patch generated request logic — instead of changing the spec or the
  generation configuration and regenerating;
- changes the API server's endpoints or datamodels without regenerating the
  committed client, leaving the UI compiling against a stale contract;
- hand-rolls an `axios` or `fetch` call to an endpoint the generated client
  already covers, bypassing the generated types;
- reconstructs data client-side, or casts through `unknown`/`any`, to avoid
  making the API return the field the feature actually needs;
- runs a formatter, lint autofixer, or codemod across `openapi-gen/`, producing
  churn the next regeneration discards.

A reviewer should reject any diff whose changes to `openapi-gen/` are not
reproducible by running `pnpm codegen`.

## Evidence

- #51755 — hooks must leave the generated directory alone rather than rewriting it.
- #51856 — the same boundary applied to the formatter, so generated output is not
  reformatted into conflict.
- #68667 — a real client-layer defect (path params with slashes) fixed in the
  client layer so the fix survives regeneration.
- #67725 — server-side type tightened so the generated client carried a precise
  shape, removing a UI-side cast through unknown.
- #69121, #68979 — the API-change-then-consume sequence: `has_note` added to the
  Grid Runs / TI responses rather than derived in the browser.
