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

The UI does not hand-write its API layer. The `codegen` script in `package.json`
runs `openapi-merge-cli` — which combines the API server's public v2 spec and its
private UI spec into `openapi.merged.json` according to `openapi-merge.json` —
and then `openapi-rq`, which emits typed request functions and React Query hooks
into `openapi-gen/` (`openapi-gen/requests/` and `openapi-gen/queries/`). Those
generated modules are what `src/queries/` wraps and what every component
ultimately calls.

This makes `openapi-gen/` *derived output that happens to be committed*. It is
committed so that a checkout builds without a running API server, but it is not
source: the next `pnpm codegen` overwrites it wholesale. The repository already
treats it accordingly — the directory is excluded from `pnpm format` and from the
formatting prek hooks, precisely because tools that rewrite it create diffs the
generator will discard and merge conflicts nobody can resolve meaningfully.

The important property this buys is *parity*. The types the UI compiles against
are the types the server publishes, checked by `tsc` at build time. A field the
server stopped returning becomes a compile error rather than an `undefined` in
production; a field the server added is available to the UI without anyone
transcribing its shape by hand. Strict TypeScript turns the server's contract
into a mechanically enforced one.

That property survives only if the pipeline is respected in both directions, and
there are two ways to break it:

- **Editing the generated output.** A local patch to `openapi-gen/` — to add a
  field, loosen a type, or fix a serialization quirk — appears to work and is
  silently reverted by the next regeneration. Worse, it makes the committed
  client a *fork* of the server's contract, so the compiler stops being a check
  and starts confirming a fiction.
- **Reaching for data the contract does not offer.** When a UI feature needs
  information the API does not return, the tempting shortcuts are a hand-rolled
  request, a client-side reconstruction from adjacent endpoints, or a cast
  through `unknown`. Each of these makes the UI's model of Airflow diverge from
  every other consumer's, and each hides the requirement from the API's own
  reviewers. The correct sequence is: change the API server, regenerate, then
  write the component.

Genuine client-layer defects — the parts `openapi-rq` gets wrong for Airflow's
key shapes, such as path parameters containing `/` — do exist. They are fixed
where the generation is configured or where the client is wrapped, so the fix
survives regeneration.

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

- The UI and the server contract stay in parity, and `tsc` enforces that parity
  at build time rather than leaving shape mismatches to surface in production.
- Regenerating is cheap and always safe, because nothing of value lives only in
  the generated directory.
- UI features that need new data are gated on API-server review — slower, but it
  keeps the requirement visible to the people who own the published contract, and
  the resulting data is available to every client rather than only to the UI.
- Reviewers can trust that a diff touching `openapi-gen/` is either a
  regeneration matching a spec change in the same PR, or a mistake.

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

- #51755 — "Do not modify openapi-gen generated files by pre-commits": established
  that hooks must leave the generated directory alone rather than rewriting it.
- #51856 — "Add codegen files to prettier ignore and from `pnpm format`": the same
  boundary applied to the formatter, so generated output is not reformatted into
  conflict.
- #68667 — "Percent-encode API client path params for keys with slashes": a real
  client-layer defect fixed in the client layer so the fix survives regeneration.
- #67725 — "Type `asset_expression` in the REST API so the UI does not cast
  through unknown": the server-side type was tightened so the generated client
  carried a precise shape, removing a UI-side cast.
- #69121 — "Add `has_note` key to the Grid Runs API response": the API-change-then
  -consume sequence — the field was added to the endpoint so the Grid could render
  it, rather than derived in the browser.
- #68979 — "Add `has_note` key to the API response for TI to render saved note
  indicator in Airflow 3 Grid View UI": the same pattern for task instances.
