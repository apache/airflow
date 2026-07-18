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

# 3. Cadwyn covers older-client to newer-server only; other directions are explicit

Date: 2026-07-18

## Status

Accepted

## Context

It is tempting to treat "we use Cadwyn" as "compatibility is handled." Cadwyn
solves exactly one direction of the compatibility matrix: an **older client
against a newer server**. The server holds the full migration chain and can
render its current internal schema back to what any older, still-deployed client
expects.

There are four directions to reason about, and Cadwyn only owns one:

1. **Older client → newer server** — covered by Cadwyn migrations.
2. **Newer client (SDK) → older server** — *not* covered. The older server does
   not know the newer version; the client must negotiate down to a version the
   server advertises, or the feature must be scoped so this pairing cannot
   occur, or the client must degrade gracefully.
3. **Adding a new endpoint/param alongside old ones** — the old endpoints and
   old parameters must keep working unchanged; a new path or optional parameter
   must not alter or remove the existing one.
4. **Same version both sides** — trivially fine.

Direction 2 is the trap. A worker running a newer SDK can reach an API server
that has not yet been upgraded. If a PR assumes the server will always be at
least as new as the client, it ships a feature that hard-fails on that pairing.

## Decision

Reason explicitly about all four compatibility directions for every
Execution API change; do not assume "Cadwyn handles compat."

- **Newer-SDK → older-server** must be handled deliberately — via version
  negotiation to a version the server supports, a graceful degradation path, or
  an explicit scope statement that the pairing is out of support for the feature
  — never left implicit.
- When a new endpoint or a new parameter is added, the pre-existing endpoints
  and parameters keep working exactly as before. New surface is additive; it
  does not repurpose or remove old surface.

## Consequences

- Features are designed against the real deploy matrix rather than an assumed
  "server is always newer" world, so a freshly upgraded worker talking to a
  lagging server behaves predictably.
- Contributors carry a small analysis burden per change (state which of the four
  directions apply and how each is satisfied), which is the point — it surfaces
  the uncovered direction before merge.

**A violating change looks like:** a PR that relies on a newer-SDK feature
against an older server with no negotiation or degradation path, defended with
"Cadwyn handles compatibility"; or a change that alters/removes an existing
endpoint or parameter when adding a new one, on the assumption that only the
older-client→newer-server direction matters.

## Evidence

- #50117 — `LazyXComSequence` slicing was implemented with the client/server version pairing reasoned through, not assumed away by Cadwyn.
