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

# 4. Auth and token scopes change only through the security-model process

Date: 2026-07-18

## Status

Accepted

## Context

The Execution API is the trust boundary between untrusted workers and the control plane.
Each task receives a short-lived JWT scoped to its task instance, and route-level
enforcement (`ExecutionAPIRoute` + `require_auth`, the `"execution"` vs `"workload"` token
distinction, and the `ti:self` path-parameter validation in `security.py`) is what keeps a
compromised worker from acting outside its own task. This is the enforced core of Airflow's
documented security model.

Because these guards look like ordinary FastAPI dependencies, a feature PR can easily reach
for a small auth tweak — widening a token scope so a new endpoint "just works," relaxing a
guard to unblock a test, attaching `token:workload` to a route that previously required the
narrower execution scope. Each such tweak silently enlarges what a worker token can do, and
the blast radius is the whole isolation model. These changes cannot be judged by a single
feature reviewer; they need the people who own the threat model.

## Decision

Do not unilaterally widen token scopes or relax auth guards inside a feature PR.

Any change to authentication, token type/scope, or task-level authorization on the
Execution API goes through the security-model process: the security team plus the dev-list,
against the documented security model (`airflow-core/docs/security/security_model.rst` and
`jwt_token_authentication.rst`), before implementation lands.

- New endpoints reuse the existing, correctly-scoped guard for their access class rather
  than introducing a broader one for convenience.
- Broadening `token:workload` onto write routes, loosening `require_auth`, or changing
  `ti:self` validation are security-model decisions, not feature decisions.

## Consequences

- The worker↔server trust boundary stays owned by the security process, so scope creep in
  token permissions is caught before it ships.
- Feature work that genuinely needs a new access shape is slower — it must open the security
  discussion first — which is the intended trade for not letting auth widen opportunistically.

**A violating change looks like:** a feature PR that attaches `token:workload` to a write
route it previously lacked, relaxes a `require_auth` guard, or alters token-scope/`ti:self`
checks as an incidental "while I'm here" change, without going through the security team and
dev-list. Such changes are reverted or closed regardless of the feature's merit.

## Evidence

- #66608 — deadline-callback context routed through the Execution API using existing scoping
  rather than widening token access.
- #63880 — "Introduce optional task-level authorization for Execution API"; closed unmerged —
  a task-authorization model change belongs to the security process, not a feature PR.
- #60431 — "Fix execution API token access checks"; closed unmerged rather than landing an
  opportunistic auth-check change outside security-model review.
