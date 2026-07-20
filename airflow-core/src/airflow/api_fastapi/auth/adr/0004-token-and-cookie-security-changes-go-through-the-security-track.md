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

# 4. Token and cookie security changes go through the security track, in both directions

Date: 2026-07-20

## Status

Accepted

## Context

ADR 3 gates changes that **widen** access on the security process. Part of the
tightening direction is gated too — but only part, and getting the boundary right
matters more here than in most ADRs, because the wrong boundary rejects real
security fixes.

The boundary is *scope*, not direction. Localised hardening merges on its merits
and does so routinely: #62771 scoped the session cookie to `base_url`, #65348 set
`Secure` on the refresh cookie over HTTPS, #66502 set `SameSite=Lax` on the login
cookie, #67909 made a non-matching `kid` raise. All four merged, in the year
*after* the closure this ADR is built on — #66502 landing on
`managers/simple/routes/login.py`, the same file where #54607 was closed. Any rule
that treats "standalone" plus "touches a cookie flag" as sufficient grounds to
refuse would have rejected all four.

What is gated is a change to the token *model* rather than to one of its settings.
Claim validation as a regime, algorithm and `kid` handling, audience and issuer,
lifetime/refresh/revocation semantics — these interact with each other, with the
execution API's task tokens, and with every auth-manager provider that issues or
consumes them. A PR that reworks how tokens are validated or how long they live
cannot be reasoned about one switch at a time, and merging such reworks piecemeal
produces a surface nobody has designed as a whole and forecloses the coherent
design the security team is working out. Airflow has closed PRs of that shape with
exactly that reasoning: the changes are coming, from a joint effort of the security
team and the people who implemented the APIs, because they need deep knowledge of
the internals to get right.

A single flag, scope, or check that is correct on its own terms is not that. It
lands on its merits, as the four PRs above did.

The same holds for the security-adjacent posture of the API server itself.
Loosening a production default for local-development convenience is refused even
when the mechanism is unrelated to auth, and an upstream CVE announcement is not
by itself a reason to change a dependency declaration — the deciding question is
Airflow's actual exposure.

None of this makes reporting or fixing a security defect harder. Genuine
vulnerabilities go to the ASF security process, which is faster than a PR, not
slower.

## Decision

Changes to the token and cookie surface are coordinated, not opportunistic:

- **Reworking the token model goes through the security-team track.** Changing the
  claim-validation regime, signing/algorithm or `kid` handling as a scheme, or the
  lifetime/refresh/revocation semantics is designed as a whole, not assembled from
  standalone PRs.
- **A localised, self-contained hardening fix is not gated.** Setting a cookie flag
  correctly, scoping a cookie to its path, making one validation reject instead of
  fall through, or replacing a primitive with a safe one lands on its merits with a
  test. If the change is fully described by "this one setting was wrong and is now
  right", it is an ordinary bugfix.
- **The test is scope, not direction.** "It is strictly more secure" does not by
  itself exempt a rework from coordination — but neither does "it touches auth" make
  a one-line fix into a security-model decision.
- **Report vulnerabilities through the ASF security process**, not as a public PR
  or issue.
- **Do not relax a production-facing default for developer convenience**, even
  when the mechanism looks unrelated to auth.
- **Assess Airflow's exposure before reacting to an upstream CVE.** A published
  advisory against a dependency is not on its own a reason to change a declared
  version range or a runtime default here.

## Consequences

- The token surface evolves as a designed whole rather than as an accumulation of
  individually-reasonable tweaks.
- Contributors who spot a real weakness get a slower-looking path for a PR and a
  faster one for a report — which is the ordering the project wants.
- Some good hardening waits for the coordinated change that supersedes it, and is
  visibly "closed" in the meantime. Reviewers should say why, and where the work
  is going, rather than closing silently.
- Reviewers can decline these PRs on process grounds without having to
  first litigate the technical merits in a public thread.

A change **violates** this decision when it:

- reworks the claim-validation regime, the signing/algorithm/`kid` scheme, or the
  lifetime/refresh/revocation semantics in a standalone PR without the
  security-team track behind it;
- argues that such a rework needs no coordination because it only tightens;
- discloses a suspected vulnerability in a public PR or issue rather than through
  the ASF security process;
- weakens a production-facing default (link modes, transport, file handling) to
  make a local development setup work;
- changes a dependency declaration or runtime default purely because an upstream
  CVE was announced, with no assessment of Airflow's exposure.

## Evidence

- #54607 — additional security options for the JWT token cookie: closed, with the
  security team's comprehensive JWT design named as the vehicle for any such
  change.
- #60431 — execution-API token access checks: closed with related reasoning, that
  further JWT checks and the security choices behind them are a joint effort of the
  security team and the API implementers. Weigh it lightly here: the PR touches
  `execution_api/` and `common/db/`, **not** this area, and the closing comment
  leads with failing CI rather than with the coordination argument.

The counter-evidence bounds the rule, and belongs in the record just as much —
these are the localised fixes that merged without the security track, three of them
after #54607 was closed:

- #62771 — scoped the session token cookie to `base_url` (merged 2026-03-03).
- #65348 — set the JWT refresh cookie `Secure` on HTTPS (merged 2026-04-16).
- #66502 — set `SameSite=Lax` on the login cookie (merged 2026-05-07), on the same
  file as the #54607 closure.
- #67909 — raised on a non-matching `kid` instead of falling back (merged
  2026-06-03).
- #52633 — enabling symlink link-mode for the API server: refused because it
  offers no advantage over hardlinks or clone and opens potential security issues
  in a production setting; the motivation was a local development configuration.
- #64402 — raising the Starlette minimum in response to an upstream CVE: declined
  on the principle that minimums are not raised unless the issue affects Airflow
  directly.
(#66797 and #66798 — UI dependency bumps — were previously cited here. They concern
neither tokens nor cookies and are already cited, correctly, in ADR 0005.)
