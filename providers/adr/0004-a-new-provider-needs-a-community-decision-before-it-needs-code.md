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

# 4. A new provider needs a community decision before it needs code

Date: 2026-07-20

## Status

Accepted

## Context

Adding a provider is cheap to write and expensive to keep. Every provider is a
released distribution — its own version stream, compatibility-matrix row, CI test
type, docs build, dependency set in the shared `uv.lock`, and a place in every
future release wave — and it acquires an implicit promise that someone here will
fix it when the vendor's SDK breaks. The scarce resource is stewardship: a
provider whose only interested party is its original author becomes unmaintained
the moment that author's priorities change, and release managers cannot cut a wave
with a provider that no longer builds. Hence the open governance discussion about
provider intake and lifecycle.

That discussion has **not** stopped intake. A pause was in effect around the #58273
discussion in late 2025; it is not in effect now. Eight new providers merged in the
six months to July 2026
(#61561, #57610, #63988, #64754, #62790, #67080, #69003, #69795), at least two with
no dev-list link. The operative
requirement is *a demonstrable case and a steward*, established wherever that
conversation currently lives; closing a first-time contributor's provider PR on
process grounds is the most expensive false positive here — the correct action is
to ask for the thread. The same reasoning applies to *new abstractions* offered to
providers (a task-instance state, a listener hookspec, an operator base class, a
cross-provider mixin): opened as code first, such work is usually correct in the
small and wrong in the large, and the series is closed and rewritten at a
different layer. The venues exist — the dev list for provider proposals and
lifecycle, the AIP process for anything that adds project-wide surface.

## Decision

The decision to accept a new provider — or a new abstraction that providers
build on — is made in the community, before the implementation is opened.

- **A new provider distribution starts on the dev list**, not as a PR. The
  thread must establish that there is demand beyond the author, and who will
  steward the package when its vendor SDK breaks.
- **A provider proposal opened as a PR without that thread is asked for it —
  not closed.** Review requests the dev-list discussion, the demand beyond the
  author, and the named steward, and the PR waits for them. It is closed only if
  an explicit intake pause is in effect at the time, or if the thread runs and
  concludes against. Check the current state of the governance discussion before
  invoking a pause; there was one around late 2025 and there is not one now.
- **A new cross-provider abstraction goes through the AIP process** when it adds
  surface the whole project must support — a task state, an exception type in
  the public hierarchy, a listener hookspec, a base class other providers are
  expected to subclass.
- **Demonstrated adopters are part of the case.** "We run this internally"
  without a second interested deployment is a reason to keep running it
  internally.
- **A provider that no longer has a steward can be suspended or removed** — the
  lifecycle end of the same decision, taken the same way.

## Consequences

- Providers in the tree have an identified reason to exist and someone who
  answered for them. The release wave stays cuttable.
- Contributors who write the code first lose that work, or hold it until the
  discussion lands. The rule is stated here so the loss is avoidable.
- Genuinely useful integrations arrive later, and some arrive as a third-party
  distribution instead — an acceptable outcome, not a failure.
- Review effort concentrates on providers the project can actually support.

A change **violates** this decision when it:

- introduces a new task-instance state, exception class in the public hierarchy,
  listener hookspec, or provider-facing base class as an implementation PR with
  no AIP or dev-list decision behind it;
- justifies a new provider solely by the author's own deployment, with no other
  adopter and no named steward;
- removes or suspends an existing provider without the same public decision.

Review **asks** — never grounds for closing a PR on their own:

- whether an intake pause is in effect is **not visible in a pull request**. There
  is no file, label, or diff that carries it; it lives in the state of the
  governance discussion on the dev list at the moment of review. A reopened
  provider proposal is therefore an ask — go and check the current state of that
  discussion, and if a pause is genuinely running, say so with a link. Absent that
  link, a reopened proposal is reviewed on its merits like any other;
- a new top-level directory under `providers/` with a `provider.yaml` and no
  linked dev-list thread: ask for the thread, the second adopter, and the
  steward. Providers have merged without a linked thread; a missing link is a
  gap to fill, not a disqualification.

## Evidence

- #58273 — closed with a pointer to the dev-list thread during the late-2025
  intake pause; that pause has since lapsed.
- #61561, #57610, #63988, #64754, #62790, #67080, #69003, #69795 — eight new
  providers merged Feb–July 2026, which is why "closed and redirected" is scoped
  to an active pause. #69003 and #62790 carry no dev-list reference and merged anyway.
- #63401 — closed by the author after no other adopters surfaced; kept internal.
- #61752, #57142 — provider proposals opened as code, which stalled without a
  community decision.
- #66402, #66445, #66410, #66453 — the AIP-96 series (a `CHECKPOINTED` state, an
  exception, supervisor wiring, a hookspec, a demo operator), all closed once
  AIP-103 met the requirement without new project-wide surface.
- #61777 — review response was to take the question to the mailing list first,
  because it alters what every backend must accept.
