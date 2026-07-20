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

Adding a provider is cheap to write and expensive to keep. Every provider in
this directory is a released Python distribution: it gets its own version
stream, its own row in the compatibility matrix, its own CI test type, its own
docs build, its own dependency set in the shared `uv.lock`, and a place in every
future release wave the release manager drives. It also acquires an implicit
promise — that when the vendor's SDK breaks, someone in this project will fix
it.

The scarce resource is not code, it is stewardship. A provider whose only
interested party is its original author becomes unmaintained the moment that
author's employer changes priorities, and the cost lands on the release
managers, who cannot cut a wave with a provider that no longer builds. That is
why the project has an open governance discussion about the conditions for
accepting new providers and about provider lifecycle at all.

That discussion has **not** stopped intake, and this ADR must not be read as if
it had. A pause on individual provider proposals was in effect around the #58273
discussion in late 2025; it is not in effect now. Eight new providers
merged in the six months to July 2026 — #61561 (common.ai), #57610
(Informatica), #63988 (Vespa), #64754 (Akeyless), #62790 (IBM MQ), #67080
(ClickHouse), #69003 (Anthropic), #69795 (common.dq) — and at least two of them
carry no dev-list link in the PR at all. The operative requirement is therefore
*a demonstrable case and a steward*, established wherever the project is
currently having that conversation. Closing a first-time contributor's provider
PR on process grounds is the most expensive false positive available here, and
is not what the record supports: the correct action is to ask for the thread.

The same reasoning applies one level up, to *new abstractions* offered to
providers. A proposed task-instance state, a new listener hookspec, a new
operator base class or a new cross-provider mixin is not a provider-local
decision either: it changes what every provider may rely on. When such work is
opened as code first, the code is usually correct in the small and wrong in the
large — the discussion that follows reframes the feature, and the PR series is
closed and rewritten at a different layer. That is not wasted review, but it is
review spent in the wrong order.

Airflow already has the venues for this: the dev list for provider proposals and
lifecycle questions, and the AIP process for anything that adds surface the whole
project must live with. The decision is cheap there and expensive in a PR.

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

- Providers in the tree have, at minimum, an identified reason to exist and
  someone who answered for them. The release wave stays cuttable.
- Contributors who write the code first lose that work, or hold it until the
  discussion lands. The rule is stated here precisely so the loss is avoidable.
- Genuinely useful integrations arrive later than they would under an
  open-intake policy, and some arrive as a third-party distribution instead —
  which is an acceptable outcome, not a failure.
- Review effort concentrates on providers the project can actually support,
  rather than being spread across packages nobody maintains.

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

- #58273 — "Great Expectations Provider": closed with a pointer to the dev-list
  thread on provider governance, during the intake pause then in effect; the
  author moved the proposal to the list. That pause has since lapsed.
- #61561, #57610, #63988, #64754, #62790, #67080, #69003, #69795 — eight new
  providers merged between February and July 2026, which is why the "closed and
  redirected" outcome is scoped to an active pause rather than stated as the
  default. #69003 and #62790 carry no dev-list reference and merged anyway.
- #63401 — "Add Stripe provider": closed by the author after no other adopters
  surfaced, with the integration kept internal instead.
- #61752 — "Add Sail provider for Spark Connect compatible engine" and
  #57142 — a set of MariaDB provider files: provider proposals opened as code,
  which then stalled without a community decision behind them.
- #66402, #66445, #66410, #66453 — the AIP-96 series (a new `CHECKPOINTED` task
  state, a new exception, the supervisor wiring, a listener hookspec and a demo
  operator), all closed once the accepted AIP-103 work showed the same
  requirement was met without new project-wide surface. Four PRs' worth of
  implementation resolved by a design decision.
- #61777 — "Support bytes data values in xcom backend": review response was to
  take the question to the mailing list first, because the change alters what
  every backend must accept.
