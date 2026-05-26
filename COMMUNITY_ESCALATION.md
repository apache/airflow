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

# Community escalation process

This document describes how the Apache Airflow community responds when a
contributor's behaviour repeatedly imposes an unreasonable burden on
maintainers or the wider community — for example, [Code of Conduct](CODE_OF_CONDUCT.md)
breaches, spamming project channels, abusing project resources, or
sustained disruption that the normal review and mentoring process
cannot resolve.

It complements the [Code of Conduct](CODE_OF_CONDUCT.md), which sets
the standards of behaviour expected from everyone interacting with the
project. The Code of Conduct says **what** is expected; this document
describes **what happens** when those expectations are repeatedly
ignored, and how affected contributors can appeal a decision.

## Scope

Behaviours that may lead to escalation include (but are not limited to):

* **Code of Conduct violations** — harassment, personal attacks,
  discriminatory behaviour, or any other conduct that breaches the
  [ASF Code of Conduct](https://www.apache.org/foundation/policies/conduct).
* **Spamming the project** — opening low-quality or duplicate issues
  and PRs in bulk, mass-pinging maintainers, or posting promotional,
  off-topic, or unsolicited content on GitHub, the mailing lists,
  Slack, the CWiki, or any other project channel.
* **Repeatedly disregarding maintainer feedback** — for example,
  reopening the same PR after closure with no substantive change, or
  continuing to bypass project quality criteria after being asked to
  stop. The general PR quality bar is described in
  [Pull request quality criteria](contributing-docs/05_pull_requests.rst#pull-request-quality-criteria).
* **Submitting unvetted Gen-AI-generated content** — the specific
  expectations around generative-AI-assisted contributions are
  described in
  [Gen-AI Assisted contributions](contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions);
  the escalation steps that follow when those expectations are
  ignored are the ones described here.
* **Conduct that violates GitHub's Terms of Service** — for example,
  evading prior blocks, automated abuse, or coordinated harassment.
* **Being generally and persistently disruptive to the community** in
  ways that are not covered by the categories above but that
  similarly burden maintainers or other contributors.

This document is **not** about ordinary disagreements, slow PRs, or
contributions that simply need more work — those are handled through
normal review and mentoring (see
[How to communicate](contributing-docs/02_how_to_communicate.rst) and
[Pull request guidelines](contributing-docs/05_pull_requests.rst)).

## Escalation steps

The community applies the minimum necessary measure at each step. The
goal is to protect maintainer time and the health of the project, not
to punish contributors. A contributor who acknowledges feedback and
changes their behaviour is welcomed to continue contributing at any
point in this sequence.

1. **Direct feedback by maintainers.** A maintainer points out the
   problem on the PR, issue, mailing list thread, or Slack message
   and links to the relevant guideline (Code of Conduct, Gen-AI
   guidelines, PR quality criteria, communication guidelines, etc.).

2. **Closing PRs or locking threads.** If the behaviour persists,
   maintainers may close one or more of the contributor's open PRs
   or lock conversations that have become unproductive. When a
   contributor accumulates multiple PRs flagged for the same problem,
   maintainers may close all of that contributor's open PRs at once
   to avoid repeated review burden. Each closure or lock is
   accompanied by a comment pointing to the violated guideline so the
   contributor knows what to fix.

3. **PMC-level blocking from the project.** If the behaviour continues
   after the previous steps, PMC members may decide to block the
   contributor from making further contributions to the Apache
   Airflow project. Such a block is a last-resort measure to protect
   the project from sustained harm and to avoid overburdening
   maintainers. The decision is taken as a
   [LAZY CONSENSUS](https://community.apache.org/committers/lazyConsensus.html)
   among PMC members on the private PMC list, so that it is
   collectively agreed, not vetoed, and recorded in the Apache
   Software Foundation's archives.

4. **ASF Infrastructure / cross-project block.** In extreme cases —
   for example, behaviour that affects multiple ASF projects or
   evasion of an Airflow-level block — the PMC may request the ASF
   Infrastructure team to block the contributor at the organisation
   level, across all ASF projects.

5. **Reporting to GitHub.** If a contributor is evidently spamming
   the project, evading prior blocks, or otherwise breaching
   [GitHub's Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service),
   maintainers may report the account to GitHub. GitHub may then
   take its own action, which can include suspending or deleting the
   account.

A separate, parallel path applies specifically to Code of Conduct
violations: anyone observing such behaviour can — and is encouraged to
— also follow the
[ASF reporting guidelines](https://www.apache.org/foundation/policies/conduct#reporting-guidelines)
directly, independently of the steps above.

## Appeals — `private@airflow.apache.org`

Any contributor affected by a decision taken under this process may
appeal it by emailing the Apache Airflow PMC at:

> **private@airflow.apache.org**

This is the appropriate appeal channel for **all** escalation decisions
described in this document — including PR closures, project-level
blocks, and infrastructure-level block requests.

When sending an appeal, please:

* Identify the decision being appealed (link to the PR, issue, thread,
  or block notice where possible).
* Describe the grounds for the appeal — what the decision got wrong,
  what additional context the PMC should consider, or what has
  changed since the decision was taken.

Appeals are reviewed by the PMC on the private list and the outcome is
communicated back to the contributor. The PMC may uphold the original
decision, modify it, or reverse it.

For Code of Conduct matters, contributors also retain the separate
right to escalate to the ASF Conduct Committee as described in the
[ASF Code of Conduct](https://www.apache.org/foundation/policies/conduct).
