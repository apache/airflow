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

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage overrides

This file is the adopter-scoped override entry point for the
[`pr-management-triage`](../../.claude/skills/pr-management-triage/SKILL.md)
skill. Per the [agentic-overrides contract](https://github.com/apache/magpie/blob/main/docs/setup/agentic-overrides.md),
the skill consults an override file named after itself before
executing default behaviour. Keep this file thin — a list of the
additional/modified steps with a one-line pointer each; the
detailed logic lives in the sibling `pr-management-triage-*.md`
files.

## Additional step: review-cost imbalance assessment

**When:** after the default classification in
[`classify-and-act.md`](../../.claude/skills/pr-management-triage/classify-and-act.md)
has produced a `suggested_action` for a PR, and **only** for PRs
whose suggested action is `mark-ready` or a pass-through (no
deterministic close/draft/comment already pending). Skip PRs the
default flow is already closing or drafting.

**What:** compute a **review-cost imbalance verdict** — is this PR
cheap to produce but expensive to review *properly*, in a
critical/hard area, from an author with no demonstrated standing
*in that area*? The full rubric (area metadata, scoring, decision
matrix, exemptions, message templates, backtest mode) lives in
[`pr-management-triage-review-imbalance.md`](pr-management-triage-review-imbalance.md).

**Dispositions it can add** (all maintainer-fired, never
autonomous):

- `recommend-close (review-imbalance)` — surface the drafted
  mentorship message; on confirmation, post + close.
- `draft-back (review-criteria)` — the PR is not closable but fails
  the area's review criteria: convert to draft, assign the author,
  and fold the specific gaps into the PR body (deduped against
  feedback already given and the author's prior responses). Runs as
  a **readiness gate before any `mark-ready`**, so a PR that fails
  area criteria never enters the reviewable queue.
- `discuss-first (review-imbalance)` — surface a "please discuss
  the approach first" comment; PR stays open.

A `pass` verdict leaves the default `suggested_action` untouched.

**Area review criteria are dual-use.** The `## Review criteria`
section of each area's `AGENTS.md` (mined from past merged +
rejected PRs) is consumed from both sides: **authoring** agents
preparing a change in that directory apply it as a pre-flight
self-review and fix the gaps *before* opening the PR; the
**triage** step above applies the same list to draft back PRs that
skipped it. One source of truth — see the linked file's §2b.

**Backtest sub-mode:** when the maintainer asks to "backtest the
imbalance step" / "how many ready PRs would this close", run the
same classifier over the already-cached
`ready for maintainer review` set from the session cache and print
a read-only rollup — acting on nothing. See the linked file's
*Backtest mode* section.
