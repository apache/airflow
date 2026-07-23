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

# 4. CLI conventions are settled once for the whole command surface

Date: 2026-07-20

## Status

Accepted

## Context

Most of the `airflowctl` command surface is generated from the operations layer,
so how a command presents itself — which parameters are positional and which are
`--flag`, how a missing required value is reported, which exit code a failure
produces, when the user is prompted — is decided by shared wiring in
`ctl/cli_config.py`, not by any one handler. That makes a per-command convention
change a peculiar defect: it looks like a small self-contained improvement, passes
review on its own merits, and produces a surface where `airflowctl dags pause` and
`airflowctl variables get` take arguments differently. Worse, because the command
is generated, the deviation is silently reverted at the next regeneration.

The argument-style question is the worked example. Two independent PRs proposed
opposite treatments of required parameters in the same window; maintainers parked
both and opened a dev-list thread, weighing supporting both positional and flag
forms (rejected as non-standard, constraining every future CLI-library choice and
permanently expanding the testing surface) against a clean break while still
pre-1.0. Lazy consensus settled on required-positional / optional-flag, applied
across every generated command at once. The same reasoning closed a `--print-token`
flag on `auth login`: the question was the whole `auth` group's convention, and the
answer — a separate `auth token` command on the `gh` model — reshaped the group.
Pre-1.0 status is why maintainers reopen these questions cheaply, and why they
insist the answer apply uniformly rather than accumulate per-command exceptions
that become a compatibility burden at 1.0.

## Decision

Conventions that span more than one command are settled for the command surface
as a whole, before implementation:

- **A change to argument style, error wording, exit-code mapping, prompt
  behaviour, or output presentation is a surface-wide change.** Implement it in
  the shared CLI wiring so every command — generated and hand-written — moves
  together.
- **Contentious surface conventions go to the dev list first.** Where two
  reasonable designs exist, the decision is taken by lazy consensus on the dev
  list and recorded there; PRs implementing either option wait for it rather
  than racing.
- **A convention deviation scoped to a single command is not accepted**, even
  when that command reads better for it. Consistency across the surface
  outranks a local improvement.
- **A secret is emitted only by a command whose sole purpose is to emit it** —
  never as a flag on, or a side effect of, a command the user ran for another
  reason.

## Consequences

Scripts and operators see one CLI, not a patchwork, and the generated layer stays
the single place a convention is expressed — which keeps regeneration safe. The
cost falls on contributors: a convention change means touching shared wiring,
re-testing every command, and often waiting weeks for a dev-list thread. Good
contributions have been parked for exactly that, and at least one was closed for
inactivity while the discussion ran — usually discovered only after writing the
per-command version.

A change **violates** this decision when it:

- alters argument arity/style, error text, exit codes, or prompting for one
  command or one command group *where the rest of the surface shares that
  convention*, leaving the others on the old one — resolving an argument defect
  specific to one command is not this: where no other command has the shape being
  changed, there is no surface left behind and no convention being forked;
- implements one side of an argument-style or output-shape question that is
  under open dev-list discussion, rather than waiting for the outcome;
- hand-edits a generated command's parameter handling to achieve a presentation
  change instead of changing the wiring that generates it;
- adds a flag or option that causes a token or other credential to be printed by
  a command whose primary purpose is something else.

## Evidence

- #65261 — required fields both positional and optional: parked, taken to the dev
  list, decided by lazy consensus, then applied surface-wide.
- #65699 — competing positional-parameter fix, held pending that discussion and
  closed for inactivity while it ran.
- #64812 — parallel attempt at the same change, closed once the agreed version
  landed in #66768.
- #66768 — the accepted implementation: required params positional, optional
  `--flag`, applied across the generated commands together.
- #63085 — `--print-token` on `auth login` rejected; reviewers required a separate
  `auth token` command on the `gh auth token` model.
- #62843 — the earlier version of the same change, where printing the token as part
  of login was first objected to.
