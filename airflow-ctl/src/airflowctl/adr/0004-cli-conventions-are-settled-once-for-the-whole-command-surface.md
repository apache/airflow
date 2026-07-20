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

Most of the `airflowctl` command surface is not written by hand. Commands are
derived from the generated operations layer, so the way a command presents
itself — which parameters are positional and which are `--flag`, how a missing
required value is reported, which exit code a failure produces, when the user is
prompted — is decided by shared wiring in `ctl/cli_config.py` and the helpers
around it, not by the handler for any one command.

That makes a per-command convention change a peculiar kind of defect. It looks
like a small, self-contained improvement, it passes review on its own merits,
and it produces a surface where `airflowctl dags pause` takes an argument one
way and `airflowctl variables get` takes it another. Worse, because the command
is generated, the local deviation is silently reverted the next time the
datamodels are regenerated — the reviewer approves a change that will not
survive.

The argument-style question is the worked example. Two independent PRs proposed
opposite treatments of required parameters in the same window. Rather than merge
either, maintainers parked both and opened a dev-list thread; the discussion
weighed supporting both positional and flag forms (rejected as a non-standard
approach that constrains every future CLI-library choice and expands the testing
surface permanently) against a clean break while the distribution is still
pre-1.0. Lazy consensus settled on required-positional / optional-flag, and only
then was it applied across every generated command at once. The same reasoning
closed a `--print-token` flag on `auth login`: the question was not whether that
one command should print a token, it was what the whole `auth` group's
convention should be, and the answer — a separate `auth token` command, on the
`gh` model — reshaped the group rather than one command.

The pre-1.0 status of this distribution cuts both ways. It is the window in
which a convention can still be changed cheaply, which is why maintainers are
willing to reopen these questions at all — and exactly why they insist the
answer be applied uniformly instead of accumulating per-command exceptions that
become a compatibility burden at 1.0.

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

Scripts and operators see one CLI, not a patchwork, and the generated layer
stays the single place a convention is expressed — which is what keeps
regeneration safe.

The cost is real and falls on contributors. A convention change cannot be
shipped as a small PR: it means touching the shared wiring, re-testing every
command, and often waiting weeks for a dev-list thread to reach consensus. Good
contributions have been parked for exactly that reason, and at least one was
eventually closed for inactivity while the discussion ran. Contributors new to
the area routinely discover this only after writing the per-command version.

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

- #65261 — required fields both positional and optional: parked, taken to the
  dev list, decided by lazy consensus, then applied surface-wide.
- #65699 — competing positional-parameter fix, explicitly held pending that
  discussion and closed for inactivity while it ran.
- #64812 — parallel attempt at the same convention change, closed once the
  agreed version landed in #66768.
- #66768 — the accepted implementation: required params positional and optional
  params `--flag`, applied across the generated commands together.
- #63085 — `--print-token` flag on `auth login` rejected; reviewers required a
  separate `auth token` command on the `gh auth token` model.
- #62843 — the earlier version of the same change, where printing the token to
  stdout as part of login was first objected to.
