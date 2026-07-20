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

# 5. Runtime refactors land with the consumer that needs them

Date: 2026-07-20

## Status

Accepted

## Context

The supervisor is a single long-lived object holding a selector loop, buffered
socket readers, subprocess lifecycle, signal handling, and the Execution-API
client. It is an obvious target for extraction: pull the socket reader and the
selector dispatch into a shared module, drop a redundant argument, replace an
idiom with a cleaner one. These changes are easy to write, produce a large
readable diff, and claim no behaviour change.

They are also the hardest kind of change to review here. "No behaviour change"
is exactly the claim a reviewer cannot verify from the diff: the runtime's
correctness lives in EOF handling, partial reads, signal delivery order, and
warm-shutdown timing, none of which a structural diff makes visible, and none of
which the existing tests fully pin. The review cost is paid up front and in
full, while the benefit is speculative — it accrues only if a second consumer
of the extracted primitive actually materialises.

Often it does not. A PR extracting the selector loop and buffered socket reader
out of `WatchedSubprocess` into a shared module, explicitly justified by "further
reusability" for a future Java-activity subprocess, drew a one-line "Why do we
need this?"; the author reconsidered and went the opposite way, reverting the
original modularization entirely (#67175). A PR splitting out one part of a
larger shared-request-handler change — reasoning that a smaller slice would be
easier to get reviewed — was closed as a duplicate once the larger PR merged
(#67090). A repo-wide swap of `type(self)` for `self.__class__` was closed by its
author after a reviewer pointed out the two differ under `lazy_object_proxy` and
that each usage would need independent justification, agreeing it was "a change
for the sake of change" (#53856). A rework of how `SUPERVISOR_COMMS` stores its
logger stalled on the same question of what it bought (#66694).

The pattern is consistent: in this directory, structural change is priced as
risk, and the price is only worth paying when something concrete needs the new
structure now.

## Decision

**A refactor of the execution runtime must land together with the consumer that
requires it, or not at all.**

- **No extraction for anticipated reuse.** A helper, module, or abstraction
  pulled out of `supervisor.py` / `task_runner.py` / `comms.py` needs a second
  caller in the same PR. "So it can back other bridges later" is not a
  justification; when the later work arrives it can do the extraction with the
  shape it actually needs.
- **No idiom-level sweeps.** Repo-wide substitutions of one spelling for another
  in the runtime are declined unless each site is individually justified —
  runtime code has proxy, fork, and metaclass edge cases where the two spellings
  are not equivalent.
- **A behaviour-preserving refactor must show how it is behaviour-preserving.**
  Name the IPC and signal paths the change touches and the tests that cover
  them; run the supervisor and task-runner suites and say so. "The behavior
  should be unchanged after this refactor" is the claim under review, not
  evidence for it.
- **Do not split a slice out of an in-flight PR to get it merged faster.** Check
  for the larger change first and contribute to it; a partial slice becomes a
  duplicate the moment the parent lands.
- **Keep refactor and behaviour change in separate PRs.** A diff that both moves
  code and changes what it does cannot be reviewed as either.

## Consequences

Cleanup that the codebase would genuinely benefit from stays undone until a
feature needs it, and the runtime keeps some duplication and some large methods
longer than a purely aesthetic standard would allow. Contributors looking for a
first change in this area will find that "tidy up the supervisor" is not an
available on-ramp — a real cost, since it is the kind of task newcomers most
often reach for.

The compensating benefit is that scarce expert review time in the highest-risk
directory in the SDK is spent on changes with a demonstrated consumer, and that
the runtime's timing-dependent behaviour is not perturbed by diffs whose value
is hypothetical. When the consumer does arrive, the extraction is designed
against a real second use rather than a guessed one.

A change **violates** this decision when it:

- extracts a helper, class, or module out of the execution runtime with only one
  caller in the PR, justified by future or parallel reuse.
- applies a mechanical idiom substitution across runtime files without a
  per-site justification.
- describes itself as a pure refactor of `supervisor.py`, `task_runner.py`, or
  `comms.py` but cites no test run over the supervisor / task-runner suites.
- mixes a structural move with a behaviour change in one diff.
- carves a slice out of an open PR by the same or another author instead of
  reviewing or contributing to that PR.
- removes or reshapes an argument, attribute, or indirection in the runtime on
  the grounds that it is redundant, without showing what the change enables.

## Evidence

- #67175 — extracting the selector loop and buffered socket reader into a shared
  module for future reuse; closed after "Why do we need this?", and the original
  modularization was reverted.
- #67090 — a slice split out of a larger shared-request-handler PR to be easier
  to merge; closed as a duplicate once the parent merged.
- #53856 — repo-wide `type(self)` to `self.__class__` swap; closed by the author
  as change for the sake of change after the `lazy_object_proxy` edge case was
  raised.
- #66694 — rework of how `SUPERVISOR_COMMS` holds its logger; stalled on what the
  change bought over the existing attribute.
- #64054 — a query-count optimisation elsewhere on this path, self-closed
  because the resulting logic was harder to read than the cost it removed.
