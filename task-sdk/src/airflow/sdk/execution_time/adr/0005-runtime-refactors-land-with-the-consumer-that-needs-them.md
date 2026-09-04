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
client. It is an obvious extraction target — pull the socket reader into a shared
module, drop a redundant argument, replace an idiom. These changes are easy to
write, produce a large readable diff, and claim no behaviour change.

They are the hardest to review here. "No behaviour change" is exactly what a
reviewer cannot verify from the diff: the runtime's correctness lives in EOF
handling, partial reads, signal delivery order, and warm-shutdown timing, none of
which a structural diff makes visible and none fully pinned by existing tests. The
review cost is paid up front and in full, while the benefit accrues only if a
second consumer of the extracted primitive materialises.

Often it does not. A PR extracting the selector loop and socket reader out of
`WatchedSubprocess` for a future Java-activity subprocess drew "Why do we need
this?" and was reverted entirely (#67175). A slice split from a larger
shared-request-handler change to be easier to review was closed as a duplicate
once the parent merged (#67090). A repo-wide `type(self)` → `self.__class__` swap
was closed by its author as "a change for the sake of change" given the
`lazy_object_proxy` difference (#53856). A rework of how `SUPERVISOR_COMMS` stores
its logger stalled on what it bought (#66694). The pattern: structural change is
priced as risk, worth paying only when something concrete needs the new structure
now.

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

Cleanup the codebase would benefit from stays undone until a feature needs it, and
the runtime keeps some duplication and large methods longer than an aesthetic
standard would allow. "Tidy up the supervisor" is not an available on-ramp — a
real cost, since it is what newcomers most often reach for.

In exchange, scarce expert review time in the highest-risk directory in the SDK is
spent on changes with a demonstrated consumer, and timing-dependent behaviour is
not perturbed by diffs whose value is hypothetical. When the consumer arrives, the
extraction is designed against a real second use.

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

- #67175 — extracting the selector loop and socket reader for future reuse; closed
  after "Why do we need this?" and reverted.
- #67090 — a slice split from a larger shared-request-handler PR; closed as a
  duplicate once the parent merged.
- #53856 — repo-wide `type(self)` → `self.__class__`; self-closed as change for
  the sake of change after the `lazy_object_proxy` edge case.
- #66694 — reworking how `SUPERVISOR_COMMS` holds its logger; stalled on what it
  bought.
- #64054 — a query-count optimisation on this path; self-closed as harder to read
  than the cost it removed.
