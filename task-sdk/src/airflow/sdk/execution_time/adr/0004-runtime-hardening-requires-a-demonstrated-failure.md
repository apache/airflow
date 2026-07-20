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

# 4. Runtime hardening requires a demonstrated failure, not a hypothesis

Date: 2026-07-20

## Status

Accepted

## Context

The supervisor/runner runtime attracts defensive changes: an extra `try`/`except`
around instrumentation, a retry on a heartbeat, a guard against a hostname
mismatch, a log line at a louder level when a process dies. Each one reads as
harmless — it only adds a branch that "should never" be taken.

In this area that reasoning is wrong in a specific way. Every added guard is a
path that no test drives, on code that runs in a forked subprocess under signal
and IPC timing. It cannot be exercised in review, and once merged it is never
removed, because nobody can prove the condition it guards against does not
happen. Worse, a guard placed on a mis-diagnosed failure actively hides the real
one: a broad `except` around a supervisor call converts a dropped terminal state
into a silent success, and a retry on a non-idempotent write converts a
transient error into duplicated side effects.

The rejection record shows reviewers applying one question to these changes:
*has this failure actually been observed?* When the answer is no, the change is
closed — including changes authored by maintainers. A proposal to wrap Sentry
instrumentation so a raising SDK could not abort the task run was closed by its
own author once a reviewer asked for a production occurrence and there was none
(#67505). A supervisor heartbeat fix was challenged with "could you show any
evidence of hostname error occurring" and the speculative half of it was
stripped before the remaining 10-line race fix stood on its own (#64221). A
change to log `CRITICAL` when a Celery task is OOM-killed was closed because the
proposed mechanism could not work — an OOM kills the supervisor too, so nothing
gets logged at any level (#61642). A triggerer startup fix was withdrawn by its
author after review established that the stack output it treated as a hidden
failure was normal debug output, so the change improved visibility of a failure
that was not occurring (#65993).

The same question kills changes whose stated bug is real but whose mechanism is
wrong: a fix that leaves the actual reported failure in place was rejected as a
"wrong fix" even though it changed a genuine defect in `task_runner.py` (#64945),
and adding another IPC message to address a swallowed exception drew "I fail to
see how adding another message to send over the IPC changes anything about this"
(#67266).

The cost of the rule is real: it declines changes that would have prevented a
future incident. The project accepts that cost because an unfalsifiable guard in
the execution runtime is not free — it is a permanent, untested branch on the
path every task in every deployment runs through.

## Decision

**A change to the execution runtime that hardens, guards, retries, or re-logs a
failure must first establish that the failure occurs, and that the proposed
mechanism actually addresses it.**

- **State the observed failure.** A production occurrence, a bug report with a
  stack trace, or a reproducer script that fails on `main`. "This could in
  principle raise" is not a justification for a new branch in the runtime.
- **Show the mechanism reaches the failure.** Trace the failure path explicitly:
  which process is alive when the guard runs, which side of the IPC boundary
  observes the error, and what state the task instance ends in. A guard in a
  process that the failure has already killed does nothing.
- **Verify the diagnosis before proposing the fix.** Confirm the symptom is a
  defect and not expected output; confirm the reported reproducer actually
  reproduces on current `main` before claiming to fix it.
- **A test must drive the new branch and fail without the change** — through the
  real supervisor/runner path, not an in-process construction that skips the IPC
  round-trip. A guard no test can enter is a guard this area does not take. For
  timing-dependent failures, a test that *forces* the interleaving — an injected
  delay, a monkeypatched sequencing point, a blocked read — while still going
  through the real socket is acceptable and expected; demanding that a race
  reproduce spontaneously would make every race unfixable, which is not the
  intent. A socket substitute that only controls chunk boundaries or arrival
  timing on real frame bytes also counts: a real `AF_UNIX` socketpair cannot be
  made to short-read on demand, so framing bugs are testable no other way. What
  does not count is a mock that fabricates the response instead of exercising the
  framing code — that proves the guard runs, not that the failure it guards
  against can reach it.
- **Never harden by broadening an `except`.** Narrow to the real exception class
  so IPC and Execution-API errors still surface; a non-success terminal state
  must never be masked as success.

## Consequences

Contributors must do diagnostic work before writing runtime code, and some
genuine latent bugs stay unfixed until they are observed in the field. Changes
that "look obviously safe" are declined, which reads as friction to first-time
contributors and is a frequent source of closed PRs.

In exchange, the runtime keeps a small, testable set of failure paths; the
supervisor keeps failing *closed* on terminal-state errors instead of silently
absorbing them; and incident investigation is not obstructed by layers of
guards that convert real errors into log noise. The rule applies equally to
maintainers, which is what makes it enforceable in review.

A change **violates** this decision when it:

- adds a `try`/`except`, retry, timeout, or fallback in `supervisor.py`,
  `task_runner.py`, or `comms.py` whose justification is that the wrapped call
  "could" fail, with no observed occurrence, reproducer, or issue cited.
- widens an existing `except` clause — broadening a named exception to
  `Exception`/`BaseException`, or dropping a class from the tuple — as part of a
  hardening change. Narrowing a bare `except:` to `except Exception:` is the
  reverse and does not count.
- claims `closes:` on an issue but ships no test that reproduces that issue's
  reported symptom.
- adds a guard or log statement in a process that the described failure
  terminates before the new code can run.
- adds a new defensive branch in `supervisor.py`, `task_runner.py`, or `comms.py`
  with no test that enters it through the real supervisor/runner IPC path — where
  forcing a timing-dependent interleaving with an injected delay, a monkeypatched
  sequencing point, or a socket substitute that only controls chunk boundaries or
  arrival timing on real frame bytes still counts as the real path, but a mock
  that fabricates the response does not. A guard whose triggering condition is a
  cited production traceback is exempt from this bullet: the failure is already
  demonstrated, and the guard is judged on mechanism instead. Files outside that
  list are out of range for this bullet.
- changes the log level or message of a failure path as the entire remedy for a
  reported bug, without changing behaviour.

Two further checks are *reviewer prompts*, not violation bullets, because
settling them means checking out the branch and running the reproducer — which no
automated pass over a diff can do. Raise them as questions on the PR:

- does the linked issue's reproducer still fail on current `main`?
- does the changed code path actually sit on the failure path the issue describes?

## Evidence

- #56695 — "Fix memory leak in remote logging connection cache"; **merged**, and
  the model of what this decision asks for. The PR body carries the production
  occurrence (Celery workers OOMing on Airflow 3.0.6), before/after memory graphs
  for the same 4 GB worker, a named mechanism (`@lru_cache` keyed on the client
  instance retaining references), and a test in
  `test_supervisor.py` alongside the `supervisor.py` change. Evidence of this
  weight is what clears the bar — not volume of prose.
- #67505 — Sentry instrumentation isolation; closed by its author after a
  reviewer asked for a production occurrence and none existed.
- #64221 — supervisor heartbeat conflict fix; a reviewer asked for evidence that
  the hostname error actually occurs and the speculative half was stripped, but
  the PR was closed unmerged on 2026-03-26 — nothing from it landed. It is cited
  here as the rule operating, not as an accepted outcome.
- #61642 — `CRITICAL` log on OOM-killed Celery task; closed because an OOM kills
  the supervisor too, so the log never emits.
- #65993 — in-process Execution API startup fix for the triggerer; withdrawn
  after review showed the treated-as-failure output was normal debug output.
- #64945 — `TriggerDagRunOperator` 404 in `dag.test()`; rejected as the wrong
  fix because the change did not reach the reported failure.
- #67266 — recovering SKIPPED state when the terminal-state IPC send fails;
  reviewer challenged that an extra IPC message does not change the swallowed
  failure.
- #61379 — `NameError` during task-execution completion; closed after review
  found the failure already addressed differently on `main`.
