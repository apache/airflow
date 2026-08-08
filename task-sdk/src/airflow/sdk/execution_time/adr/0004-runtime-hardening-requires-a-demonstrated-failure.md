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

The supervisor/runner runtime attracts defensive changes — an extra `try`/`except`,
a retry on a heartbeat, a guard against a hostname mismatch, a louder log line —
each a branch that "should never" be taken. Here that reasoning is wrong in a
specific way: every added guard is a path no test drives, on code that runs in a
forked subprocess under signal and IPC timing. It can't be exercised in review and
once merged is never removed, because nobody can prove the guarded condition
doesn't happen. Worse, a guard on a mis-diagnosed failure hides the real one — a
broad `except` around a supervisor call converts a dropped terminal state into
silent success; a retry on a non-idempotent write duplicates side effects.

So reviewers apply one question — *has this failure actually been observed?* — and
close the change when the answer is no, including maintainers': Sentry-instrumentation
wrapping closed for want of a production occurrence (#67505), a speculative
heartbeat guard stripped for lack of evidence the hostname error occurs (#64221).
The same question kills changes whose bug is real but mechanism wrong — a "wrong
fix" that never reaches the reported failure (#64945). The rule declines changes
that might have prevented a future incident; the project accepts that, because an
unfalsifiable guard is a permanent, untested branch on the path every task in
every deployment runs through.

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

Contributors must do diagnostic work first, and some genuine latent bugs stay
unfixed until observed in the field. Changes that "look obviously safe" are
declined — friction to first-time contributors and a frequent source of closed
PRs.

In exchange, the runtime keeps a small, testable set of failure paths; the
supervisor keeps failing *closed* on terminal-state errors; and incident
investigation is not obstructed by guards that convert real errors into log noise.
The rule applies equally to maintainers, which makes it enforceable.

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

- #56695 — "Fix memory leak in remote logging connection cache"; **merged**, the
  model of what this decision asks for: the PR body carries the production
  occurrence (Celery workers OOMing on 3.0.6), before/after memory graphs, a named
  mechanism (`@lru_cache` retaining client references), and a `test_supervisor.py`
  test alongside the `supervisor.py` change. That weight clears the bar — not
  volume of prose.
- #67505 — Sentry instrumentation isolation; closed by its author when no
  production occurrence existed.
- #64221 — supervisor heartbeat conflict fix; the speculative half stripped after
  a reviewer asked for evidence, but the PR was closed unmerged on 2026-03-26 —
  nothing landed. Cited as the rule operating, not an accepted outcome.
- #61642 — `CRITICAL` log on OOM-killed Celery task; closed because an OOM kills
  the supervisor too, so the log never emits.
- #65993 — triggerer in-process Execution API startup fix; withdrawn once the
  treated-as-failure output proved normal debug output.
- #64945 — `TriggerDagRunOperator` 404 in `dag.test()`; rejected as the wrong fix,
  not reaching the reported failure.
- #67266 — recovering SKIPPED on terminal-state IPC send failure; an extra IPC
  message does not change the swallowed failure.
- #61379 — `NameError` during task-execution completion; closed as already
  addressed differently on `main`.
