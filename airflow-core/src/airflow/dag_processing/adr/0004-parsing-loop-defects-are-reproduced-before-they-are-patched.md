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

# 4. Parsing-loop defects are reproduced before they are patched

Date: 2026-07-20

## Status

Accepted

## Context

The Dag File Processor is a loop of short-lived subprocesses writing concurrently
to shared tables, often with several processors running against one database. Its
characteristic failures — a duplicate-key error surfacing as a spurious import
error, a Dag marked stale that should not be, a callback that appears to lose its
context, a clone racing another clone — all look, from a stack trace or a screenshot,
like a missing guard at the line that raised.

They usually are not. This subsystem already carries several layers that absorb
exactly these conditions: the parse results are committed at an outer boundary
rather than where the per-file work happens, and that boundary is wrapped in
database retry logic that turns a losing race into a no-op on the next attempt. A
patch aimed at the inner function is therefore frequently a patch to a line that
never raises in production, guarding a condition that is already handled one frame
out. It passes review reading, it passes a unit test built from the same
misunderstanding, and it changes nothing.

The evidence for this is unusually clean, because one contributor did the work.
Suspecting a duplicate-key race between two Dag processors from a red banner in
the UI, they proposed a fix, then stood the actual two-processor race up on `main`
against a real PostgreSQL instance. The error fired at the outer commit, not in the
function being patched, and the existing retry already absorbed it. They closed
their own PR as not needed. No reviewer could have reached that conclusion from
the diff.

The inverse also holds and is the reason this is a decision rather than advice: a
reviewer disputing a reported defect is expected to reproduce too. A report of
duplicate Dag identifiers not warning was answered by a reviewer running two Dags
with the same identifier in Breeze, getting the import error and the processor log,
and posting both.

The cost of the alternative is concentrated rather than spread. Every unreproduced
parsing-loop patch consumes expert review time in the area with the fewest experts,
to reach a conclusion the author could have reached in an hour with two processors
and a real database. Enough of them arrived — many of them unreviewed generated
code — that they became a standing drain and a driver for the project's generative-AI
contribution guidance.

## Decision

**A change to the parsing loop, and a claim that a parsing-loop defect exists,
carry a reproduction.**

- **Reproduce against a running processor before proposing the fix**, and describe
  the setup in the PR: number of Dag processors, backend (SQLite passes for things
  PostgreSQL and MySQL do not), and how the condition was triggered.
- **Show where the error actually surfaces.** Name the frame that raises and the
  commit boundary it belongs to. A patch to a function that does not raise in the
  reproduction is not a fix.
- **Check the existing absorption layers first** — the retry wrapper around the
  results commit, the per-file error handling that degrades one file rather than the
  loop. If one of them already handles the condition, the correct outcome is to close
  the PR.
- **A UI symptom is not a diagnosis.** A red banner, a stale marker, or a log line
  identifies where the condition became visible, not where it originated.
- **Disputing a report also requires a reproduction** — post the commands, the
  resulting behaviour, and the processor log, not an assertion.

## Consequences

- Review effort in the area with the fewest qualified reviewers is spent on changes
  that are known to do something.
- A repeated failure to reproduce is itself a useful finding: it says the layer
  under suspicion is already correct, and that belongs in the PR discussion where
  the next person will find it.
- The bar is real and it is high — standing up multiple Dag processors against
  PostgreSQL is more work than writing most of these patches.

**If you cannot meet the bar, report the observation instead of proposing a
mechanism** — and here that means a GitHub issue, which is a deliberate exception
to the repository's golden rule that a known fix goes straight to a PR. The rule
exists because an issue that a PR closes a day later is duplicate accounting and
attracts drive-by fixes. Neither applies to an observation with no proposed
mechanism: there is no in-flight PR for it to duplicate, and the thing being
recorded — *this happened, on this configuration, and I could not determine why* —
is exactly the input someone with a reproduction environment needs, and it outlives
any single PR. Describe what you saw, the backend, the processor count, and what you
ruled out. State plainly that you could not reproduce it. Do not attach a speculative
patch — that converts it back into the case the golden rule covers.

A change **violates** this decision when it:

- adds a guard, retry, or exception handler for a concurrency condition and its PR
  body describes neither a reproduction of that condition nor an observed
  occurrence carrying the concrete failure signature;
- patches a function that does not raise in the author's own reproduction;
- cites only a UI symptom, a log line, or an issue report as evidence of the
  mechanism it changes, without explaining how that symptom follows from the
  mechanism;
- reports the reproduction only on SQLite for a defect whose mechanism is
  multi-writer.

Reviewer prompts — the judgement this ADR exists to inform, which no diff can
settle on its own:

- Does the results-commit retry wrapper or the per-file error handling already
  absorb this condition? If so the correct outcome is to close the PR — but
  reaching that answer takes the reproduction, not the diff. This is the finding
  #66788's author could only reach by running it.

## Evidence

- #66788 — "Don't surface duplicate-key Dag-write race between Dag processors as
  import error": closed by its own author after standing the two-processor race up on
  `main` against real PostgreSQL and finding the `IntegrityError` fires at the outer
  `update_dag_parsing_results_in_db` commit — not in the patched function — where the
  existing database-retry wrapper already turns it into a no-op.
- #60761 — "Fix duplicate dag warning": a reviewer reproduced the scenario in
  Breeze with two Dags sharing an identifier, obtained both the import error and the
  Dag-processor log entry, and posted the evidence, which contradicted the premise.
- #60013, #60100, #60136 — stale-Dag-detection safeguards, import-error persistence
  from the Git bundle, and a scheduler/completed-Dag-run performance fix: all closed
  as unreviewed generated changes whose descriptions did not match their diffs.
- #61680 — a further unreproduced parsing-path patch from the same wave; the
  maintainer response to that wave noted the volume was driving the project's
  generative-AI contribution guidance and the related dev-list discussion.
- #66820 — "clear identity map between `_do_scheduling` phases": the positive
  example, on the scheduler side rather than the parsing loop. The author suspected
  a `dag_run` / `task_instance` deadlock across scheduling phases, then stood up two
  to three real schedulers against a MySQL 8.0 metadata database under load and
  closed their own PR: the deadlock did not occur, both fetches already take their
  rows with `FOR UPDATE SKIP LOCKED`, and the emitted SQL was identical with and
  without the change. The reproduction is what produced that finding; no reviewer
  could have reached it from the diff.
- #56710 — "Avoid `KeyError` in `_execute_task_callbacks` when a Dag is missing from
  the DagBag": drafted back for lacking tests and any account of how the condition was
  produced.
