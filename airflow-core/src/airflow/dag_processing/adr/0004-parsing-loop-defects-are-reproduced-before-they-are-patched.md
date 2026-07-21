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
to shared tables, often with several processors on one database. Its characteristic
failures — a duplicate-key error surfacing as a spurious import error, a wrongly
stale Dag, a callback that seems to lose context, a clone racing a clone — all look,
from a trace or screenshot, like a missing guard at the line that raised.

They usually are not. This subsystem already commits parse results at an outer
boundary wrapped in database retry logic that turns a losing race into a no-op on
the next attempt. A patch to the inner function is therefore often a patch to a line
that never raises in production, guarding a condition handled one frame out — it
reads fine, passes a unit test built from the same misunderstanding, and changes
nothing. One contributor proved this cleanly: suspecting a two-processor
duplicate-key race from a red UI banner, they stood the race up on `main` against
real PostgreSQL, found the error fired at the outer commit where retry already
absorbed it, and closed their own PR. No reviewer could have reached that from the
diff. The inverse holds too — a reviewer disputing a report is expected to reproduce,
as one did by running two same-identifier Dags in Breeze and posting the import
error and processor log. The cost of the alternative is concentrated: every
unreproduced parsing-loop patch consumes expert review time in the area with the
fewest experts, and enough arrived — much of it unreviewed generated code — to drive
the project's generative-AI contribution guidance.

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

- Scarce expert review effort is spent on changes known to do something.
- A repeated failure to reproduce is itself a useful finding — the suspected layer
  is already correct — and belongs in the PR discussion.
- The bar is high: standing up multiple processors against PostgreSQL is more work
  than writing most of these patches.

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

- #66788 — closed by its own author; the two-processor race on real PostgreSQL fired the `IntegrityError` at the outer `update_dag_parsing_results_in_db` commit, not the patched function, where the retry wrapper already no-ops it.
- #60761 — a reviewer reproduced the duplicate-identifier scenario in Breeze and posted the import error and processor log, contradicting the premise.
- #60013, #60100, #60136 — unreviewed generated changes, closed; descriptions did not match the diffs.
- #61680 — another unreproduced parsing-path patch from the same wave that drove the generative-AI contribution guidance.
- #66820 — positive scheduler-side example: the author stood up 2-3 real schedulers on MySQL 8.0 under load, found no deadlock (both fetches use `FOR UPDATE SKIP LOCKED`, SQL identical), and closed their own PR.
- #56710 — `KeyError` patch drafted back for lacking tests and any account of how the condition arose.
