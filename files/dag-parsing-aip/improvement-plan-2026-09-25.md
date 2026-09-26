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

# Improving executor parsing: recovery and measurement

The measurement corrections, local publication-failure recovery and unused-source-scan
removal are implemented. See the [validation and experiment report](recovery-and-scan-20260926.md).
The remaining candidates below are separate future experiments.

The core remains an M0 prototype. The local benchmark measures its current command,
including discovery, startup and publication. That cost matters even if a later
deployment reuses an already-running API. Report cold startup and repeated coverage
separately; neither a small local case nor a remote case replaces the other.

The reverted tier-1 and tier-2 experiments are exploratory history. Their raw results
and exact source variants were not retained here, so their apparent gains and losses
are not acceptance criteria. The earlier profile also used mounted runtime storage
and inferred CPU cost from wall-time samples. It cannot establish the quoted CPU
savings or isolate profiler overhead.

## 1. Make measurements reproducible

- Use the same container-local runtime storage in the benchmark and profiler.
  Copy evidence to the mounted output directory after timing.
- Save the base revision, tracked source patch, changed Python sources and untracked
  measurement drivers. Detect source edits during a run.
- Clean up remembered descendants even after the parent exits. Mark forced cleanup
  and shutdown escalation; exclude affected resource samples while retaining valid
  pre-shutdown completion measurements.
- Distinguish first full coverage, repeat full-inventory coverage and aggregate
  acceptances per second. They answer different questions.
- Record per-process CPU deltas. Sampled stacks include native waits and multiple
  threads; separate importer samples from worker samples and do not label them CPU time.
- Keep parsing slots, storage, fixtures and observer settings matched between runs.
  Record the prototype's 0.1-second interval floor and disabled import preloading.
  Treat changes to those behaviors as separate experiments.

Run both modes with identical resource limits. Begin with the existing local fixture,
then add longer runs, realistic imports and higher parallelism for both modes.
Remote execution needs its own comparison. No scenario is assumed to win.

## 2. Recover publication failures without guessing termination

A generic executor failure or elapsed deadline is not proof that an importer stopped.
The current host aborts after an unresolved workload's deadline; it does not release
that workload's durable reservation.

The narrow improvement is local publication failure after observed importer exit.
Have the worker preserve its execution identity when returning that evidence through
the executor. The current local runner can atomically retire unaccepted attempts,
preserve accepted results and let the orchestrator retry after its existing backoff.
This includes lost acknowledgments: an already committed receipt must survive recovery.

Uncertain claims, another execution's ownership, possible surviving importers and
submissions inherited after restart remain reserved. Do not generalize the local
evidence to Celery or Kubernetes. Those routes still need their own termination proof.

Validate rejection before commit, lost acknowledgments after commit, partial batches,
stale publication after retirement, retry backoff, ownership mismatch and uncertain
termination. Keep the existing claim, deadline and recovery regression tests.

## 3. Reintroduce one small optimization

Start by skipping `get_sources()` when `max_runs == -1`. That scan computes eligibility
for finite invocations; continuous admission does not use it. Preserve finite-run
counts and discovery behavior, add a regression test, and compare the same identified
source revision with and without that change.

Next candidates are read transactions for pure getters and less frequent saturation
logging. Measure each independently. Connection pooling needs transaction rollback,
concurrency and process-lifetime tests. WAL and synchronous durability settings are
separate choices; do not weaken durability as part of a connection-reuse optimization.

## 4. Keep structural changes separate

Respect the configured process-start method. If fork/preloading is tested, select it
at the dedicated process entry point, preserve the alternatives and test them.
Do not mutate the caller's multiprocessing context inside `_run_executor`.

Batch claims may reduce round trips, but need atomic fencing and acknowledgment
recovery tests. Overlapping result publication needs bounded outstanding results,
joined shutdown and explicit failure semantics; acceptance must remain durable.

Completing one definition does not free a worker that is still executing the same
batch. Try smaller or adaptive batches before redesigning capacity accounting.
Keep capacity reserved until the work it covers is known to have stopped.

Use notifications or bounded polling for handoff improvements. Avoid a busy loop.
Profile the normal manager under the same conditions before treating shared
serialization costs as specific to executors.

Generated-by: Codex (GPT-6).
