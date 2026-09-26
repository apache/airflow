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

# Publication recovery and source-scan experiment

The prototype now recovers local publication failures when the worker can confirm
that every importer it started has exited. Continuous execution also skips an unused
source-table scan. The experiment does not establish a reliable performance gain:
the executor's median completion improved by 2.1%, but the unchanged manager control
improved by 1.8% and the executor timing ranges overlap.

## Recovery behavior

The worker returns its execution identity with a publication failure only after
observing importer exit. The current local runner uses that evidence to atomically
retire unfinished attempts and release their reservation. Accepted results remain
accepted, including a result whose acknowledgment was lost. Unaccepted definitions,
including claimed definitions that never started importing, become eligible after
the orchestrator's existing retry backoff.

Unknown claims, another execution's ownership, possible surviving importers and
submissions inherited after restart remain reserved. Elapsed deadlines and generic
executor failures do not establish termination. This change does not implement
remote termination proof or automatic recovery after a host failure.

## Measurement corrections

The benchmark saves the base revision, tracked patch, changed Python sources and
measurement drivers. It verifies captured source hashes after each sample, remembers
descendants for cleanup after parent exit, and excludes resource measurements affected
by forced cleanup or shutdown escalation. Full-inventory coverage and aggregate
acceptance rates are separate metrics.

The profiler now uses container-local runtime storage, identifies importer children
separately from workers, and records process CPU deltas. Sampled stacks include
waiting threads and are not CPU seconds. Earlier CPU-saving claims are withdrawn in
the [revised plan](improvement-plan-2026-09-25.md).

## Source-scan comparison

Both variants include the recovery correction. The only production difference is
skipping `get_sources()` when `max_runs == -1`; finite invocation accounting remains
unchanged. The regression test fails before this change and passes afterward.

Each sample uses 100 Python definitions, 20 tasks per Dag, two parsing slots, and
five accepted parses per definition. Both commands run continuously against fresh
container-local SQLite databases in Breeze, using Python 3.10 and a 20 ms observer.
Fixture creation, migration and artifact copying are outside timing. No tests or
profilers ran concurrently with the timed samples. The host remains shared.

Values below are medians; parentheses show observed ranges, not confidence intervals.

| Mode | Before completion, seconds | After completion, seconds | Completed samples, before / after |
| --- | --- | --- | --- |
| Existing manager | 43.32 (41.99–45.45) | 42.53 (40.74–44.27) | 3 / 4 |
| Executor prototype | 66.12 (65.80–67.62) | 64.70 (63.46–66.89) | 3 / 3 |

| Executor metric | Before | After |
| --- | --- | --- |
| First complete inventory, seconds | 31.22 | 30.28 |
| Repeat inventory coverage, definitions/s | 11.09 | 12.00 |
| Aggregate repeat acceptances/s | 11.28 | 12.00 |
| Whole-run CPU, seconds | 89.55 | 86.97 |
| Peak summed process RSS, MiB | 1067.3 | 1067.6 |

Repeat coverage excludes the first complete inventory. Its observed ranges also
overlap: 10.99–11.66 before and 10.62–12.05 after. CPU includes startup and shutdown;
summed RSS counts shared pages repeatedly. The commands perform unequal total work:
the manager accepts many more extra parses before the slowest definition reaches
five. These resource totals are not equal-work efficiency measurements.

All 13 completed samples passed serialized-result checks. All six completed executor
samples had matching receipts and no remaining admissions. One baseline manager
sample needed shutdown escalation; its CPU and RSS are excluded from resource
summaries, while its completed parse timing is retained.

The original third optimized executor sample crossed a six-hour wall-clock gap
during shutdown, consistent with host sleep/resume. The driver timed out and killed
the process tree. Its database had reached five parses per definition but retained
two submitted reservations after forced shutdown. The entire sample is excluded;
its logs and database are preserved. A fresh manager/executor pair completed after
the interruption. All completed controls are retained, hence four manager samples
after the change. This interruption further limits causal performance conclusions.

Keep the scan removal because it eliminates demonstrably unused work. The prototype
still trails the manager locally. Larger performance claims need longer experiments,
realistic imports and a comparison that controls startup and accepted work.

## Validation and evidence

- Expanded recovery and measurement regression: **243 passed**.
- Follow-up manager/orchestrator regression, including the real CLI restart and
  reparsing integration: **99 passed**. Counts overlap the earlier run.
- Repository static checks, including core and development mypy: passed.
- Corrected profiler smoke: passed with all five long-lived processes identified,
  importer samples separated, measured CPU deltas, 23 acceptances during the
  five-second recording, and no forced cleanup. This validates the tool, not a
  performance conclusion. [Profile evidence](benchmarks/profile-smoke-20260926/profile-summary.json).
- [Before samples](benchmarks/scan-before-20260925/samples.json),
  [after samples](benchmarks/scan-after-20260925/samples.json), and
  [replacement pair](benchmarks/scan-after-confirm-20260926/samples.json).
- Each measurement directory contains `environment.json`, `source-state.json`,
  `source.patch`, source snapshots and command logs. All use base commit
  `49572f28d5de099259667fe9bb26f246f27b9ca2` plus the captured patch.
- Test logs: `recovery-measurement-regression.log`,
  `scan-regression-before.log`, `scan-recovery-cli-regression.log`, and
  `recovery-scan-final-static.log`.

To measure the current worktree, first capture its tracked patch on the host, then
run the driver in Breeze. Use a new output directory for each run. The cached local
image used here additionally needs the repository, image-check and Python options
recorded in the command logs.

```sh
git diff --binary HEAD > files/dag-parsing-aip/current-source.patch
breeze run uv run --no-project dev/dag_parsing_poc/benchmark.py \
  --revision "$(git rev-parse HEAD)" \
  --source-patch /files/dag-parsing-aip/current-source.patch \
  --output /files/dag-parsing-aip/benchmarks/current-scan \
  --definitions 100 --tasks 20 --parallelism 2 \
  --cycles 5 --repetitions 3 --sample-interval 0.02
```

Generated-by: Codex (GPT-6).
