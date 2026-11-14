---
name: airflow-perf-harness
description: Measure a scheduler or executor-path change under load with the performance harness in airflow-core/tests/integration/performance. Use when asked to benchmark, load test or compare the performance of a change to the scheduler, the executors or task scheduling. Covers gating the change, shaping the load, running the harness in breeze, reading its dashboard and report, and dropping the scratch changes afterwards.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Measure a change with the performance harness

The harness in [`airflow-core/tests/integration/performance`](../../../airflow-core/tests/integration/performance/README.md)
is a scratch environment, not a test suite: it runs an Airflow deployment inside breeze against a
backlog of runs of a fan-out Dag and shows what the scheduler and the workers cost. Read its README
first; this skill is the workflow around it.

## Workflow

1. **Put the change under the toggle.** The test runs two test cases and sets `PERF_TEST__NEW_CODE` to
   `false` and then `true` before starting the processes. Wrap the change so the `false` test case is
   exactly what `main` does:

   ```python
   if os.environ.get("PERF_TEST__NEW_CODE", "").lower() == "true":
       ...  # the change
   else:
       ...  # the original code, untouched
   ```

   Purely additive code needs only the first branch. Do not add a config option, a metric or a
   registry entry for the toggle; the environment variable is enough and leaves nothing to clean
   up. Read the variable where the change is, not in the harness.

2. **Shape the load.** The top of the test function sets the run count, the mapped instances per
   stage and the time limit; each can be overridden for one run with its
   `PERF_TEST__<NAME>` variable, for example `PERF_TEST__RUN_COUNT=35`. Start small to check the
   plumbing, then scale to what the change is about. The Dag itself is fixed; a different shape is
   a new file in `dags/` that reads its sizes from `params`, with empty task bodies. A different
   load is a new test function that picks its Dag ids, run count and run `conf` and calls
   `execute_dag_runs`; do not change `execute_dag_runs` or the class setup for it.

3. **Run it.** Breeze needs Docker. If breeze cannot run, stop and say which call failed; do not
   build a substitute environment.

   ```bash
   breeze shell --backend postgres --integration otel --integration redis
   # inside the shell
   pytest airflow-core/tests/integration/performance -s
   ```

   `-s` matters without a terminal: it is what shows the progress lines, the report and the
   processes' output. `-k new_code` runs one test case. `AIRFLOW__*` variables from the shell only reach
   the processes with `--keep-env-variables`. The first run on a machine raises the Postgres
   connection limit and restarts the container itself; if it reports that it could not, ask for
   the restart it names and run again.

4. **Read the results.** The headline is `drain_s` in the report printed after each test case: how
   long the deployment took to finish the same backlog with and without the change. The same
   report gives throughput, concurrency and the mean time per phase, overall and per task, one
   row per test case. On the "Scheduler performance"
   dashboard at `http://localhost:23000`:
   - "Utilization" and "CPU by role" say whether the scheduler or the workers were the
     constraint. A scheduler change shows as lower scheduler CPU or busy share at the same
     hand-off rate, or a higher hand-off rate at the same utilization.
   - "Task timeline" splits a task's life into scheduled, queued and running. A scheduler change
     should move the scheduled curve; the other two are the executor and the workers.
   - "Tasks handed off, cumulative" draws one curve per test case; compare their slopes.

   Prometheus scrapes every 30 seconds, so a run needs several minutes of load before the rate
   and mean panels say anything; the report is exact regardless of run length.

   Report both test cases side by side with the load you used. If the mean running time per task
   differs between test cases or climbs during a run, the machine was oversubscribed and the numbers
   are not comparable; lower the load or the profile and run again.

5. **Clean up.** The toggle and any Dag or attribute changes are scratch work. Remove them with
   the rest of the experiment and leave the harness as it was. Never commit a `PERF_TEST__NEW_CODE`
   read.

## Rules

- The harness never runs in CI. Do not remove the gate in `conftest.py`, add the folder to a
  workflow, or mark the test with anything that would select it elsewhere.
- Do not edit the breeze compose files to make a run fit; use the `custom` profile in
  `test_perf.py` instead.
- Do not read the measurement from pytest's pass or fail. The test only fails when a process
  dies, a run fails or the environment is missing; the numbers live in the report and on the
  dashboard.
