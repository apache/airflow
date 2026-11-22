---
name: airflow-performance-harness
description: Measure a scheduler or executor-path change under load with the performance harness in airflow-core/tests/integration/performance. Use when asked to benchmark, load test or compare the performance of a change to the scheduler, the executors or task scheduling. Covers putting the change behind the flag, shaping the load, running the harness in breeze, reading its dashboard, and dropping the scratch changes afterwards.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Measure a change with the performance harness

The harness in [`airflow-core/tests/integration/performance`](../../../airflow-core/tests/integration/performance/README.md)
is a scratch environment, not a test suite. It runs the Airflow processes inside breeze against a
backlog of Dag runs, once with the original code and once with the change, and the comparison is
how long each took plus what the dashboard shows. Read its README first.

## Workflow

1. **Put the change behind the flag.** The test sets `PERF_TEST__NEW_CODE` to `false` for the first
   test case and `true` for the second, before starting the processes. In the code under test:

   ```python
   if os.environ.get("PERF_TEST__NEW_CODE", "").lower() == "true":
       ...  # new code with changes
   else:
       ...  # original code, e.g. code from main
   ```

   Code that is purely additive only needs the first branch of the if-else statement. The
   `else` branch must be exactly what `main` does. Do not add a config option or anything else
   for the flag; the environment variable is enough and leaves nothing to clean up.

2. **Shape the load.** The test function chooses its load at its top, in `test_perf.py`. Edit the
   numbers there, or override one for a single run with its `PERF_TEST__<NAME>` variable in the
   shell. Start small to check the plumbing, then scale to what the change is about. The README
   says how to change the Dags or the runs.

3. **Run it.** Breeze needs Docker. If breeze cannot run, stop and say which call failed; do not
   build a substitute environment.

   ```bash
   breeze shell --backend postgres --integration otel --integration redis
   # inside the shell
   pytest airflow-core/tests/integration/performance/test_perf.py -s
   ```

   Without a terminal, for example when an agent runs it, add `-s` so that output is not held
   back by pytest's capture. Run a single test case with `-k original_code` or `-k new_code`. The
   first run on a machine raises the Postgres connection limit the profile needs, persists it in
   the breeze volume and restarts the container from inside breeze. If the restart is not
   possible, the test fails with the command to run on the host; ask for it and run again.

4. **Read the results.** Progress lines are printed in the terminal every 15 seconds, and the
   elapsed time at the end of each test case; that elapsed time is the headline. The "Scheduler
   performance" dashboard at `http://localhost:23000` says whether the scheduler or the workers
   were the constraint and where a task's time went; a scheduler change should move the
   scheduler's share and leave the workers' alone. Prometheus scrapes every 30 seconds, so a run
   needs several minutes before the rate panels say anything. Report both test cases side by side
   with the load you used, and treat the numbers as invalid if the machine was oversubscribed,
   which shows as the mean running time per task climbing with the load.

5. **Clean up.** The flag belongs to the scratch change: nothing in the harness depends on it, so
   drop it together with the rest when you are done. Never commit a `PERF_TEST__NEW_CODE` read.

The test setup isn't meant to run in the CI. The folder is collected only when its path is named
on the pytest command line; do not change `conftest.py` or any workflow to select it. The test
only fails when a process dies, a run fails or the environment is missing; it is not where the
measurement is read.
