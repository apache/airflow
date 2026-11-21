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

# Performance harness

A scratch environment for measuring a scheduler or executor-path change under load, inside
breeze. The test starts an API server, Celery workers and a scheduler as its own subprocesses,
creates a backlog of queued Dag runs and measures how long the deployment takes to finish running them,
once with the new code under test and once with the original code. While the Dag runs are active,
metrics are captured which can then be examined in Grafana. Apart from the internal Airflow metrics,
there are usage metrics for the scheduler and the workers which are captured and exported by a
`poll_usage.py` script that runs for as long as the services are running.

The test setup isn't meant to run in the CI. The folder is collected only when its path is named on the pytest command line.

## Running it

Start breeze with Postgres and the two integrations the harness needs, and then run the test:

```bash
breeze shell --backend postgres --integration otel --integration redis
pytest airflow-core/tests/integration/performance/test_perf.py
```

Progress lines are printed in the terminal every 15 seconds, and the elapsed time at the end of each test case.
Without a terminal, for example when an agent runs it, add `-s` so that output is not held back
by pytest's capture.

Watch the run from the host: Grafana at <http://localhost:23000>, dashboard "Scheduler
performance"; Jaeger at <http://localhost:26686>; Prometheus at <http://localhost:29090>.

The first run on a machine raises the Postgres connection limit the profile needs, persists it
in the breeze volume and restarts the container from inside breeze. If the restart is not
possible, the test fails with the command to run on the host.

## Comparing new code with the original

The test runs two test cases, `original_code` and then `new_code`, and sets `PERF_TEST__NEW_CODE` to `false`
or `true` before starting the processes. Put the change in the code under test behind the flag:

```python
if os.environ.get("PERF_TEST__NEW_CODE", "").lower() == "true":
    ...  # new code with changes
else:
    ...  # original code, e.g. code from main
```

Code that is purely additive only needs the first branch of the if-else statement.
The flag belongs to the scratch change:
nothing in the harness depends on it, so drop it together with the rest when you are done.

Run a single test case with `-k original_code` or `-k new_code`.

`AIRFLOW__*` variables exported in the shell do not reach the processes, because the test plugin
strips them. Pass `--keep-env-variables` to pytest to keep them.

## Shaping the load

The test function chooses its load at its top, in `test_perf.py`. Edit the numbers there, or
override one for a single run with its `PERF_TEST__<NAME>` variable in the shell. The profile is the one
class-level choice, because it sizes the deployment before the database is created.

| Setting               | Default   | Override                   | Effect                                                                 |
|-----------------------|-----------|----------------------------|------------------------------------------------------------------------|
| `dag_run_count`           | `5`       | `PERF_TEST__DAG_RUN_COUNT`           | Queued runs created up front, before the scheduler starts.             |
| `mapped_task_count`   | `20`      | `PERF_TEST__MAPPED_TASK_COUNT`   | Mapped instances per stage, passed to every run in its `conf`.         |
| `timeout_seconds`     | `3600`    | `PERF_TEST__TIMEOUT_SECONDS`     | Give up on a test case after this; unfinished runs compare with nothing.   |
| `profile`             | `dynamic` | `PERF_TEST__PROFILE`             | Concurrency profile, see below.                                        |

The Dag is fixed: a first task producing a list, then nine mapped stages that expand over it,
each depending on the whole previous stage. Only the size of a run changes, through its `conf`.
The total mapped task instances per test case are `dag_run_count * 9 * mapped_task_count`.

To adjust:

- The load: edit the numbers at the top of the test function, or override them in the shell.
- The Dags, the runs or their `conf`: add a test function that picks them and calls
  `execute_dag_runs`.
- The shape: add a Dag file in `dags/` that reads its sizes from `params`. The class serializes
  every Dag in the folder. Keep task bodies empty, or the run turns into a worker throughput test.

## Profiles

`dynamic` checks the available CPU cores and sets the required configs based on the machine size.

`custom` is the `custom` entry of `PROFILES` in `test_perf.py`. Fill in what the machine or the experiment needs
and select it with `profile = "custom"` or `PERF_TEST__PROFILE=custom`.
The current values set all configs at their Airflow default.
