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
breeze. The test starts an API server, Celery workers and a scheduler as its own subprocesses, creates a backlog of queued runs of the Dag in `dags/` and
measures how long the deployment takes to drain it, once per test case of the code under test. The
drain time per test case is the `drain_s` column of the report the test prints after each test case; Grafana
and the usage gauges the sidecar pushes say where the time went. A test case fails if a process dies or a run does
not succeed within the time limit.

It never runs in CI: the folder is collected only when its path is named on the pytest command
line.

## Running it

Start breeze with Postgres and the two integrations the harness needs, then run the test:

```bash
breeze shell --backend postgres --integration otel --integration redis
pytest airflow-core/tests/integration/performance
```

Progress lines reach the terminal every 15 seconds and the per-test case report follows each test case.
Without a terminal, for example when an agent runs it, add `-s` so that output is not held back
by pytest's capture.

Watch the run from the host: Grafana at <http://localhost:23000>, dashboard "Scheduler
performance"; Jaeger at <http://localhost:26686>; Prometheus at <http://localhost:29090>.

The first run on a machine raises the Postgres connection limit the profile needs, persists it
in the breeze volume and restarts the container from inside breeze. If the restart is not
possible, the test fails with the command to run on the host.

## Comparing new code with the original

The test runs two test cases, `original_code` and then `new_code`, and sets `PERF_TEST__NEW_CODE` to `false`
or `true` before starting the processes. Gate the change in the code under test on it:

```python
if os.environ.get("PERF_TEST__NEW_CODE", "").lower() == "true":
    ...  # the change
else:
    ...  # what main does today
```

Code that is purely additive only needs the first branch. Keep the `false` test case identical to
`main`, otherwise the comparison measures the gate. The gate belongs to the scratch change:
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
| `run_count`           | `5`       | `PERF_TEST__RUN_COUNT`           | Queued runs created up front, before the scheduler starts.             |
| `mapped_task_count`   | `20`      | `PERF_TEST__MAPPED_TASK_COUNT`   | Mapped instances per stage, passed to every run in its `conf`.         |
| `timeout_seconds`     | `3600`    | `PERF_TEST__TIMEOUT_SECONDS`     | Give up on a test case after this; a partial drain compares with nothing.   |
| `profile`             | `dynamic` | `PERF_TEST__PROFILE`             | Concurrency profile, see below.                                        |

The Dag is fixed: a first task producing a list, then nine mapped stages that expand over it,
each depending on the whole previous stage. Only the size of a run changes, through its `conf`.
Mapped task instances per test case are `run_count * 9 * mapped_task_count`. Another load is another
test function: it picks its Dag ids, run count and run `conf` and hands them to `execute_dag_runs`, which
does the rest. Another shape is another file in `dags/` that reads its own sizes from `params`;
the class serializes every Dag the folder holds. Keep task bodies empty, or the run turns into a
worker throughput test.

## Profiles

`dynamic` sizes the deployment from the CPUs the container may use so that the scheduler is the
only constraint: three Celery workers with two thirds of the CPUs each as concurrency, API server
workers at a third of the CPUs with a floor of four, and parallelism, pool slots, per-Dag task and
run limits far above what the workers can execute.

`custom` is `CUSTOM_PROFILE` in `test_perf.py`. Fill in what the machine or the experiment needs
and select it with `profile = "custom"` or `PERF_TEST__PROFILE=custom`. Its process counts start at the
tools' own defaults; every Airflow setting left empty keeps Airflow's default.

## What is measured

- The dashboard: scheduler busy share and worker slot occupancy, CPU and resident memory per
  process role, hand-off rate, the mean time a task spends scheduled, queued and running, the
  scheduler loop duration, the pool and executor backlogs, and cumulative hand-offs with one
  curve per test case.
- `poll_usage.py`, started by the test next to the processes, pushes the CPU, memory and Celery
  slot gauges that the utilization panels read.
- `report_run.py`, run by the test after each test case, prints the drain time, throughput, concurrency
  and the mean time per phase, overall and per task, with one row per test case. It can be run by hand as well,
  before the next run resets the database.
- Traces from every process go to Jaeger.

Average running time per task is the tripwire for an oversubscribed machine: if it climbs with
the slot count, every measurement inflates uniformly and the test cases are not comparable.

Prometheus scrapes the collector every 30 seconds. A run that finishes within a couple of scrapes
shows little on the rate and mean panels; give a measurement several minutes of load, and read
the exact per-phase times from the report either way.
