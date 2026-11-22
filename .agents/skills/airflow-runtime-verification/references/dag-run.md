<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# A Dag run, from the file to the result

A user writes a Dag with `airflow.sdk`, saves it in the Dags folder, sees it parsed, triggers it and
reads the outcome: the run state, the task states, the XCom values and the task logs. Most changes
to the scheduler, the Dag processor, the executor, the Task SDK supervisor and task runner, and the
Execution API show here.

The instance from [`SKILL.md`](../SKILL.md) is up and healthy. `<web>` is its port, `verify-<run>`
its compose project, `<dag_id>` the id of the Dag under test.

## Write the Dag

Save it in `files/dags/`, for example as `files/dags/verify_probe.py`. Use the changed behaviour in
the smallest realistic way and print the value the change affects, with a marker word that is easy
to find in the log:

```python
from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None)
def verify_probe():
    @task
    def emit() -> str:
        return "probe-ok"

    @task
    def check(value: str) -> None:
        print(f"VERIFY received {value}")

    check(emit())


verify_probe()
```

## Parse, trigger, wait

1. Wait until the Dag is parsed. The Dag processor looks for new files every 5 seconds:

   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' http://localhost:<web>/api/v2/dags/<dag_id>
   ```

   `200` means parsed. If it is still `404` after half a minute, read
   `curl -s http://localhost:<web>/api/v2/importErrors`: a file that fails to import is listed there
   with its traceback.

2. Unpause and trigger. A new Dag starts paused, and a run of a paused Dag stays `queued` until
   the Dag is unpaused.

   ```bash
   curl -s -X PATCH -H 'Content-Type: application/json' \
     http://localhost:<web>/api/v2/dags/<dag_id> -d '{"is_paused": false}'
   curl -s -X POST -H 'Content-Type: application/json' \
     http://localhost:<web>/api/v2/dags/<dag_id>/dagRuns -d '{"logical_date": null}'
   ```

   The second response holds `dag_run_id`. Put it in double quotes wherever a later command uses
   it. A body without `logical_date` is rejected with `422`. Add `"conf": {...}` to pass run
   configuration.

3. Poll the run until `state` is `success` or `failed`:

   ```bash
   curl -s "http://localhost:<web>/api/v2/dags/<dag_id>/dagRuns/<dag_run_id>"
   ```

## Read the result

- Task states and try numbers: `GET .../dagRuns/<dag_run_id>/taskInstances`.
- XCom: `GET .../dagRuns/<dag_run_id>/taskInstances/emit/xcomEntries/return_value` returns
  `probe-ok` in the `value` field.
- Task log, one per try:

  ```bash
  curl -s -H 'Accept: application/json' \
    "http://localhost:<web>/api/v2/dags/<dag_id>/dagRuns/<dag_run_id>/taskInstances/check/logs/1" \
    | grep -o 'VERIFY received probe-ok'
  ```

- Second surface, the CLI:

  ```bash
  breeze exec --project-name verify-<run> -- \
    -c "AIRFLOW__CORE__LOAD_EXAMPLES=False airflow dags list-runs <dag_id> -o plain"
  ```

## Variables, Connections and retries

Create a Variable and a Connection through the public API. Both calls return `201`:

```bash
curl -s -X POST -H 'Content-Type: application/json' http://localhost:<web>/api/v2/variables \
  -d '{"key": "verify_key", "value": "from-the-api"}'
curl -s -X POST -H 'Content-Type: application/json' http://localhost:<web>/api/v2/connections \
  -d '{"connection_id": "verify_conn", "conn_type": "http", "host": "example.invalid", "port": 8443}'
```

Read them inside a task and assert on the printed line in the task log. Add tasks like these to a
Dag; the third fails on its first try only, which shows a retry:

```python
from datetime import timedelta

from airflow.sdk import Connection, Variable, get_current_context, task


@task
def read_variable() -> None:
    print(f"VERIFY variable={Variable.get('verify_key')}")


@task
def read_connection() -> None:
    connection = Connection.get("verify_conn")
    print(f"VERIFY connection={connection.host}:{connection.port}")


@task(retries=1, retry_delay=timedelta(seconds=1))
def fail_first_try() -> None:
    try_number = get_current_context()["ti"].try_number
    print(f"VERIFY try={try_number}")
    if try_number == 1:
        raise RuntimeError("first try fails")
```

For the retried task, `.../taskInstances/fail_first_try/logs/1` holds the failed try and `logs/2`
the next one. Without `retry_delay` the retry waits 5 minutes.

## `airflow dags test`

```bash
breeze exec --project-name verify-<run> -- \
  -c "AIRFLOW__CORE__LOAD_EXAMPLES=False airflow dags test <dag_id>"; echo "exit_code=$?"
```

The exit code is `0` when the Dag run succeeds and `1` when it fails. What a task prints appears as
a line without a prefix between the log lines. The command also runs a paused Dag.

`dags test` runs the tasks in one process and calls the Execution API routes inside that process.
Under the scheduler a task goes through the executor and a supervisor process, and reaches the
Execution API on the API server. A change to the executor, the supervisor or the way a task talks to
the API server needs the triggered run above, not only `dags test`.

`dags test` also writes a Dag run to the database of the instance. Its run id looks like a manual
run; the `triggered_by` field is `test`. A run triggered with the `curl` call above has `rest_api`,
one triggered in the UI has `ui`. Filter on that field before counting runs or reading the latest
one.
