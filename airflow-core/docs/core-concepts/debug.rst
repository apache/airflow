 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _concepts:debugging:

Debugging Airflow Dags
======================

Testing Dags with dag.test()
-----------------------------

To debug Dags in an IDE, you can set up the ``dag.test`` command in your Dag file and run through your Dag in a single
serialized python process.

This approach can be used with any supported database (including a local SQLite database) and will
*fail fast* as all tasks run in a single process.

``dag.test()`` executes a real Dag run, so it has two prerequisites:

* an initialized metadata database (``airflow db migrate``), and
* a Dag that Airflow can serialize, which means the Dag must be defined in a file inside a
  configured :doc:`Dag bundle </administration-and-deployment/dag-bundles>` — by default, your
  Dags folder. ``dag.test()`` re-parses the bundle and serializes the Dag for you.

A Dag built in memory — for instance inside a pytest function, in a file outside your Dags folder —
is not in any bundle, so nothing gets serialized and the call fails with
``Cannot create DagRun for DAG <dag_id> because the dag is not serialized``. To test operator
behaviour without a Dag run at all, call the operator directly as described in
:ref:`Unit tests <best_practices:unit_tests>`.

To set up ``dag.test``, add these two lines to the bottom of your Dag file:

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

and that's it! You can add optional arguments to fine tune the testing but otherwise you can run or debug Dags as
needed. Here are some examples of arguments:

* ``logical_date`` if you want to test argument-specific Dag runs
* ``use_executor`` if you want to test the Dag using an executor. By default ``dag.test`` runs the Dag without an
  executor, it just runs all the tasks locally.
  By providing this argument, the Dag is executed using the executor configured in the Airflow environment.

Conditionally skipping tasks
----------------------------

If you don't wish to execute some subset of tasks in your local environment (e.g. dependency check sensors or cleanup steps),
you can automatically mark them successful supplying a pattern matching their ``task_id`` in the ``mark_success_pattern`` argument.

In the following example, testing the Dag won't wait for either of the upstream Dags to complete. Instead, testing data
is manually ingested. The cleanup step is also skipped, making the intermediate csv is available for inspection.

.. code-block:: python

  with DAG("example_dag", default_args=default_args) as dag:
      sensor = ExternalTaskSensor(task_id="wait_for_ingestion_dag", external_dag_id="ingest_raw_data")
      sensor2 = ExternalTaskSensor(task_id="wait_for_dim_dag", external_dag_id="ingest_dim")
      collect_stats = PythonOperator(task_id="extract_stats_csv", python_callable=extract_stats_csv)
      # ... run other tasks
      cleanup = PythonOperator(task_id="cleanup", python_callable=Path.unlink, op_args=[collect_stats.output])

      [sensor, sensor2] >> collect_stats >> cleanup

  if __name__ == "__main__":
      ingest_testing_data()
      run = dag.test(mark_success_pattern="wait_for_.*|cleanup")
      print(f"Intermediate csv: {run.get_task_instance('collect_stats').xcom_pull(task_ids='collect_stats')}")


Debugging Airflow Dags on the command line
------------------------------------------

With the same two line addition as mentioned in the above section, you can now easily debug a Dag using pdb as well.
Run ``python -m pdb <path to Dag file>.py`` for an interactive debugging experience on the command line.

.. code-block:: bash

  [Breeze:3.11.16] root@ef2c84ad4856:/opt/airflow# python -m pdb providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py
  > /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py(18)<module>()
  -> """Example Dag demonstrating the usage of the BashOperator."""
  (Pdb) b 45
  Breakpoint 1 at /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py:45
  (Pdb) c
  > /opt/airflow/providers/standard/src/airflow/providers/standard/example_dags/example_bash_operator.py(45)<module>()
  -> task_id="run_after_loop",
  (Pdb) run_this_last
  <Task(EmptyOperator): run_this_last>

**IDE setup steps:**

1. Add ``main`` block at the end of your Dag file to make it runnable.

.. code-block:: python

  if __name__ == "__main__":
      dag.test()

2. Run / debug the Dag file.

Debugging Dags in a running deployment
--------------------------------------

``dag.test()`` covers local development. Once a Dag runs in a shared deployment, most debugging
starts from a failed or misbehaving task instance. The techniques below work from the UI and from
the ``airflow`` CLI on any machine that can reach the metadata database.

Reading task logs
'''''''''''''''''

Open the failed task instance from the Grid view and go to the **Logs** tab.

* Use the log level filter (**All Log Levels** by default) to show only ``WARNING``, ``ERROR`` or
  ``CRITICAL`` lines. You can select more than one level. The selection is kept in the URL
  (``log_level``), so a filtered link can be shared with a teammate.
* Use **Search logs...** to jump between matches for an error message, a table name or a request
  ID. Press ``l``, ``t`` or ``w`` to show or hide log levels and timestamps, or toggle line wrapping.
* A task that retried has one log per try. Pick the try from **Task Tries** above the log. The
  first try usually has the original error. Later tries can fail on a side effect of it, such as a
  partially written table.

Inspecting XComs
''''''''''''''''

Many failures come from a task receiving the wrong input rather than from its own code. Open the
upstream task instance and check its **XCom** tab to see exactly what it pushed for that run. To
compare values across runs and Dags, use **Browse > XComs**.

For how XComs are pushed and pulled, see :ref:`concepts:xcom`.

Checking Connections and Variables
''''''''''''''''''''''''''''''''''

A task that works in one environment and fails in another usually points at configuration. Check
that the Connection or Variable the task uses exists and resolves to the value you expect:

.. code-block:: bash

  airflow connections get my_postgres
  airflow variables get my_variable

Both commands resolve values the same way tasks do, so they also find Connections and Variables
defined in a secrets backend or in ``AIRFLOW_CONN_*`` and ``AIRFLOW_VAR_*`` environment variables.
**Admin > Connections** and **Admin > Variables** in the UI only list entries stored in the
metadata database.

If ``[core] test_connection`` is set to ``Enabled``, ``airflow connections test my_postgres``, or
**Test Connection** in **Admin > Connections**, checks that Airflow can reach the target system.

Looking at past runs
''''''''''''''''''''

Comparing a failing run with the last good one narrows down what changed. The Grid view shows the
state of every task across recent runs, and the **Runs** tab of a Dag lists them with their run
type and duration. From the CLI:

.. code-block:: bash

  airflow dags list-runs my_dag --state failed --limit 5
  airflow tasks states-for-dag-run my_dag <run_id>

Check whether the failure started with a particular run, a new Dag version or a change in input
data before changing any code.

Re-running a single task
''''''''''''''''''''''''

Once you have a fix, clear only the task that failed instead of triggering a new Dag run. In the UI,
select the task instance and use **Clear Task Instance**. The dialog lets you include
**Downstream** tasks, which you usually want so that results depending on the fixed task are
recomputed, and **Upstream**, **Past** or **Future** task instances when needed. It lists the
affected tasks before you confirm.

The CLI equivalent clears matching task instances over a date range:

.. code-block:: bash

  airflow tasks clear my_dag --task-regex '^load_orders$' --downstream \
      --start-date 2026-01-01 --end-date 2026-01-02

Use **Mark Task Instance as...** instead of clearing when the work was done outside Airflow and you
only need the state to reflect that.

Running one task with ``airflow tasks test``
''''''''''''''''''''''''''''''''''''''''''''

To reproduce a failure without waiting for the scheduler, run a single task in your terminal:

.. code-block:: bash

  airflow tasks test my_dag load_orders <logical_date_or_run_id>

The task runs without checking its dependencies, and its log is printed to your terminal. Useful
options:

* ``--task-params '{"limit": 10}'`` overrides Dag params for this run.
* ``--env-vars '{"MY_FLAG": "1"}'`` sets environment variables while the Dag is parsed and while
  the task runs.
* ``--post-mortem`` opens a debugger at the exception if the task fails.
* ``--map-index`` selects a single mapped task instance.
