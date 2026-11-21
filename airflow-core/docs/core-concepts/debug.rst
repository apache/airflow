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


Real-world debugging strategies
-------------------------------

``dag.test()`` and pdb are excellent for failing fast in a single process. In shared
scheduler environments (staging or production-like), teams usually debug against a real
Dag run: inspect what a task wrote, re-run one task after a fix, and compare logs across
tries. The workflows below complement the local approaches above.

Inspect task results and XComs after a run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Trigger the Dag (UI **Trigger DAG**, or ``airflow dags trigger <dag_id>``).
2. Open the Grid / Graph view for that Dag run and select the task instance of interest.
3. Use **XCom** (or the task details panel) to confirm the values the task pushed. If a
   downstream task fails with missing or unexpected data, start here before changing code.
4. Open **Log** for the same task instance and search for the operator's own error lines —
   provider hooks often bury the actionable message after a stack of Airflow internals.

When you already have a ``DagRun`` object from ``dag.test()``, you can also pull XComs in
Python (as in the ``mark_success_pattern`` example above) without opening the UI.

Re-run a single task from the UI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After you change Dag or operator code (and the scheduler has picked up the new version):

1. Open the failed (or suspicious) task instance in the UI.
2. Clear or mark the task for re-run according to your Airflow version (**Clear** /
   **Run** actions on the task instance). Prefer clearing only that task and its
   downstream dependents when you need to re-validate the rest of the path.
3. Watch the new try's log and XComs. If the task succeeds in isolation but fails when
   scheduled normally, compare ``logical_date``, mapped index, and pool/slot availability.

This loop — fix, deploy/sync Dag code, re-run one task — is the usual enterprise debug
cycle when full ``dag.test()`` cannot recreate production connections or data volumes.

Compare tries and isolate environment issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* **Same task, multiple tries:** open each try's log. Failures that disappear on retry
  often point to transient external systems; failures that always fail at the same line
  point to code or bad input XComs.
* **Connections and Variables:** confirm the connection ID used by the operator matches
  what you edited in Admin → Connections. A local ``dag.test()`` may use different
  connection defaults than the worker.
* **Keep the blast radius small:** when testing in a shared environment, use a dedicated
  Dag id or pause sibling Dags so a debug run does not enqueue unrelated work.

These strategies do not replace unit tests or ``dag.test()``. Use local serialized runs to
fail fast, then use UI re-runs and XCom/log inspection to validate behaviour against real
executors, queues, and external systems.
