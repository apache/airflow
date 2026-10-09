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



.. _howto/operator:TriggerDagRunOperator:

TriggerDagRunOperator
=======================

Use the :class:`~airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator` to trigger Dag from another Dag.

.. exampleinclude:: /../src/airflow/providers/standard/example_dags/example_trigger_controller_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_trigger_dagrun]
    :end-before: [END howto_operator_trigger_dagrun]

Automatically clearing failed child tasks
-----------------------------------------

By default, when the triggered child Dag run already exists, ``TriggerDagRunOperator`` either
raises, skips, or -- with ``reset_dag_run=True`` -- clears and re-runs the *whole* child run. The
opt-in ``auto_clear_failed_tasks`` flag offers a narrower behavior: when the existing child run is in
a failed state, only its failed tasks (and their downstream) are cleared and re-run, so
already-succeeded upstream tasks are preserved and not re-executed.

The flag is off by default. To enable it, set ``auto_clear_failed_tasks=True`` and run the operator
synchronously (``wait_for_completion=True``). The failed-only clear happens on the next execution of
the operator -- for example on a task retry -- so bound the attempts with ``retries``:

.. code-block:: python

    TriggerDagRunOperator(
        task_id="run_child",
        trigger_dag_id="child_dag",
        wait_for_completion=True,  # sync mode (v1 scope)
        retries=2,  # bounds the auto-clear + retry
        auto_clear_failed_tasks=True,  # on retry, clear failed + downstream of the failed child run
    )

Caveats
~~~~~~~

* **Synchronous-only (v1).** ``auto_clear_failed_tasks`` applies when ``wait_for_completion=True`` and
  is not supported with ``deferrable=True`` -- setting both raises ``ValueError``.
* **Cosmetic window.** Until the operator re-executes and triggers the clear, the child run may still
  be shown as ``failed`` in the UI.
* **Task idempotency.** Previously-succeeded tasks are not re-run and the failed ones are, so the
  triggered Dag's tasks should be idempotent; re-running a non-idempotent task may cause duplicate
  side effects.
* **Precedence with ``reset_dag_run``.** If both are set, ``reset_dag_run`` wins and the whole run is
  cleared; at most one clear is performed.
* **Airflow 3.x core-version requirement.** On Airflow 3.x the failed-only clear is delivered
  server-side via the Execution API and requires a new enough core (Execution API version
  ``2026-11-13`` or later). Against an older core the operator raises ``NotImplementedError`` rather
  than silently falling back to a whole-run clear.
