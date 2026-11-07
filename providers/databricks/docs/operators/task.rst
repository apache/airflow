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

.. _howto/operator:DatabricksTaskOperator:


DatabricksTaskOperator
======================

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksTaskOperator` to launch and monitor
task runs on Databricks as Airflow tasks. This can be used as a standalone operator in a Dag and as well as part of a
Databricks Workflow by using it as an operator(task) within the
:class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`.



Examples
--------

Running a notebook in Databricks using DatabricksTaskOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_task_notebook]
    :end-before: [END howto_operator_databricks_task_notebook]

Running a SQL query in Databricks using DatabricksTaskOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_task_sql]
    :end-before: [END howto_operator_databricks_task_sql]

Configuring Databricks-native task retries
-------------------------------------------

Use ``max_retries``, ``min_retry_interval_millis`` and ``retry_on_timeout`` to configure
`Databricks-native task retries <https://docs.databricks.com/api/workspace/jobs/create#tasks-max_retries>`_.
Databricks reruns failed task attempts within the same job run, so Airflow sees only the final result.
Set ``max_retries`` to ``-1`` to retry indefinitely, or ``0`` to disable retries.

These settings are independent of the Airflow task-level ``retries`` parameter, which retries the
whole Airflow task. You can set the same fields directly in ``task_config``. When both are set, the
operator parameter takes precedence. If a field is unset, Databricks uses its default.

Airflow ``retries`` behaves differently depending on where the operator runs. For a standalone
operator, a retry reconnects to the run the previous try submitted only when that try was lost
while the run was still going; a run that already reached a terminal failure on Databricks is
replaced by a fresh submit (see `Durable execution`_ below). Inside a
:class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`,
the Airflow task monitors a sub-run that was already submitted by the workflow launch task, so a
retry only re-polls the terminal sub-run. Use ``max_retries`` to retry Databricks work inside a
workflow task group.

Inside a
:class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`,
a task that exhausts a finite ``max_retries`` is reported as failed as soon as its final failed
attempt is observed, so downstream failure handling is not delayed by long-running sibling tasks.
Only unlimited retries (``max_retries=-1``) keep the Airflow task waiting (or deferring) until the
parent workflow run reaches a terminal state, because Databricks may still launch a retry attempt
under the same ``task_key`` until then. Sibling tasks in the run continue independently.


Durable execution
-----------------

Outside a
:class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`,
this operator submits a run and then waits for it, on the worker or in the Triggerer when
``deferrable=True``. By default it runs in a *durable* mode: the Databricks run id is persisted to
:doc:`task state store <apache-airflow:core-concepts/task-state-store>` before the wait begins, so if
the worker crashes or is preempted and the task is retried, the operator reconnects to the run that
is already executing on Databricks instead of submitting a duplicate.

On retry the operator checks the prior run's state:

* if it is still running, the operator reconnects and continues polling
* if it already succeeded, the operator returns immediately without resubmitting
* if it failed terminally, the operator submits a fresh run

Pass ``durable=False`` to always submit a fresh run on retry.

Unlike :class:`~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` and
:class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator`, where
``durable`` has no effect in deferrable mode, durable execution here also covers
``deferrable=True``: the run id is stored before the task defers, so a try lost while the Triggerer
was waiting still reconnects on the next one.

Durable execution requires Airflow 3.3 or newer, since it relies on the task state store. On
earlier Airflow versions the flag is a no-op and the operator always submits a fresh run on retry.
If the task state store is unavailable at runtime, the operator logs that crash recovery is
disabled and behaves the same way.

Inside a ``DatabricksWorkflowTaskGroup`` nothing changes: the run is submitted by the workflow
launch task and this operator only monitors its own task within that shared run, so a retry already
reconnects rather than resubmitting.

The stored run id is not deleted automatically; that only happens when someone runs
``airflow state-store clean``. If a task's ``retry_delay`` is longer than
``[state_store] default_retention_days`` (30 days by default) and cleanup runs in between, the run
id is gone by the next retry and the operator submits a fresh run instead of reconnecting. Avoid
running cleanup on a schedule shorter than your longest ``retry_delay``. See
:doc:`apache-airflow:administration-and-deployment/task-and-asset-state-store-cleanup`.

Clearing a task is treated the same as a retry, which matters for a task whose run already
succeeded: clearing does not delete the stored run id, so the next attempt reads it back and
returns without submitting a new run. See
:doc:`apache-airflow:core-concepts/resumable-tasks` for why, and for the
``[state_store] clear_on_success`` setting that restores "clearing always resubmits."
