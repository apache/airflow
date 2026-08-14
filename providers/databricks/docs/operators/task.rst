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
operator, each retry submits a new Databricks run. Inside a
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
