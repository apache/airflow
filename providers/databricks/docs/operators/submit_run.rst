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



.. _howto/operator:DatabricksSubmitRunOperator:


DatabricksSubmitRunOperator
===========================

Use the :class:`~airflow.providers.databricks.operators.DatabricksSubmitRunOperator` to submit
a new Databricks job via Databricks `api/2.2/jobs/runs/submit <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_ API endpoint.


Using the Operator
------------------

There are three ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.2/jobs/runs/submit`` endpoint and pass it directly to our ``DatabricksSubmitRunOperator`` through the
``json`` parameter.  With this approach you get full control over the underlying payload to Jobs REST API, including
execution of Databricks jobs with multiple tasks, but it's harder to detect errors because of the lack of the type checking.

.. code-block:: python

  json = {
      "new_cluster": {"spark_version": "2.1.0-db3-scala2.11", "num_workers": 2},
      "notebook_task": {
          "notebook_path": "/Users/airflow@example.com/PrepareData",
      },
  }
  notebook_run = DatabricksSubmitRunOperator(task_id="notebook_run", json=json)

The second way to accomplish the same thing is to use the named parameters of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
one named parameter for each top level parameter in the ``runs/submit`` endpoint.  When using named parameters you must to specify following:

* Task specification - it should be one of:

  * ``spark_jar_task`` - main class and parameters for the JAR task
  * ``notebook_task`` - notebook path and parameters for the task
  * ``spark_python_task`` - python file path and parameters to run the python file with
  * ``spark_submit_task`` - parameters needed to run a ``spark-submit`` command
  * ``pipeline_task`` - parameters needed to run a Delta Live Tables pipeline
  * ``dbt_task`` - parameters needed to run a dbt project

* Cluster specification - it should be one of:
  * ``new_cluster`` - specs for a new cluster on which this task will be run
  * ``existing_cluster_id`` - ID for existing cluster on which to run this task

* ``pipeline_task`` - may refer to either a ``pipeline_id`` or ``pipeline_name``

In the case where both the json parameter **AND** the named parameters
are provided, they will be merged together. If there are conflicts during the merge,
the named parameters will take precedence and override the top level ``json`` keys.

Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
    - ``spark_jar_task``
    - ``notebook_task``
    - ``spark_python_task``
    - ``spark_submit_task``
    - ``pipeline_task``
    - ``dbt_task``
    - ``git_source``
    - ``new_cluster``
    - ``existing_cluster_id``
    - ``libraries``
    - ``run_name``
    - ``timeout_seconds``

.. code-block:: python

  new_cluster = {"spark_version": "10.1.x-scala2.12", "num_workers": 2}
  notebook_task = {
      "notebook_path": "/Users/airflow@example.com/PrepareData",
  }
  notebook_run = DatabricksSubmitRunOperator(
      task_id="notebook_run", new_cluster=new_cluster, notebook_task=notebook_task
  )

Another way to do is use the param tasks to pass array of objects to instantiate this operator. Here the value of tasks param that is used to invoke ``api/2.2/jobs/runs/submit`` endpoint is passed through the ``tasks`` param in ``DatabricksSubmitRunOperator``. Instead of invoking single task, you can pass array of task and submit a one-time run.

.. code-block:: python

  tasks = [
      {
          "new_cluster": {"spark_version": "2.1.0-db3-scala2.11", "num_workers": 2},
          "notebook_task": {"notebook_path": "/Users/airflow@example.com/PrepareData"},
      }
  ]
  notebook_run = DatabricksSubmitRunOperator(task_id="notebook_run", tasks=tasks)


Forwarding Airflow Dag params as task parameters
------------------------------------------------

Unlike ``api/2.2/jobs/create`` and ``api/2.2/jobs/run-now``, the
``api/2.2/jobs/runs/submit`` endpoint has no top-level parameter slot — each task in
``tasks`` carries its own parameters whose shape depends on the task type.

If the operator's ``params`` dict is non-empty, it is forwarded as-is into the
dict-shaped parameter slot of every task in ``json`` whose corresponding field is empty:

* ``notebook_task.base_parameters`` (e.g. for ``notebook_task``)
* ``python_wheel_task.named_parameters``
* ``sql_task.parameters``
* ``run_job_task.job_parameters``

Tasks whose only parameter slot is ``List[str]`` (``spark_jar_task``, ``spark_python_task``,
``spark_submit_task``) are skipped because there is no canonical mapping from a key/value
dict to a positional argument list — pass those parameters explicitly via the ``json``
or ``tasks`` argument.

.. code-block:: python

  notebook_run = DatabricksSubmitRunOperator(
      task_id="notebook_run",
      notebook_task={"notebook_path": "/Users/airflow@example.com/PrepareData"},
      new_cluster={"spark_version": "15.4.x-scala2.12", "num_workers": 2},
      params={"env": "dev", "shard": "1"},
  )
  # The submitted run's notebook_task.base_parameters becomes:
  #   {"env": "dev", "shard": "1"}
  # i.e. the same dict, copied into the task's dict-shaped parameter slot.


Examples
--------

Specifying parameters as JSON
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksSubmitRunOperator is as follows:

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_json]
    :end-before: [END howto_operator_databricks_json]

Using named parameters
^^^^^^^^^^^^^^^^^^^^^^

You can also use named parameters to initialize the operator and run the job.

.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_named]
    :end-before: [END howto_operator_databricks_named]

Durable execution
-----------------

``DatabricksSubmitRunOperator`` submits a run and then polls it to completion on the worker.
By default the operator runs in a *durable* mode that makes the runs crash-safe: the Databricks
run id is persisted to :doc:`task state store <apache-airflow:core-concepts/task-state-store>`
before polling begins, so if the worker crashes or is preempted and the task is retried, the
operator reconnects to the run that is already executing on Databricks instead of submitting a
duplicate.

On retry the operator checks the prior run's state:

* if it is still running, the operator reconnects and continues polling
* if it already succeeded, the operator returns immediately without resubmitting
* if it failed terminally, the operator submits a fresh run

This avoids redundant Databricks job submissions when a worker is lost mid-run, which is common for
long-running jobs.

Durable execution requires Airflow 3.3 or newer, since it relies on the task state store. On
earlier Airflow versions the flag is a no-op and the operator always submits a fresh
run on retry, exactly as before. If the task state store is unavailable at runtime, the
operator logs that crash recovery is disabled and behaves the same way.

To opt out and always submit a fresh run on retry, set ``durable=False``:

.. code-block:: python

  notebook_run = DatabricksSubmitRunOperator(
      task_id="notebook_run",
      notebook_task={"notebook_path": "/Users/airflow@example.com/PrepareData"},
      new_cluster={"spark_version": "15.4.x-scala2.12", "num_workers": 2},
      durable=False,
  )

Durable execution applies to the synchronous path. When ``deferrable=True`` is set, the
Triggerer already tracks the run across the wait, so deferrable mode takes precedence and
``durable`` has no effect.


DatabricksSubmitRunDeferrableOperator
=====================================

Deferrable version of the :class:`~airflow.providers.databricks.operators.DatabricksSubmitRunOperator` operator.

It allows to utilize Airflow workers more effectively using `new functionality introduced in Airflow 2.2.0 <https://airflow.apache.org/docs/apache-airflow/2.2.0/concepts/deferring.html#triggering-deferral>`_
