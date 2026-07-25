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



DatabricksRunNowOperator
========================

Use the :class:`~airflow.providers.databricks.operators.DatabricksRunNowOperator` to trigger a run of an existing Databricks job
via `api/2.2/jobs/run-now <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.2/jobs/run-now`` endpoint and pass it directly to our ``DatabricksRunNowOperator`` through the ``json`` parameter.

Another way to accomplish the same thing is to use the named parameters of the ``DatabricksRunNowOperator`` directly.
Note that there is exactly one named parameter for each top level parameter in the ``jobs/run-now`` endpoint.

The only required parameters are either:

* ``job_id`` - to specify ID of the existing Databricks job
* ``job_name`` - Name of the existing Databricks job. It will throw exception if job isn't found, of if there are multiple jobs with the same name.

All other parameters are optional and described in documentation for ``DatabricksRunNowOperator``.  For example, you can pass additional parameters to a job using one of the following parameters, depending on the type of tasks in the job:

* ``notebook_params``
* ``python_params``
* ``python_named_params``
* ``jar_params``
* ``spark_submit_params``
* ``idempotency_token``
* ``repair_run``
* ``cancel_previous_runs``

Forwarding Airflow Dag params as Databricks job parameters
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Databricks ``api/2.2/jobs/run-now`` endpoint accepts a top-level `job_parameters
<https://docs.databricks.com/api/workspace/jobs/runnow#job_parameters>`_ field — a plain
``Dict[str, str]`` mapping parameter name to value — that overrides the job's defaults
for this run.

If ``job_parameters`` is not set in ``json`` and the operator's ``params`` dict is
non-empty, ``params`` is forwarded as ``job_parameters`` as-is, so Airflow Dag params can
be passed dynamically to a run without hardcoding them in ``json``. If ``json`` already
contains ``job_parameters``, it is left untouched.

.. code-block:: python

  run_now = DatabricksRunNowOperator(
      task_id="run_now",
      job_id=123,
      params={"env": "staging", "batch_size": "42"},
  )
  # The triggered run receives:
  #   job_parameters={"env": "staging", "batch_size": "42"}
  # i.e. the same dict, passed straight through to the run-now request body.


Durable execution
^^^^^^^^^^^^^^^^^

``DatabricksRunNowOperator`` triggers a run of an existing job and then polls it to completion on
the worker. By default the operator runs in a *durable* mode that makes this crash-safe: the
Databricks run id is persisted to :doc:`task state store
<apache-airflow:core-concepts/task-state-store>` before polling begins, so if the worker crashes
or is preempted and the task is retried, the operator reconnects to the run that is already
executing on Databricks instead of triggering a duplicate run of the same job.

On retry the operator checks the prior run's state:

* if it is still running, the operator reconnects and continues polling
* if it already succeeded, the operator returns immediately without triggering a new run
* if it failed terminally, or the run no longer exists because its history expired, the operator
  triggers a fresh run

This avoids triggering the same Databricks job twice when a worker is lost mid-run, which is common
for long-running jobs.

Durable execution requires Airflow 3.3 or newer, since it relies on the task state store. On earlier
Airflow versions the flag is a no-op and the operator always triggers a fresh run on retry,
exactly as before. If the task state store is unavailable at runtime, the operator logs that crash
recovery is disabled and behaves the same way.

To opt out and always trigger a fresh run on retry, set ``durable=False``:

.. code-block:: python

  run_now = DatabricksRunNowOperator(
      task_id="run_now",
      job_id=123,
      durable=False,
  )

Durable execution applies to the synchronous path. When ``deferrable=True`` is set, the Triggerer
already tracks the run across the wait, so deferrable mode takes precedence and ``durable`` has no
effect.


DatabricksRunNowDeferrableOperator
==================================

Deferrable version of the :class:`~airflow.providers.databricks.operators.DatabricksRunNowOperator` operator.

It allows to utilize Airflow workers more effectively using `new functionality introduced in Airflow 2.2.0 <https://airflow.apache.org/docs/apache-airflow/2.2.0/concepts/deferring.html#triggering-deferral>`_
