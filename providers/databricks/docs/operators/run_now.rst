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


DatabricksRunNowDeferrableOperator
==================================

Deferrable version of the :class:`~airflow.providers.databricks.operators.DatabricksRunNowOperator` operator.

It allows to utilize Airflow workers more effectively using `new functionality introduced in Airflow 2.2.0 <https://airflow.apache.org/docs/apache-airflow/2.2.0/concepts/deferring.html#triggering-deferral>`_

.. _howto/operator:DatabricksRunNowDeferrableOperator:retry-args:

Retry args in deferrable mode
-----------------------------

When ``deferrable=True``, the ``databricks_retry_args`` dictionary is serialized across the
trigger boundary and must contain only Airflow-serializable values (plain Python primitives
such as ``int``, ``float``, ``str``, ``bool``, ``None``, ``dict``, and ``list``).

**Supported** (serialization-safe):

.. code-block:: python

    # Integer / float primitives
    databricks_retry_args = {"stop_after_attempt": 3, "wait_fixed": 30}

    # Nested plain-dict form
    databricks_retry_args = {"stop": {"type": "stop_after_attempt", "value": 3}}

**Not supported** in deferrable mode (will raise ``ValueError`` at task submission):

.. code-block:: python

    from tenacity import stop_after_attempt, wait_incrementing

    # Tenacity strategy objects — NOT serializable
    databricks_retry_args = {"stop": stop_after_attempt(3)}
    databricks_retry_args = {"wait": wait_incrementing(start=30, increment=30)}

    # Arbitrary callables — NOT serializable
    databricks_retry_args = {"retry": my_custom_retry_callable}

If you need a custom callable retry strategy, use the non-deferrable
:class:`~airflow.providers.databricks.operators.DatabricksRunNowOperator` (``deferrable=False``).
