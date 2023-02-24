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

dbt Cloud Operators
===================

These operators can execute dbt Cloud jobs, poll for status of a currently-executing job, and download run
artifacts locally.

Each of the operators can be tied to a specific dbt Cloud Account in two ways:

    * Explicitly provide the Account ID (via the ``account_id`` parameter) to the operator.
    * Or, specify the dbt Cloud Account in the Airflow Connection. The operators will fallback to using this
      automatically if the Account ID is not passed to the operator.

.. _howto/operator:DbtCloudRunJobOperator:

Trigger a dbt Cloud Job
~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator` to trigger a run of a dbt
Cloud job. By default, the operator will periodically check on the status of the executed job to terminate
with a successful status every ``check_interval`` seconds or until the job reaches a ``timeout`` length of
execution time. This functionality is controlled by the ``wait_for_termination`` parameter. Alternatively,
``wait_for_termination`` can be set to False to perform an asynchronous wait (typically paired with the
:class:`~airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor`). Setting ``wait_for_termination`` to
False is a good approach for long-running dbt Cloud jobs.

The ``deferrable`` parameter along with ``wait_for_termination`` will control the functionality
whether to poll the job status on the worker or defer using the Triggerer.
When ``wait_for_termination`` is True and ``deferrable`` is False,we submit the job and ``poll``
for its status on the worker. This will keep the worker slot occupied till the job execution is done.
When ``wait_for_termination`` is True and ``deferrable`` is True,
we submit the job and ``defer`` using Triggerer. This will release the worker slot leading to savings in
resource utilization while the job is running.

When ``wait_for_termination`` is False and ``deferrable`` is False, we just submit the job and can only
track the job status with the :class:`~airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor`.


While ``schema_override`` and ``steps_override`` are explicit, optional parameters for the
``DbtCloudRunJobOperator``, custom run configurations can also be passed to the operator using the
``additional_run_config`` dictionary. This parameter can be used to initialize additional runtime
configurations or overrides for the job run such as ``threads_override``, ``generate_docs_override``,
``git_branch``, etc. For a complete list of the other configurations that can used at runtime, reference the
`API documentation <https://docs.getdbt.com/dbt-cloud/api-v2#operation/triggerRun>`__.

The below examples demonstrate how to instantiate DbtCloudRunJobOperator tasks with both synchronous and
asynchronous waiting for run termination, respectively. To note, the ``account_id`` for the operators is
referenced within the ``default_args`` of the example DAG.

.. exampleinclude:: /../../tests/system/providers/dbt/cloud/example_dbt_cloud.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_cloud_run_job]
    :end-before: [END howto_operator_dbt_cloud_run_job]

This next example also shows how to pass in custom runtime configuration (in this case for ``threads_override``)
via the ``additional_run_config`` dictionary.

.. exampleinclude:: /../../tests/system/providers/dbt/cloud/example_dbt_cloud.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_cloud_run_job_async]
    :end-before: [END howto_operator_dbt_cloud_run_job_async]


.. _howto/operator:DbtCloudJobRunSensor:

Poll for status of a dbt Cloud Job run
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor` to periodically retrieve the
status of a dbt Cloud job run and check whether the run has succeeded. This sensor provides all of the same
functionality available with the :class:`~airflow.sensors.base.BaseSensorOperator`.

In the example below, the ``run_id`` value in the example below comes from the output of a previous
DbtCloudRunJobOperator task by utilizing the ``.output`` property exposed for all operators. Also, to note,
the ``account_id`` for the task is referenced within the ``default_args`` of the example DAG.

.. exampleinclude:: /../../tests/system/providers/dbt/cloud/example_dbt_cloud.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_cloud_run_job_sensor]
    :end-before: [END howto_operator_dbt_cloud_run_job_sensor]

.. _howto/operator:DbtCloudJobRunAsyncSensor:

Poll for status of a dbt Cloud Job run asynchronously
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunAsyncSensor`
(deferrable version) to periodically retrieve the
status of a dbt Cloud job run asynchronously. This sensor will free up the worker slots since
polling for job status happens on the Airflow triggerer, leading to efficient utilization
of resources within Airflow.

.. exampleinclude:: /../../tests/system/providers/dbt/cloud/example_dbt_cloud.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_cloud_run_job_async_sensor]
    :end-before: [END howto_operator_dbt_cloud_run_job_async_sensor]

.. _howto/operator:DbtCloudGetJobRunArtifactOperator:

Download run artifacts
~~~~~~~~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.dbt.cloud.operators.dbt.DbtCloudGetJobRunArtifactOperator` to download
dbt-generated artifacts for a dbt Cloud job run. The specified ``path`` value should be rooted at the
``target/`` directory.  Typical artifacts include ``manifest.json``, ``catalog.json``, and
``run_results.json``, but other artifacts such as raw SQL of models or ``sources.json`` can also be
downloaded.

For more information on dbt Cloud artifacts, reference
`this documentation <https://docs.getdbt.com/docs/dbt-cloud/using-dbt-cloud/artifacts>`__.

.. exampleinclude:: /../../tests/system/providers/dbt/cloud/example_dbt_cloud.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_cloud_get_artifact]
    :end-before: [END howto_operator_dbt_cloud_get_artifact]


.. _howto/operator:DbtCloudListJobsOperator:

List jobs
~~~~~~~~~

Use the :class:`~airflow.providers.dbt.cloud.operators.dbt.DbtCloudListJobsOperator` to list
all jobs tied to a specified dbt Cloud account. The ``account_id`` must be supplied either
through the connection or supplied as a parameter to the task.

If a ``project_id`` is supplied, only jobs pertaining to this project id will be retrieved.

For more information on dbt Cloud list jobs, reference
`this documentation <https://docs.getdbt.com/dbt-cloud/api-v2#tag/Jobs/operation/listJobsForAccount>`__.

.. exampleinclude:: /../../tests/system/providers/dbt/cloud/example_dbt_cloud.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_cloud_list_jobs]
    :end-before: [END howto_operator_dbt_cloud_list_jobs]
