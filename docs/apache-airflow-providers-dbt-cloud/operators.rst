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
