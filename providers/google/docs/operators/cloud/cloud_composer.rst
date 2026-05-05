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

Managed Service for Apache Airflow (formerly Cloud Composer) Operators
======================================================================

Managed Service for Apache Airflow (formerly Cloud Composer) is a fully managed workflow
orchestration service, enabling you to create, schedule, monitor, and manage workflows that span
across clouds and on-premises data centers.

Managed Service for Apache Airflow (formerly Cloud Composer) is built on the popular Apache Airflow
open source project and operates using the Python programming language.

By using Managed Service for Apache Airflow (formerly Cloud Composer) instead of a local instance
of Apache Airflow, you can benefit from the best of Airflow with no installation or management
overhead. Managed Service for Apache Airflow (formerly Cloud Composer) helps you create Airflow
environments quickly and use Airflow-native tools, such as the powerful Airflow web interface and
command-line tools, so you can focus on your workflows and not your infrastructure.

For more information about the service visit the `product documentation
<https://cloud.google.com/composer/docs/concepts/overview>`__.

Create an environment
---------------------

Before you create a Managed Service for Apache Airflow environment you need to define it.
For more information about the available fields to pass when creating an environment, visit the
`Managed Service for Apache Airflow create environment API
<https://cloud.google.com/composer/docs/reference/rest/v1/projects.locations.environments#Environment>`__.

A simple environment configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_managed_airflow_simple_environment]
    :end-before: [END howto_operator_managed_airflow_simple_environment]

With this configuration we can create the environment:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowCreateEnvironmentOperator`

The executable example below still imports the compatibility name
``CloudComposerCreateEnvironmentOperator``. The preferred alias for new code is
``ManagedAirflowCreateEnvironmentOperator``.

The create operator only succeeds after the Composer environment reaches the ``RUNNING`` state.
If the long-running create operation finishes but the environment remains in another state such as
``ERROR`` or ``CREATING``, the task fails so downstream tasks do not run against an unusable environment.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_managed_airflow_environment]
    :end-before: [END howto_operator_create_managed_airflow_environment]

or you can define the same operator in the deferrable mode:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowCreateEnvironmentOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_managed_airflow_environment_deferrable_mode]
    :end-before: [END howto_operator_create_managed_airflow_environment_deferrable_mode]

For retry-heavy system tests, you can clean up a failed environment before retrying the create task.
The example below only deletes environments that are already in the ``ERROR`` state and leaves other
states untouched.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_composer_retry_cleanup]
    :end-before: [END howto_operator_composer_retry_cleanup]

Get an environment
------------------

To get an environment you can use:

:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowGetEnvironmentOperator`

The executable example below still imports the compatibility name
``CloudComposerGetEnvironmentOperator``. The preferred alias for new code is
``ManagedAirflowGetEnvironmentOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_managed_airflow_environment]
    :end-before: [END howto_operator_get_managed_airflow_environment]

List environments
--------------------

To list environments you can use:

:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowListEnvironmentsOperator`

The executable example below still imports the compatibility name
``CloudComposerListEnvironmentsOperator``. The preferred alias for new code is
``ManagedAirflowListEnvironmentsOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_list_managed_airflow_environments]
    :end-before: [END howto_operator_list_managed_airflow_environments]

Update environments
----------------------

You can update the environments by providing an environment config and an updateMask.
In the updateMask argument you specify the path, relative to the environment, of the field to update.
For more information on updateMask and other parameters take a look at the
`Managed Service for Apache Airflow update environment API
<https://cloud.google.com/composer/docs/reference/rest/v1/projects.locations.environments/patch>`__.

An example of a new service config and the updateMask:

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_managed_airflow_update_environment]
    :end-before: [END howto_operator_managed_airflow_update_environment]

To update a service you can use:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowUpdateEnvironmentOperator`

The executable example below still imports the compatibility name
``CloudComposerUpdateEnvironmentOperator``. The preferred alias for new code is
``ManagedAirflowUpdateEnvironmentOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_managed_airflow_environment]
    :end-before: [END howto_operator_update_managed_airflow_environment]

or you can define the same operator in the deferrable mode:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowUpdateEnvironmentOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_update_managed_airflow_environment_deferrable_mode]
    :end-before: [END howto_operator_update_managed_airflow_environment_deferrable_mode]

Delete an environment
---------------------

To delete an environment you can use:

:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowDeleteEnvironmentOperator`

The executable example below still imports the compatibility name
``CloudComposerDeleteEnvironmentOperator``. The preferred alias for new code is
``ManagedAirflowDeleteEnvironmentOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_managed_airflow_environment]
    :end-before: [END howto_operator_delete_managed_airflow_environment]

or you can define the same operator in the deferrable mode:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowDeleteEnvironmentOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_delete_managed_airflow_environment_deferrable_mode]
    :end-before: [END howto_operator_delete_managed_airflow_environment_deferrable_mode]


List of Managed Airflow Images
------------------------------

You can also list all supported Managed Service for Apache Airflow images:

:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowListImageVersionsOperator`

The executable example below still imports the compatibility name
``CloudComposerListImageVersionsOperator``. The preferred alias for new code is
``ManagedAirflowListImageVersionsOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_managed_airflow_image_list]
    :end-before: [END howto_operator_managed_airflow_image_list]

Run Airflow CLI commands
------------------------

You can run Airflow CLI commands in your environments, use:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowRunAirflowCLICommandOperator`

The executable example below still imports the compatibility name
``CloudComposerRunAirflowCLICommandOperator``. The preferred alias for new code is
``ManagedAirflowRunAirflowCLICommandOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_run_airflow_cli_command]
    :end-before: [END howto_operator_run_airflow_cli_command]

or you can define the same operator in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_run_airflow_cli_command_deferrable_mode]
    :end-before: [END howto_operator_run_airflow_cli_command_deferrable_mode]

Check if a Dag run has completed
--------------------------------

You can use sensor that checks if a Dag run has completed in your environments, use:
:class:`~airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerDAGRunSensor`

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_dag_run]
    :end-before: [END howto_sensor_dag_run]

or you can define the same sensor in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_dag_run_deferrable_mode]
    :end-before: [END howto_sensor_dag_run_deferrable_mode]

Trigger a Dag run
-----------------

You can trigger a DAG in another Managed Service for Apache Airflow environment, use:
:class:`~airflow.providers.google.cloud.operators.managed_airflow.ManagedAirflowTriggerDAGRunOperator`

The executable example below still imports the compatibility name
``CloudComposerTriggerDAGRunOperator``. The preferred alias for new code is
``ManagedAirflowTriggerDAGRunOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_trigger_dag_run]
    :end-before: [END howto_operator_trigger_dag_run]

Waits for a different Dag, task group, or task to complete
----------------------------------------------------------

You can use sensor that waits for a different DAG, task group, or task to complete for a specific
Managed Service for Apache Airflow environment, use:
:class:`~airflow.providers.google.cloud.sensors.cloud_composer.CloudComposerExternalTaskSensor`

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_external_task]
    :end-before: [END howto_sensor_external_task]

or you can define the same sensor in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/composer/example_cloud_composer.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_external_task_deferrable_mode]
    :end-before: [END howto_sensor_external_task_deferrable_mode]
