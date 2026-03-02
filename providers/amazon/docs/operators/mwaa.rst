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

==================================================
Amazon Managed Workflows for Apache Airflow (MWAA)
==================================================

`Amazon Managed Workflows for Apache Airflow (MWAA) <https://aws.amazon.com/managed-workflows-for-apache-airflow/>`__
is a managed service for Apache Airflow that lets you use your current, familiar Apache Airflow platform to orchestrate
your workflows. You gain improved scalability, availability, and security without the operational burden of managing
underlying infrastructure.

Note: Unlike Airflow's built-in operators, these operators are meant for interaction with external Airflow environments
hosted on AWS MWAA.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:MwaaTriggerDagRunOperator:

Trigger a Dag run in an Amazon MWAA environment
===============================================

To trigger a Dag run in an Amazon MWAA environment you can use the
:class:`~airflow.providers.amazon.aws.operators.mwaa.MwaaTriggerDagRunOperator`

In the following example, the task ``trigger_dag_run`` triggers a Dag run for the Dag ``hello_world`` in the environment
``MyAirflowEnvironment`` and waits for the run to complete.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_mwaa.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_mwaa_trigger_dag_run]
    :end-before: [END howto_operator_mwaa_trigger_dag_run]

Sensors
-------

.. _howto/sensor:MwaaDagRunSensor:

Wait on the state of an AWS MWAA Dag Run
========================================

To wait for a Dag Run running on Amazon MWAA until it reaches one of the given states, you can use the
:class:`~airflow.providers.amazon.aws.sensors.mwaa.MwaaDagRunSensor`

In the following example, the task ``wait_for_dag_run`` waits for the Dag run created in the above task to complete.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_mwaa.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_mwaa_dag_run]
    :end-before: [END howto_sensor_mwaa_dag_run]

.. _howto/sensor:MwaaTaskSensor:

Wait on the state of an AWS MWAA Task
========================================

To wait for a Dag task instance across MWAA environments until it reaches one of the given states, you can use the
:class:`~airflow.providers.amazon.aws.sensors.mwaa.MwaaTaskSensor`

In the following example, the task ``wait_for_task`` waits for the Dag run created in the above task to complete.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_mwaa.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_mwaa_task]
    :end-before: [END howto_sensor_mwaa_task]

References
----------

* `AWS boto3 library documentation for MWAA <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mwaa.html>`__
