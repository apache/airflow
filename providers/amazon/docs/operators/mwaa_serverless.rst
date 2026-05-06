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

================================================
Amazon MWAA Serverless (Managed Workflows)
================================================

Amazon MWAA Serverless provides a serverless execution environment for Apache Airflow
workflows. Use the operators below to manage MWAA Serverless workflow runs.

.. _howto/operator:MwaaServerlessCreateWorkflowOperator:

Create a Workflow
-----------------

To create an Amazon MWAA Serverless workflow, use
:class:`~airflow.providers.amazon.aws.operators.mwaa_serverless.MwaaServerlessCreateWorkflowOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_mwaa_serverless.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_mwaa_serverless_create_workflow]
    :end-before: [END howto_operator_mwaa_serverless_create_workflow]

.. _howto/operator:MwaaServerlessStartWorkflowRunOperator:

Start a Workflow Run
~~~~~~~~~~~~~~~~~~~~

To start a new execution of an MWAA Serverless workflow, use
:class:`~airflow.providers.amazon.aws.operators.mwaa_serverless.MwaaServerlessStartWorkflowRunOperator`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_mwaa_serverless.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_mwaa_serverless_start_workflow_run]
    :end-before: [END howto_operator_mwaa_serverless_start_workflow_run]

Reference
~~~~~~~~~

* `AWS boto3 Library Documentation for MWAA Serverless <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/mwaa-serverless.html>`__

.. _howto/sensor:MwaaServerlessWorkflowRunSensor:

Wait for a Workflow Run
-----------------------

To wait for an Amazon MWAA Serverless workflow run to complete, use
:class:`~airflow.providers.amazon.aws.sensors.mwaa_serverless.MwaaServerlessWorkflowRunSensor`.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_mwaa_serverless.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_mwaa_serverless_workflow_run]
    :end-before: [END howto_sensor_mwaa_serverless_workflow_run]
