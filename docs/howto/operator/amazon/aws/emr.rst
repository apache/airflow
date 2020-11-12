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


.. _howto/operator:EMROperators:

Amazon EMR Operators
====================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Overview
--------

Airflow to AWS EMR integration provides several operators to create and interact with EMR service.

 - :class:`~airflow.providers.amazon.aws.sensors.emr_job_flow.EmrJobFlowSensor`
 - :class:`~airflow.providers.amazon.aws.sensors.emr_step.EmrStepSensor`
 - :class:`~airflow.providers.amazon.aws.operators.emr_create_job_flow.EmrCreateJobFlowOperator`
 - :class:`~airflow.providers.amazon.aws.operators.emr_add_steps.EmrAddStepsOperator`
 - :class:`~airflow.providers.amazon.aws.operators.emr_modify_cluster.EmrModifyClusterOperator`
 - :class:`~airflow.providers.amazon.aws.operators.emr_terminate_job_flow.EmrTerminateJobFlowOperator`

Two example_dags are provided which showcase these operators in action.

 - example_emr_job_flow_automatic_steps.py
 - example_emr_job_flow_manual_steps.py

.. note::
    In order to run the 2 examples successfully, you need to create the IAM Service Roles (``EMR_EC2_DefaultRole`` and ``EMR_DefaultRole``) for Amazon EMR.

    You can create these roles using the AWS CLI: ``aws emr create-default-roles``

Create EMR Job Flow with automatic steps
----------------------------------------

Purpose
"""""""

This example dag ``example_emr_job_flow_automatic_steps.py`` use ``EmrCreateJobFlowOperator`` to create a new EMR job flow calculating the mathematical constant ``Pi``, and monitor the progress
with ``EmrJobFlowSensor``. The cluster will be terminated automatically after finishing the steps.

JobFlow configuration
"""""""""""""""""""""

To create a job flow at EMR, you need to specify the configuration for the EMR cluster:

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_emr_job_flow_automatic_steps.py
    :language: python
    :start-after: [START howto_operator_emr_automatic_steps_config]
    :end-before: [END howto_operator_emr_automatic_steps_config]

Here we create a EMR single-node Cluster *PiCalc*. It only has a single step *calculate_pi* which calculates the value of ``Pi`` using Spark.
The config ``'KeepJobFlowAliveWhenNoSteps': False`` tells the cluster to shut down after the step is finished.
For more config information, please refer to `Boto3 EMR client <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow>`__.

Defining tasks
""""""""""""""

In the following code we are creating a new job flow, add a step, monitor the step, and then terminate the cluster.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_emr_job_flow_automatic_steps.py
    :language: python
    :start-after: [START howto_operator_emr_automatic_steps_tasks]
    :end-before: [END howto_operator_emr_automatic_steps_tasks]

Create EMR Job Flow with manual steps
-------------------------------------

Purpose
"""""""

This example dag ``example_emr_job_flow_manual_steps.py`` is similar to the previous one except that instead of adding job flow step during cluster creation,
we add the step after the cluster is created. And the cluster is manually terminated at the end.
This allows for further customization on how you want to run your jobs.

JobFlow configuration
"""""""""""""""""""""

The configuration is similar to the previous example, except that we set ``'KeepJobFlowAliveWhenNoSteps': True`` because we will terminate the cluster manually.
Also, we wouldn't specify ``Steps`` in the config when creating the cluster.

Defining tasks
""""""""""""""

Here is the task definitions for our DAG.

.. exampleinclude:: /../airflow/providers/amazon/aws/example_dags/example_emr_job_flow_manual_steps.py
    :language: python
    :start-after: [START howto_operator_emr_manual_steps_tasks]
    :end-before: [END howto_operator_emr_manual_steps_tasks]

Reference
---------

For further information, look at:

* `Boto3 Library Documentation for EMR <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html>`__
* `AWS CLI - create-default-roles <https://docs.aws.amazon.com/cli/latest/reference/emr/create-default-roles.html>`__
* `Configure IAM Service Roles for Amazon EMR Permissions <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html>`__
