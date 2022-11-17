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

==========
Amazon EMR
==========

`Amazon EMR <https://aws.amazon.com/emr/>`__ (previously called Amazon Elastic MapReduce)
is a managed cluster platform that simplifies running big data frameworks, such as Apache
Hadoop and Apache Spark, on AWS to process and analyze vast amounts of data. Using these
frameworks and related open-source projects, you can process data for analytics purposes
and business intelligence workloads.  Amazon EMR also lets you transform and move large
amounts of data into and out of other AWS data stores and databases, such as Amazon Simple
Storage Service (Amazon S3) and Amazon DynamoDB.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. note::
    In order to run the examples successfully, you need to create the IAM Service
    Roles(``EMR_EC2_DefaultRole`` and ``EMR_DefaultRole``) for Amazon EMR.  You can
    create these roles using the AWS CLI: ``aws emr create-default-roles``.

.. _howto/operator:EmrCreateJobFlowOperator:

Create an EMR job flow
======================

You can use :class:`~airflow.providers.amazon.aws.operators.emr.EmrCreateJobFlowOperator` to
create a new EMR job flow.  The cluster will be terminated automatically after finishing the steps.

JobFlow configuration
"""""""""""""""""""""

To create a job flow on EMR, you need to specify the configuration for the EMR cluster:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr.py
    :language: python
    :start-after: [START howto_operator_emr_steps_config]
    :end-before: [END howto_operator_emr_steps_config]

Here we create an EMR single-node Cluster *PiCalc*. It only has a single step *calculate_pi* which
calculates the value of ``Pi`` using Spark.  The config ``'KeepJobFlowAliveWhenNoSteps': False``
tells the cluster to shut down after the step is finished.  Alternatively, a config without a ``Steps``
value can be used and Steps can be added at a later date using
:class:`~airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator`.  See details below.

.. note::
    EMR clusters launched with the EMR API like this one are not visible to all users by default, so
    you may not see the cluster in the EMR Management Console - you can change this by adding
    ``'VisibleToAllUsers': True`` at the end of the ``JOB_FLOW_OVERRIDES`` dict.

For more config information, please refer to `Boto3 EMR client <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow>`__.

Create the Job Flow
"""""""""""""""""""

In the following code we are creating a new job flow using the configuration as explained above.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_emr_create_job_flow]
    :end-before: [END howto_operator_emr_create_job_flow]

.. _howto/operator:EmrAddStepsOperator:

Add Steps to an EMR job flow
============================

To add steps to an existing EMR Job flow you can use
:class:`~airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_emr_add_steps]
    :end-before: [END howto_operator_emr_add_steps]

.. _howto/operator:EmrTerminateJobFlowOperator:

Terminate an EMR job flow
=========================

To terminate an EMR Job Flow you can use
:class:`~airflow.providers.amazon.aws.operators.emr.EmrTerminateJobFlowOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_emr_terminate_job_flow]
    :end-before: [END howto_operator_emr_terminate_job_flow]

.. _howto/operator:EmrModifyClusterOperator:

Modify Amazon EMR container
===========================

To modify an existing EMR container you can use
:class:`~airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_emr_modify_cluster]
    :end-before: [END howto_operator_emr_modify_cluster]

Sensors
-------

.. _howto/sensor:EmrJobFlowSensor:

Wait on an Amazon EMR job flow state
====================================

To monitor the state of an EMR job flow you can use
:class:`~airflow.providers.amazon.aws.sensors.emr.EmrJobFlowSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_emr.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_emr_job_flow]
    :end-before: [END howto_sensor_emr_job_flow]

.. _howto/sensor:EmrStepSensor:

Wait on an Amazon EMR step state
================================

Reference
---------

* `AWS boto3 library documentation for EMR <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html>`__
* `AWS CLI - create-default-roles <https://docs.aws.amazon.com/cli/latest/reference/emr/create-default-roles.html>`__
* `Configure IAM Service Roles for Amazon EMR Permissions <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-roles.html>`__
