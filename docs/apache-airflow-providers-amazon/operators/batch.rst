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

=========
AWS Batch
=========

`AWS Batch <https://aws.amazon.com/batch/>`__ enables you to run batch computing workloads on the AWS Cloud.
Batch computing is a common way for developers, scientists, and engineers to access large amounts of compute
resources. AWS Batch removes the undifferentiated heavy lifting of configuring and managing the required
infrastructure.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:BatchOperator:

Submit a new AWS Batch job
==========================

To submit a new AWS Batch job and monitor it until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.operators.batch.BatchOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch]
    :end-before: [END howto_operator_batch]

.. _howto/operator:BatchCreateComputeEnvironmentOperator:

Create an AWS Batch compute environment
=======================================

To create a new AWS Batch compute environment you can
use :class:`~airflow.providers.amazon.aws.operators.batch.BatchCreateComputeEnvironmentOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_batch_create_compute_environment]
    :end-before: [END howto_operator_batch_create_compute_environment]

Sensors
-------

.. _howto/sensor:BatchSensor:

Wait on an AWS Batch job state
==============================

To wait on the state of an AWS Batch Job until it reaches a terminal state you can
use :class:`~airflow.providers.amazon.aws.sensors.batch.BatchSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_batch]
    :end-before: [END howto_sensor_batch]

In order to monitor the state of the AWS Batch Job asynchronously, use
:class:`~airflow.providers.amazon.aws.sensors.batch.BatchSensor` with the
parameter ``deferrable`` set to True.

Since this will release the Airflow worker slot , it will lead to efficient
utilization of available resources on your Airflow deployment.
This will also need the triggerer component to be available in your
Airflow deployment.

.. _howto/sensor:BatchComputeEnvironmentSensor:

Wait on an AWS Batch compute environment status
===============================================

To wait on the status of an AWS Batch compute environment until it reaches a terminal status you can
use :class:`~airflow.providers.amazon.aws.sensors.batch.BatchComputeEnvironmentSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_batch_compute_environment]
    :end-before: [END howto_sensor_batch_compute_environment]

.. _howto/sensor:BatchJobQueueSensor:

Wait on an AWS Batch job queue status
=====================================

To wait on the status of an AWS Batch job queue until it reaches a terminal status you can
use :class:`~airflow.providers.amazon.aws.sensors.batch.BatchJobQueueSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_batch.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_batch_job_queue]
    :end-before: [END howto_sensor_batch_job_queue]

Reference
---------

* `AWS boto3 library documentation for Batch <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html>`__
