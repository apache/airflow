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

========================================
Amazon Managed Service for Apache Flink
========================================

`Amazon Managed Service for Apache Flink <https://aws.amazon.com/managed-service-apache-flink/>`__ is a fully managed service that you can use to process and analyze streaming data using
Java, Python, SQL, or Scala. The service enables you to quickly author and run Java, SQL, or Scala code against streaming sources to perform time series analytics,
feed real-time dashboards, and create real-time metrics.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:KinesisAnalyticsV2CreateApplicationOperator:

Create an Amazon Managed Service for Apache Flink Application
==============================================================

To create an Amazon Managed Service for Apache Flink application, you can use
:class:`~airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2CreateApplicationOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_kinesis_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_application]
    :end-before: [END howto_operator_create_application]

.. _howto/operator:KinesisAnalyticsV2StartApplicationOperator:

Start an Amazon Managed Service for Apache Flink Application
=============================================================

To start an Amazon Managed Service for Apache Flink application, you can use
:class:`~airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2StartApplicationOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_kinesis_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_start_application]
    :end-before: [END howto_operator_start_application]

.. _howto/operator:KinesisAnalyticsV2StopApplicationOperator:

Stop an Amazon Managed Service for Apache Flink Application
=============================================================

To stop an Amazon Managed Service for Apache Flink application, you can use
:class:`~airflow.providers.amazon.aws.operators.kinesis_analytics.KinesisAnalyticsV2StopApplicationOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_kinesis_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_stop_application]
    :end-before: [END howto_operator_stop_application]

Sensors
-------

.. _howto/sensor:KinesisAnalyticsV2StartApplicationCompletedSensor:

Wait for an Amazon Managed Service for Apache Flink Application to start
========================================================================

To wait on the state of an Amazon Managed Service for Apache Flink Application to start you can use
:class:`~airflow.providers.amazon.aws.sensors.kinesis_analytics.KinesisAnalyticsV2StartApplicationCompletedSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_kinesis_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_start_application]
    :end-before: [END howto_sensor_start_application]

.. _howto/sensor:KinesisAnalyticsV2StopApplicationCompletedSensor:

Wait for an Amazon Managed Service for Apache Flink Application to stop
========================================================================

To wait on the state of an Amazon Managed Service for Apache Flink Application to stop you can use
:class:`~airflow.providers.amazon.aws.sensors.kinesis_analytics.KinesisAnalyticsV2StopApplicationCompletedSensor`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_kinesis_analytics.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_stop_application]
    :end-before: [END howto_sensor_stop_application]

Reference
---------

* `AWS boto3 library documentation for Amazon Managed Service for Apache Flink (Kinesis Analytics) <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesisanalyticsv2.html>`__
