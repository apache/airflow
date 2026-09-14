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

Athena Spark Operators and Sensors
==================================

Amazon Athena supports Apache Spark calculations through session-based APIs.
This page documents the provider support for submitting and monitoring those
calculations from Airflow.

Prerequisite Tasks
------------------

Before using the Athena Spark operator or sensor, make sure that:

* an Athena Spark session already exists;
* an AWS connection is configured in Airflow;
* the connection has permission to start and read Athena calculation
  executions for the target session.

The :class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
submits Spark code to an existing Athena session and waits for the calculation
to reach a terminal state. It does not create the Athena Spark session.

Operators
---------

Submit Spark code to an Athena session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
to submit Spark code to an existing Athena Spark session.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_spark.py
    :language: python
    :start-after: [START howto_operator_athena_spark]
    :end-before: [END howto_operator_athena_spark]

Sensors
-------

Wait for an Athena Spark calculation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :class:`~airflow.providers.amazon.aws.sensors.athena_spark.AthenaSparkSensor`
to wait for an existing Athena Spark calculation execution to reach a terminal
state.

The sensor expects a calculation execution ID that was already created, for
example by calling
:class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
or by starting the calculation outside Airflow.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_spark.py
    :language: python
    :start-after: [START howto_sensor_athena_spark]
    :end-before: [END howto_sensor_athena_spark]
