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

Athena Spark Operators
======================

<<<<<<< HEAD
`Amazon Athena <https://aws.amazon.com/athena/>`__ supports Apache Spark calculations through session-based APIs.
=======
Amazon Athena supports Apache Spark calculations through session-based APIs.
>>>>>>> c99becd043 (Add AthenaHook methods for starting, polling, inspecting, and stopping)
This page documents the provider support for submitting and monitoring those
calculations from Airflow.

Prerequisite Tasks
------------------

<<<<<<< HEAD
.. include:: ../../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../../_partials/generic_parameters.rst
=======
Before using the Athena Spark operator or sensor, make sure that:

* an Athena Spark session already exists;
* an AWS connection is configured in Airflow;
* the connection has permission to start and read Athena calculation
  executions for the target session.

The :class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
submits Spark code to an existing Athena session and waits for the calculation
to reach a terminal state. It does not create the Athena Spark session.
>>>>>>> c99becd043 (Add AthenaHook methods for starting, polling, inspecting, and stopping)

Operators
---------

<<<<<<< HEAD
.. _howto/operator:AthenaSparkOperator:

=======
>>>>>>> c99becd043 (Add AthenaHook methods for starting, polling, inspecting, and stopping)
Submit Spark code to an Athena session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
to submit Spark code to an existing Athena Spark session.

<<<<<<< HEAD
In the following example, we submit PySpark code to an existing Athena Spark
session and wait for the calculation to complete. For more examples of how to use
this operator, please see the `Sample Dag <https://github.com/apache/airflow/blob/|version|/providers/amazon/tests/system/amazon/aws/example_athena_spark.py>`__.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_spark.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_athena_spark]
    :end-before: [END howto_operator_athena_spark]

Reference
---------

* `AWS boto3 documentation for Athena calculation APIs <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html>`__
=======
.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_spark.py
    :language: python
    :start-after: [START howto_operator_athena_spark]
    :end-before: [END howto_operator_athena_spark]
>>>>>>> c99becd043 (Add AthenaHook methods for starting, polling, inspecting, and stopping)
