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

`Amazon Athena <https://aws.amazon.com/athena/>`__ supports Apache Spark calculations through session-based APIs.
This page documents the provider support for submitting and monitoring those
calculations from Airflow.

Prerequisite Tasks
------------------

.. include:: ../../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:AthenaSparkOperator:

Submit Spark code to an Athena session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use :class:`~airflow.providers.amazon.aws.operators.athena_spark.AthenaSparkOperator`
to submit Spark code to an existing Athena Spark session.

In the following example, we submit PySpark code to an existing Athena Spark
session and wait for the calculation to complete. For more examples of how to use
this operator, please see the `Sample Dag <https://github.com/apache/airflow/blob/|version|/providers/amazon/tests/system/amazon/aws/example_athena_spark.py>`__.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_spark.py
    :language: python
    :start-after: [START howto_operator_athena_spark]
    :end-before: [END howto_operator_athena_spark]

Reference
---------

* `AWS boto3 documentation for Athena calculation APIs <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html>`__
