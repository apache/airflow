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

=============
Amazon Athena
=============

`Amazon Athena <https://aws.amazon.com/athena/>`__ is an interactive query service
that makes it easy to analyze data in Amazon Simple Storage Service (S3) using
standard SQL.  Athena is serverless, so there is no infrastructure to setup or
manage, and you pay only for the queries you run.  To get started, simply point
to your data in S3, define the schema, and start querying using standard SQL.

Prerequisite Tasks
------------------

.. include:: ../../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:AthenaOperator:

Run a query in Amazon Athena
============================

Use the :class:`~airflow.providers.amazon.aws.operators.athena.AthenaOperator`
to run a query in Amazon Athena.

In the following example, we query an existing Athena table and send the results to
an existing Amazon S3 bucket.  For more examples of how to use this operator, please
see the `Sample Dag <https://github.com/apache/airflow/blob/|version|/providers/amazon/tests/system/amazon/aws/example_athena.py>`__.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena.py
    :language: python
    :start-after: [START howto_operator_athena]
    :dedent: 4
    :end-before: [END howto_operator_athena]

.. _howto/operator:AthenaOperator:managed-query-results:

Run a query in a workgroup that uses managed query results
==========================================================

With `managed query results <https://docs.aws.amazon.com/athena/latest/ug/managed-results.html>`__,
Athena stores the query results for you, keeps them for 24 hours, and returns them through the
Athena API. To run a query in such a workgroup, pass the workgroup and leave ``output_location``
unset:

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_managed_query_results.py
    :language: python
    :start-after: [START howto_operator_athena_managed_query_results]
    :dedent: 4
    :end-before: [END howto_operator_athena_managed_query_results]

Read the results with
:meth:`~airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_query_results` or
:meth:`~airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_query_results_paginator`.
These queries have no S3 output location, so
:meth:`~airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_output_location` returns
``None`` for them.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena_managed_query_results.py
    :language: python
    :start-after: [START howto_athena_managed_query_results_read]
    :end-before: [END howto_athena_managed_query_results_read]

Managed query results do not support query result reuse.

Sensors
-------

.. _howto/sensor:AthenaSensor:

Wait on Amazon Athena query results
===================================

Use the :class:`~airflow.providers.amazon.aws.sensors.athena.AthenaSensor`
to wait for the results of a query in Amazon Athena.

.. exampleinclude:: /../../amazon/tests/system/amazon/aws/example_athena.py
    :language: python
    :start-after: [START howto_sensor_athena]
    :dedent: 4
    :end-before: [END howto_sensor_athena]


Reference
---------

* `AWS boto3 library documentation for Athena <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html>`__
