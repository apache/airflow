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


Amazon Athena Operators
=======================

`Amazon Athena <https://aws.amazon.com/athena/>`__ is an interactive query service
that makes it easy to analyze data in Amazon Simple Storage Service (S3) using
standard SQL.  Athena is serverless, so there is no infrastructure to setup or
manage, and you pay only for the queries you run.  To get started, simply point
to your data in S3, define the schema, and start querying using standard SQL.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst


.. _howto/operator:AthenaOperator:

Athena Operator
^^^^^^^^^^^^^^^

Use the :class:`~airflow.providers.amazon.aws.operators.athena.AthenaOperator`
to run a query in Amazon Athena.


In the following example, we query an existing Athena table and send the results to
an existing Amazon S3 bucket.  For more examples of how to use this operator, please
see the `Sample DAG <https://github.com/apache/airflow/blob/main/airflow/providers/amazon/aws/example_dags/example_athena.py>`__.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_athena.py
    :language: python
    :start-after: [START howto_athena_operator]
    :dedent: 4
    :end-before: [END howto_athena_operator]

.. _howto/operator:AthenaSensor:

Athena Sensor
^^^^^^^^^^^^^

Use the :class:`~airflow.providers.amazon.aws.sensors.athena.AthenaSensor`
to wait for the results of a query in Amazon Athena.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_athena.py
    :language: python
    :start-after: [START howto_athena_sensor]
    :dedent: 4
    :end-before: [END howto_athena_sensor]


Reference
^^^^^^^^^

For further information, look at:

* `Boto3 Library Documentation for Athena <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html>`__
