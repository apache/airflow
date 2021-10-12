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


.. _howto/operator:AWSAthenaOperator:

Amazon Athena Operator
======================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Using Operator
--------------
Use the
:class:`~airflow.providers.amazon.aws.operators.athena.AWSAthenaOperator`
to run a query in Amazon Athena.  To get started with Amazon Athena please visit
`aws.amazon.com/athena <https://aws.amazon.com/athena>`_


In the following example, we create an Athena table and run a query based upon a CSV file
created in an S3 bucket and populated with SAMPLE_DATA.  The example waits for the query
to complete and then drops the created table and deletes the sample CSV file in the S3
bucket.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_athena.py
    :language: python
    :start-after: [START howto_athena_operator_and_sensor]
    :end-before: [END howto_athena_operator_and_sensor]

More information
----------------

For further information, look at the documentation of :meth:`~Athena.Client.start_query_execution` method
in `boto3`_.

.. _boto3: https://pypi.org/project/boto3/
