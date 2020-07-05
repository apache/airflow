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


.. _howto/operator:S3ToRedshiftOperator:

S3 To Redshift Transfer Operator
================================

.. contents::
  :depth: 1
  :local:

Overview
--------

The ``S3ToRedshiftOperator`` copies data from a S3 Bucket into a Redshift table.

The example dag provided showcases the
:class:`~airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator`
in action.

 - example_s3_to_redshift.py

example_s3_to_redshift.py
-------------------------

Purpose
"""""""

This is a basic example dag for using ``S3ToRedshiftOperator`` to copies data from a S3 Bucket into a Redshift table.

Environment variables
"""""""""""""""""""""

This example relies on the following variables, which can be passed via OS environment variables.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_s3_to_redshift.py
    :language: python
    :start-after: [START howto_operator_s3_to_redshift_env_variables]
    :end-before: [END howto_operator_s3_to_redshift_env_variables]

You need to set at least the ``S3_BUCKET``.

Copy S3 key into Redshift table
"""""""""""""""""""""""""""""""

In the following code we are copying the S3 key ``s3://{S3_BUCKET}/{S3_KEY}/{REDSHIFT_TABLE}`` into the Redshift table
``PUBLIC.{REDSHIFT_TABLE}``.

.. exampleinclude:: ../../../../../airflow/providers/amazon/aws/example_dags/example_s3_to_redshift.py
    :language: python
    :start-after: [START howto_operator_s3_to_redshift_task_1]
    :end-before: [END howto_operator_s3_to_redshift_task_1]

You can find more information to the ``COPY`` command used
`here <https://docs.aws.amazon.com/us_en/redshift/latest/dg/copy-parameters-data-source-s3.html>`__.

Reference
---------

For further information, look at:

* `AWS COPY from Amazon S3 Documentation <https://docs.aws.amazon.com/us_en/redshift/latest/dg/copy-parameters-data-source-s3.html>`__
* `AWS boto3 Library Documentation for S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
