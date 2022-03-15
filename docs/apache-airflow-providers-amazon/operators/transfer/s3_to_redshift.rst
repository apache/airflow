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

Amazon S3 to Redshift Transfer Operator
=======================================

Use the S3ToRedshiftOperator transfer to copy the data from an Amazon Simple Storage Service (S3) file into an
Amazon Redshift table.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: ../_partials/prerequisite_tasks.rst

.. _howto/operator:S3ToRedshiftOperator:

Amazon S3 To Amazon Redshift
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This operator loads data from Amazon S3 to an existing Amazon Redshift table.

To get more information about this operator visit:
:class:`~airflow.providers.amazon.aws.transfers.s3_to_redshift.S3ToRedshiftOperator`

Example usage:

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_s3_to_redshift.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_redshift]
    :end-before: [END howto_transfer_s3_to_redshift]

You can find more information to the ``COPY`` command used
`here <https://docs.aws.amazon.com/us_en/redshift/latest/dg/copy-parameters-data-source-s3.html>`__.

Reference
^^^^^^^^^

* `AWS COPY from Amazon S3 Documentation <https://docs.aws.amazon.com/us_en/redshift/latest/dg/copy-parameters-data-source-s3.html>`__
* `AWS boto3 Library Documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
* `AWS boto3 Library Documentation for Amazon Redshift <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html>`__
