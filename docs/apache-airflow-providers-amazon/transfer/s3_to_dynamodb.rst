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

============================
Amazon S3 to DynamoDB
============================

Use the ``S3ToDynamoDBOperator`` transfer to load data stored in Amazon Simple Storage Service (S3) bucket
to an existing or new Amazon DynamoDB table.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/transfer:S3ToDynamoDBOperator:

Amazon S3 To DynamoDB transfer operator
==============================================

This operator loads data from Amazon S3 to an Amazon DynamoDB table. It uses the Amazon DynamoDB
ImportTable Services that interacts with different AWS Services such Amazon S3 and CloudWatch. The
default behavior is to load S3 data into a new Amazon DynamoDB table. The import into an existing
table is currently not supported by the Service. Thus, the operator uses a custom approach. It creates
a temporary DynamoDB table and loads S3 data into the table. Then it scans the temporary Amazon
DynamoDB table and writes the received records to the target table.


To get more information visit:
:class:`~airflow.providers.amazon.aws.transfers.s3_to_dynamodb.S3ToDynamoDBOperator`

Example usage:

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_s3_to_dynamodb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_dynamodb]
    :end-before: [END howto_transfer_s3_to_dynamodb]


To load S3 data into an existing DynamoDB table use:

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_s3_to_dynamodb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_s3_to_dynamodb_existing_table]
    :end-before: [END howto_transfer_s3_to_dynamodb_existing_table]


Reference
---------

* `AWS boto3 library documentation for Amazon DynamoDB <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
