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
Amazon DynamoDB to Amazon S3
============================

Use the ``DynamoDBToS3Operator`` transfer to copy the content of an existing Amazon DynamoDB table
to an existing Amazon Simple Storage Service (S3) bucket.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/transfer:DynamoDBToS3Operator:

Amazon DynamoDB To Amazon S3 transfer operator
==============================================

This operator replicates records from an Amazon DynamoDB table to a file in an Amazon S3 bucket.
It scans an Amazon DynamoDB table and writes the received records to a file on the local
filesystem. It flushes the file to Amazon S3 once the file size exceeds the file size limit
specified by the user.

Users can also specify a filtering criteria using ``dynamodb_scan_kwargs`` to only replicate
records that satisfy the criteria.

To get more information visit:
:class:`~airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator`

Example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_dynamodb_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_dynamodb_to_s3]
    :end-before: [END howto_transfer_dynamodb_to_s3]

To parallelize the replication, users can create multiple ``DynamoDBToS3Operator`` tasks using the
``TotalSegments`` parameter.  For instance to replicate with parallelism of 2, create two tasks:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_dynamodb_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_dynamodb_to_s3_segmented]
    :end-before: [END howto_transfer_dynamodb_to_s3_segmented]

Users can also pass in ``point_in_time_export`` boolean param to ``DynamoDBToS3Operator`` to recover data from a point in time.

Full export example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_dynamodb_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_dynamodb_to_s3_in_some_point_in_time_full_export]
    :end-before: [END howto_transfer_dynamodb_to_s3_in_some_point_in_time_full_export]

Incremental export example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_dynamodb_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_dynamodb_to_s3_in_some_point_in_time_incremental_export]
    :end-before: [END howto_transfer_dynamodb_to_s3_in_some_point_in_time_incremental_export]

Reference
---------

* `AWS boto3 library documentation for Amazon DynamoDB <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html>`__
* `AWS boto3 library documentation for Amazon S3 <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html>`__
