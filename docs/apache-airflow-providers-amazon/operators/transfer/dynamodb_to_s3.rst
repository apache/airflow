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


Amazon DynamoDB to S3 Transfer Operator
=======================================

Use the DynamoDBToS3Operator transfer to copy the contents of an existing Amazon DynamoDB table
to an existing Amazon Simple Storage Service (S3) bucket.

.. _howto/transfer:DynamoDBToS3Operator:

DynamoDB To S3 Operator
^^^^^^^^^^^^^^^^^^^^^^^

This operator transfers an Amazon DynamoDB table to an existing Amazon Simple Storage Service (S3)
bucket as a json-serialized file.

To get more information about operator visit:
:class:`~airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator`

Example usage:

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_dynamodb_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_dynamodb_to_s3]
    :end-before: [END howto_transfer_dynamodb_to_s3]
