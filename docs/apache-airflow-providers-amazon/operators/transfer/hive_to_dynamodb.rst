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

==============================
Apache Hive to Amazon DynamoDB
==============================

Use the ``HiveToDynamoDBOperator`` transfer to copy the contents of an
existing Apache Hive table to an existing Amazon DynamoDB table.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Operators
---------

.. _howto/transfer:HiveToDynamoDBOperator:

Apache Hive to Amazon DynamoDB transfer operator
================================================

This operator replicates records from an Apache Hive table to an Amazon DynamoDB table. The user must
specify an `HQL query <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Commands>`__
to use as filtering criteria.

To get more information visit:
:class:`~airflow.providers.amazon.aws.transfers.hive_to_dynamodb.HiveToDynamoDBOperator`

Example usage:

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_hive_to_dynamodb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_hive_to_dynamodb]
    :end-before: [END howto_transfer_hive_to_dynamodb]

Reference
---------

* `Apache Hive language manual <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Commands>`__
* `AWS boto3 library documentation for Amazon DynamoDB <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html>`__
