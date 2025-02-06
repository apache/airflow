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

:py:mod:`tests.system.providers.amazon.aws.example_hive_to_dynamodb`
====================================================================

.. py:module:: tests.system.providers.amazon.aws.example_hive_to_dynamodb

.. autoapi-nested-parse::

   This DAG will not work unless you create an Amazon EMR cluster running
   Apache Hive and copy data into it following steps 1-4 (inclusive) here:
   https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/EMRforDynamoDB.Tutorial.html



Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_hive_to_dynamodb.create_dynamodb_table
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.get_dynamodb_item_count
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.delete_dynamodb_table
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.configure_hive_connection



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_hive_to_dynamodb.DAG_ID
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.HIVE_CONNECTION_ID_KEY
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.HIVE_HOSTNAME_KEY
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.sys_test_context_task
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.DYNAMODB_TABLE_HASH_KEY
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.HIVE_SQL
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.test_context
   tests.system.providers.amazon.aws.example_hive_to_dynamodb.test_run


.. py:data:: DAG_ID
   :value: 'example_hive_to_dynamodb'



.. py:data:: HIVE_CONNECTION_ID_KEY
   :value: 'HIVE_CONNECTION_ID'



.. py:data:: HIVE_HOSTNAME_KEY
   :value: 'HIVE_HOSTNAME'



.. py:data:: sys_test_context_task



.. py:data:: DYNAMODB_TABLE_HASH_KEY
   :value: 'feature_id'



.. py:data:: HIVE_SQL
   :value: 'SELECT feature_id, feature_name, feature_class, state_alpha FROM hive_features'



.. py:function:: create_dynamodb_table(table_name)


.. py:function:: get_dynamodb_item_count(table_name)

   A DynamoDB table has an ItemCount value, but it is only updated every six hours.
   To verify this DAG worked, we will scan the table and count the items manually.


.. py:function:: delete_dynamodb_table(table_name)


.. py:function:: configure_hive_connection(connection_id, hostname)


.. py:data:: test_context



.. py:data:: test_run
