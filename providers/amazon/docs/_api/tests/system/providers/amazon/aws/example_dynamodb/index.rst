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

:py:mod:`tests.system.providers.amazon.aws.example_dynamodb`
============================================================

.. py:module:: tests.system.providers.amazon.aws.example_dynamodb


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_dynamodb.create_table
   tests.system.providers.amazon.aws.example_dynamodb.delete_table



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_dynamodb.DAG_ID
   tests.system.providers.amazon.aws.example_dynamodb.sys_test_context_task
   tests.system.providers.amazon.aws.example_dynamodb.PK_ATTRIBUTE_NAME
   tests.system.providers.amazon.aws.example_dynamodb.SK_ATTRIBUTE_NAME
   tests.system.providers.amazon.aws.example_dynamodb.TABLE_ATTRIBUTES
   tests.system.providers.amazon.aws.example_dynamodb.TABLE_KEY_SCHEMA
   tests.system.providers.amazon.aws.example_dynamodb.TABLE_THROUGHPUT
   tests.system.providers.amazon.aws.example_dynamodb.test_context
   tests.system.providers.amazon.aws.example_dynamodb.test_run


.. py:data:: DAG_ID
   :value: 'example_dynamodbvaluesensor'



.. py:data:: sys_test_context_task



.. py:data:: PK_ATTRIBUTE_NAME
   :value: 'PK'



.. py:data:: SK_ATTRIBUTE_NAME
   :value: 'SK'



.. py:data:: TABLE_ATTRIBUTES



.. py:data:: TABLE_KEY_SCHEMA



.. py:data:: TABLE_THROUGHPUT



.. py:function:: create_table(table_name)


.. py:function:: delete_table(table_name)


.. py:data:: test_context



.. py:data:: test_run
