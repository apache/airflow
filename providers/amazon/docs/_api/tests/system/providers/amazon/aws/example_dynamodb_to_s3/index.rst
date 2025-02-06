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

:py:mod:`tests.system.providers.amazon.aws.example_dynamodb_to_s3`
==================================================================

.. py:module:: tests.system.providers.amazon.aws.example_dynamodb_to_s3


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_dynamodb_to_s3.enable_point_in_time_recovery
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.set_up_table
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.get_export_time
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.wait_for_bucket
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.delete_dynamodb_table



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_dynamodb_to_s3.log
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.DAG_ID
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.sys_test_context_task
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.TABLE_ATTRIBUTES
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.TABLE_KEY_SCHEMA
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.TABLE_THROUGHPUT
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.S3_KEY_PREFIX
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.test_context
   tests.system.providers.amazon.aws.example_dynamodb_to_s3.test_run


.. py:data:: log



.. py:data:: DAG_ID
   :value: 'example_dynamodb_to_s3'



.. py:data:: sys_test_context_task



.. py:data:: TABLE_ATTRIBUTES



.. py:data:: TABLE_KEY_SCHEMA



.. py:data:: TABLE_THROUGHPUT



.. py:data:: S3_KEY_PREFIX
   :value: 'dynamodb-segmented-file'



.. py:function:: enable_point_in_time_recovery(table_name)


.. py:function:: set_up_table(table_name)


.. py:function:: get_export_time(table_name)


.. py:function:: wait_for_bucket(s3_bucket_name)


.. py:function:: delete_dynamodb_table(table_name)


.. py:data:: test_context



.. py:data:: test_run
