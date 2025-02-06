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

:py:mod:`tests.system.providers.amazon.aws.example_datasync`
============================================================

.. py:module:: tests.system.providers.amazon.aws.example_datasync


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_datasync.get_s3_bucket_arn
   tests.system.providers.amazon.aws.example_datasync.create_location
   tests.system.providers.amazon.aws.example_datasync.create_source_location
   tests.system.providers.amazon.aws.example_datasync.create_destination_location
   tests.system.providers.amazon.aws.example_datasync.create_task
   tests.system.providers.amazon.aws.example_datasync.delete_task
   tests.system.providers.amazon.aws.example_datasync.delete_task_created_by_operator
   tests.system.providers.amazon.aws.example_datasync.list_locations
   tests.system.providers.amazon.aws.example_datasync.delete_locations



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_datasync.DAG_ID
   tests.system.providers.amazon.aws.example_datasync.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_datasync.sys_test_context_task
   tests.system.providers.amazon.aws.example_datasync.test_context
   tests.system.providers.amazon.aws.example_datasync.test_run


.. py:data:: DAG_ID
   :value: 'example_datasync'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:function:: get_s3_bucket_arn(bucket_name)


.. py:function:: create_location(bucket_name, role_arn)


.. py:function:: create_source_location(bucket_source, role_arn)


.. py:function:: create_destination_location(bucket_destination, role_arn)


.. py:function:: create_task(**kwargs)


.. py:function:: delete_task(task_arn)


.. py:function:: delete_task_created_by_operator(**kwargs)


.. py:function:: list_locations(bucket_source, bucket_destination)


.. py:function:: delete_locations(locations)


.. py:data:: test_context



.. py:data:: test_run
