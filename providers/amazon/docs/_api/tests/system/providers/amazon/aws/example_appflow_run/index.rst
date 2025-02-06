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

:py:mod:`tests.system.providers.amazon.aws.example_appflow_run`
===============================================================

.. py:module:: tests.system.providers.amazon.aws.example_appflow_run


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_appflow_run.create_s3_to_s3_flow
   tests.system.providers.amazon.aws.example_appflow_run.setup_bucket_permissions
   tests.system.providers.amazon.aws.example_appflow_run.delete_flow



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_appflow_run.sys_test_context_task
   tests.system.providers.amazon.aws.example_appflow_run.DAG_ID
   tests.system.providers.amazon.aws.example_appflow_run.test_context
   tests.system.providers.amazon.aws.example_appflow_run.test_run


.. py:data:: sys_test_context_task



.. py:data:: DAG_ID
   :value: 'example_appflow_run'



.. py:function:: create_s3_to_s3_flow(flow_name, bucket_name, source_folder)

   creates a flow that takes a CSV and converts it to a json containing the same data


.. py:function:: setup_bucket_permissions(bucket_name)


.. py:function:: delete_flow(flow_name)


.. py:data:: test_context



.. py:data:: test_run
