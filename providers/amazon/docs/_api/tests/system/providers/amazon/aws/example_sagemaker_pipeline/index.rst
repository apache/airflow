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

:py:mod:`tests.system.providers.amazon.aws.example_sagemaker_pipeline`
======================================================================

.. py:module:: tests.system.providers.amazon.aws.example_sagemaker_pipeline


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sagemaker_pipeline.create_pipeline
   tests.system.providers.amazon.aws.example_sagemaker_pipeline.delete_pipeline



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sagemaker_pipeline.DAG_ID
   tests.system.providers.amazon.aws.example_sagemaker_pipeline.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_sagemaker_pipeline.sys_test_context_task
   tests.system.providers.amazon.aws.example_sagemaker_pipeline.test_context
   tests.system.providers.amazon.aws.example_sagemaker_pipeline.test_run


.. py:data:: DAG_ID
   :value: 'example_sagemaker_pipeline'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:function:: create_pipeline(name, role_arn)


.. py:function:: delete_pipeline(name)


.. py:data:: test_context



.. py:data:: test_run
