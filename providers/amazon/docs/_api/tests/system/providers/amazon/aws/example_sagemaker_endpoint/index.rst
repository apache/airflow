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

:py:mod:`tests.system.providers.amazon.aws.example_sagemaker_endpoint`
======================================================================

.. py:module:: tests.system.providers.amazon.aws.example_sagemaker_endpoint


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sagemaker_endpoint.call_endpoint
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.delete_endpoint_config
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.delete_endpoint
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.archive_logs
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.set_up



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sagemaker_endpoint.DAG_ID
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.sys_test_context_task
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.KNN_IMAGES_BY_REGION
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.TRAIN_DATA
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.SAMPLE_TEST_DATA
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.test_context
   tests.system.providers.amazon.aws.example_sagemaker_endpoint.test_run


.. py:data:: DAG_ID
   :value: 'example_sagemaker_endpoint'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:data:: KNN_IMAGES_BY_REGION



.. py:data:: TRAIN_DATA
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """0,4.9,2.5,4.5,1.7
        1,7.0,3.2,4.7,1.4
        0,7.3,2.9,6.3,1.8
        2,5.1,3.5,1.4,0.2
        """

    .. raw:: html

        </details>



.. py:data:: SAMPLE_TEST_DATA
   :value: '6.4,3.2,4.5,1.5'



.. py:function:: call_endpoint(endpoint_name)


.. py:function:: delete_endpoint_config(endpoint_config_job_name)


.. py:function:: delete_endpoint(endpoint_name)


.. py:function:: archive_logs(log_group_name)


.. py:function:: set_up(env_id, role_arn, ti=None)


.. py:data:: test_context



.. py:data:: test_run
