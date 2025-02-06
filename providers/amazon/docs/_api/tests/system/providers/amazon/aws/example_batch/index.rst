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

:py:mod:`tests.system.providers.amazon.aws.example_batch`
=========================================================

.. py:module:: tests.system.providers.amazon.aws.example_batch


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_batch.create_job_definition
   tests.system.providers.amazon.aws.example_batch.create_job_queue
   tests.system.providers.amazon.aws.example_batch.describe_job
   tests.system.providers.amazon.aws.example_batch.delete_job_definition
   tests.system.providers.amazon.aws.example_batch.disable_compute_environment
   tests.system.providers.amazon.aws.example_batch.delete_compute_environment
   tests.system.providers.amazon.aws.example_batch.disable_job_queue
   tests.system.providers.amazon.aws.example_batch.delete_job_queue



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_batch.log
   tests.system.providers.amazon.aws.example_batch.DAG_ID
   tests.system.providers.amazon.aws.example_batch.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_batch.SUBNETS_KEY
   tests.system.providers.amazon.aws.example_batch.SECURITY_GROUPS_KEY
   tests.system.providers.amazon.aws.example_batch.sys_test_context_task
   tests.system.providers.amazon.aws.example_batch.JOB_OVERRIDES
   tests.system.providers.amazon.aws.example_batch.test_context
   tests.system.providers.amazon.aws.example_batch.test_run


.. py:data:: log



.. py:data:: DAG_ID
   :value: 'example_batch'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: SUBNETS_KEY
   :value: 'SUBNETS'



.. py:data:: SECURITY_GROUPS_KEY
   :value: 'SECURITY_GROUPS'



.. py:data:: sys_test_context_task



.. py:data:: JOB_OVERRIDES
   :type: dict



.. py:function:: create_job_definition(role_arn, job_definition_name)


.. py:function:: create_job_queue(job_compute_environment_name, job_queue_name)


.. py:function:: describe_job(job_id)


.. py:function:: delete_job_definition(job_definition_name)


.. py:function:: disable_compute_environment(job_compute_environment_name)


.. py:function:: delete_compute_environment(job_compute_environment_name)


.. py:function:: disable_job_queue(job_queue_name)


.. py:function:: delete_job_queue(job_queue_name)


.. py:data:: test_context



.. py:data:: test_run
