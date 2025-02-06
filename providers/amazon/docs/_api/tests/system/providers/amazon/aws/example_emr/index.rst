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

:py:mod:`tests.system.providers.amazon.aws.example_emr`
=======================================================

.. py:module:: tests.system.providers.amazon.aws.example_emr


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_emr.get_ami_id
   tests.system.providers.amazon.aws.example_emr.configure_security_config
   tests.system.providers.amazon.aws.example_emr.delete_security_config
   tests.system.providers.amazon.aws.example_emr.get_step_id



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_emr.DAG_ID
   tests.system.providers.amazon.aws.example_emr.CONFIG_NAME
   tests.system.providers.amazon.aws.example_emr.EXECUTION_ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_emr.SECURITY_CONFIGURATION
   tests.system.providers.amazon.aws.example_emr.SPARK_STEPS
   tests.system.providers.amazon.aws.example_emr.JOB_FLOW_OVERRIDES
   tests.system.providers.amazon.aws.example_emr.sys_test_context_task
   tests.system.providers.amazon.aws.example_emr.test_context
   tests.system.providers.amazon.aws.example_emr.test_run


.. py:data:: DAG_ID
   :value: 'example_emr'



.. py:data:: CONFIG_NAME
   :value: 'EMR Runtime Role Security Configuration'



.. py:data:: EXECUTION_ROLE_ARN_KEY
   :value: 'EXECUTION_ROLE_ARN'



.. py:data:: SECURITY_CONFIGURATION



.. py:data:: SPARK_STEPS



.. py:data:: JOB_FLOW_OVERRIDES



.. py:function:: get_ami_id()

   Returns an AL2 AMI compatible with EMR


.. py:function:: configure_security_config(config_name)


.. py:function:: delete_security_config(config_name)


.. py:function:: get_step_id(step_ids)


.. py:data:: sys_test_context_task



.. py:data:: test_context



.. py:data:: test_run
