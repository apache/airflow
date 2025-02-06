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

:py:mod:`tests.system.providers.amazon.aws.example_step_functions`
==================================================================

.. py:module:: tests.system.providers.amazon.aws.example_step_functions


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_step_functions.create_state_machine
   tests.system.providers.amazon.aws.example_step_functions.delete_state_machine



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_step_functions.DAG_ID
   tests.system.providers.amazon.aws.example_step_functions.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_step_functions.sys_test_context_task
   tests.system.providers.amazon.aws.example_step_functions.STATE_MACHINE_DEFINITION
   tests.system.providers.amazon.aws.example_step_functions.test_context
   tests.system.providers.amazon.aws.example_step_functions.test_run


.. py:data:: DAG_ID
   :value: 'example_step_functions'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:data:: STATE_MACHINE_DEFINITION



.. py:function:: create_state_machine(env_id, role_arn)


.. py:function:: delete_state_machine(state_machine_arn)


.. py:data:: test_context



.. py:data:: test_run
