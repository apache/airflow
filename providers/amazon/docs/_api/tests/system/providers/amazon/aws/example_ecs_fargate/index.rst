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

:py:mod:`tests.system.providers.amazon.aws.example_ecs_fargate`
===============================================================

.. py:module:: tests.system.providers.amazon.aws.example_ecs_fargate


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_ecs_fargate.create_cluster
   tests.system.providers.amazon.aws.example_ecs_fargate.register_task_definition
   tests.system.providers.amazon.aws.example_ecs_fargate.delete_task_definition
   tests.system.providers.amazon.aws.example_ecs_fargate.delete_cluster



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_ecs_fargate.DAG_ID
   tests.system.providers.amazon.aws.example_ecs_fargate.SUBNETS_KEY
   tests.system.providers.amazon.aws.example_ecs_fargate.SECURITY_GROUPS_KEY
   tests.system.providers.amazon.aws.example_ecs_fargate.sys_test_context_task
   tests.system.providers.amazon.aws.example_ecs_fargate.test_context
   tests.system.providers.amazon.aws.example_ecs_fargate.test_run


.. py:data:: DAG_ID
   :value: 'example_ecs_fargate'



.. py:data:: SUBNETS_KEY
   :value: 'SUBNETS'



.. py:data:: SECURITY_GROUPS_KEY
   :value: 'SECURITY_GROUPS'



.. py:data:: sys_test_context_task



.. py:function:: create_cluster(cluster_name)

   Creates an ECS cluster.


.. py:function:: register_task_definition(task_name, container_name)

   Creates a Task Definition.


.. py:function:: delete_task_definition(task_definition_arn)

   Deletes the Task Definition.


.. py:function:: delete_cluster(cluster_name)

   Deletes the ECS cluster.


.. py:data:: test_context



.. py:data:: test_run
