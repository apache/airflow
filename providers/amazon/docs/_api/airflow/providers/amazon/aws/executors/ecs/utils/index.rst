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

:py:mod:`airflow.providers.amazon.aws.executors.ecs.utils`
==========================================================

.. py:module:: airflow.providers.amazon.aws.executors.ecs.utils

.. autoapi-nested-parse::

   AWS ECS Executor Utilities.

   Data classes and utility functions used by the ECS executor.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.executors.ecs.utils.EcsQueuedTask
   airflow.providers.amazon.aws.executors.ecs.utils.EcsTaskInfo
   airflow.providers.amazon.aws.executors.ecs.utils.BaseConfigKeys
   airflow.providers.amazon.aws.executors.ecs.utils.RunTaskKwargsConfigKeys
   airflow.providers.amazon.aws.executors.ecs.utils.AllEcsConfigKeys
   airflow.providers.amazon.aws.executors.ecs.utils.EcsExecutorTask
   airflow.providers.amazon.aws.executors.ecs.utils.EcsTaskCollection



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.executors.ecs.utils.parse_assign_public_ip
   airflow.providers.amazon.aws.executors.ecs.utils.camelize_dict_keys



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.executors.ecs.utils.CommandType
   airflow.providers.amazon.aws.executors.ecs.utils.ExecutorConfigFunctionType
   airflow.providers.amazon.aws.executors.ecs.utils.ExecutorConfigType
   airflow.providers.amazon.aws.executors.ecs.utils.CONFIG_GROUP_NAME
   airflow.providers.amazon.aws.executors.ecs.utils.RUN_TASK_KWARG_DEFAULTS
   airflow.providers.amazon.aws.executors.ecs.utils.CONFIG_DEFAULTS


.. py:data:: CommandType



.. py:data:: ExecutorConfigFunctionType



.. py:data:: ExecutorConfigType



.. py:data:: CONFIG_GROUP_NAME
   :value: 'aws_ecs_executor'



.. py:data:: RUN_TASK_KWARG_DEFAULTS



.. py:data:: CONFIG_DEFAULTS



.. py:class:: EcsQueuedTask


   Represents an ECS task that is queued. The task will be run in the next heartbeat.

   .. py:attribute:: key
      :type: airflow.models.taskinstance.TaskInstanceKey



   .. py:attribute:: command
      :type: CommandType



   .. py:attribute:: queue
      :type: str



   .. py:attribute:: executor_config
      :type: ExecutorConfigType



   .. py:attribute:: attempt_number
      :type: int




.. py:class:: EcsTaskInfo


   Contains information about a currently running ECS task.

   .. py:attribute:: cmd
      :type: CommandType



   .. py:attribute:: queue
      :type: str



   .. py:attribute:: config
      :type: ExecutorConfigType




.. py:class:: BaseConfigKeys


   Base Implementation of the Config Keys class. Implements iteration for child classes to inherit.

   .. py:method:: __iter__()



.. py:class:: RunTaskKwargsConfigKeys


   Bases: :py:obj:`BaseConfigKeys`

   Keys loaded into the config which are valid ECS run_task kwargs.

   .. py:attribute:: ASSIGN_PUBLIC_IP
      :value: 'assign_public_ip'



   .. py:attribute:: CLUSTER
      :value: 'cluster'



   .. py:attribute:: LAUNCH_TYPE
      :value: 'launch_type'



   .. py:attribute:: PLATFORM_VERSION
      :value: 'platform_version'



   .. py:attribute:: SECURITY_GROUPS
      :value: 'security_groups'



   .. py:attribute:: SUBNETS
      :value: 'subnets'



   .. py:attribute:: TASK_DEFINITION
      :value: 'task_definition'



   .. py:attribute:: CONTAINER_NAME
      :value: 'container_name'




.. py:class:: AllEcsConfigKeys


   Bases: :py:obj:`RunTaskKwargsConfigKeys`

   All keys loaded into the config which are related to the ECS Executor.

   .. py:attribute:: MAX_RUN_TASK_ATTEMPTS
      :value: 'max_run_task_attempts'



   .. py:attribute:: AWS_CONN_ID
      :value: 'conn_id'



   .. py:attribute:: RUN_TASK_KWARGS
      :value: 'run_task_kwargs'



   .. py:attribute:: REGION_NAME
      :value: 'region_name'




.. py:exception:: EcsExecutorException


   Bases: :py:obj:`Exception`

   Thrown when something unexpected has occurred within the ECS ecosystem.


.. py:class:: EcsExecutorTask(task_arn, last_status, desired_status, containers, started_at = None, stopped_reason = None)


   Data Transfer Object for an ECS Fargate Task.

   .. py:method:: get_task_state()

      This is the primary logic that handles state in an ECS task.

      It will determine if a status is:
          QUEUED - Task is being provisioned.
          RUNNING - Task is launched on ECS.
          REMOVED - Task provisioning has failed for some reason. See `stopped_reason`.
          FAILED - Task is completed and at least one container has failed.
          SUCCESS - Task is completed and all containers have succeeded.


   .. py:method:: __repr__()

      Return repr(self).



.. py:class:: EcsTaskCollection


   A five-way dictionary between Airflow task ids, Airflow cmds, ECS ARNs, and ECS task objects.

   .. py:method:: add_task(task, airflow_task_key, queue, airflow_cmd, exec_config, attempt_number)

      Adds a task to the collection.


   .. py:method:: update_task(task)

      Updates the state of the given task based on task ARN.


   .. py:method:: task_by_key(task_key)

      Get a task by Airflow Instance Key.


   .. py:method:: task_by_arn(arn)

      Get a task by AWS ARN.


   .. py:method:: pop_by_key(task_key)

      Deletes task from collection based off of Airflow Task Instance Key.


   .. py:method:: get_all_arns()

      Get all AWS ARNs in collection.


   .. py:method:: get_all_task_keys()

      Get all Airflow Task Keys in collection.


   .. py:method:: failure_count_by_key(task_key)

      Get the number of times a task has failed given an Airflow Task Key.


   .. py:method:: increment_failure_count(task_key)

      Increment the failure counter given an Airflow Task Key.


   .. py:method:: info_by_key(task_key)

      Get the Airflow Command given an Airflow task key.


   .. py:method:: __getitem__(value)

      Gets a task by AWS ARN.


   .. py:method:: __len__()

      Determines the number of tasks in collection.



.. py:function:: parse_assign_public_ip(assign_public_ip)

   Convert "assign_public_ip" from True/False to ENABLE/DISABLE.


.. py:function:: camelize_dict_keys(nested_dict)

   Accept a potentially nested dictionary and recursively convert all keys into camelCase.
