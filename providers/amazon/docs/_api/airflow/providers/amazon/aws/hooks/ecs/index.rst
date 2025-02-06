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

:py:mod:`airflow.providers.amazon.aws.hooks.ecs`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.ecs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ecs.EcsClusterStates
   airflow.providers.amazon.aws.hooks.ecs.EcsTaskDefinitionStates
   airflow.providers.amazon.aws.hooks.ecs.EcsTaskStates
   airflow.providers.amazon.aws.hooks.ecs.EcsHook
   airflow.providers.amazon.aws.hooks.ecs.EcsProtocol



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ecs.should_retry
   airflow.providers.amazon.aws.hooks.ecs.should_retry_eni



.. py:function:: should_retry(exception)

   Check if exception is related to ECS resource quota (CPU, MEM).


.. py:function:: should_retry_eni(exception)

   Check if exception is related to ENI (Elastic Network Interfaces).


.. py:class:: EcsClusterStates


   Bases: :py:obj:`airflow.providers.amazon.aws.utils._StringCompareEnum`

   Contains the possible State values of an ECS Cluster.

   .. py:attribute:: ACTIVE
      :value: 'ACTIVE'



   .. py:attribute:: PROVISIONING
      :value: 'PROVISIONING'



   .. py:attribute:: DEPROVISIONING
      :value: 'DEPROVISIONING'



   .. py:attribute:: FAILED
      :value: 'FAILED'



   .. py:attribute:: INACTIVE
      :value: 'INACTIVE'




.. py:class:: EcsTaskDefinitionStates


   Bases: :py:obj:`airflow.providers.amazon.aws.utils._StringCompareEnum`

   Contains the possible State values of an ECS Task Definition.

   .. py:attribute:: ACTIVE
      :value: 'ACTIVE'



   .. py:attribute:: INACTIVE
      :value: 'INACTIVE'



   .. py:attribute:: DELETE_IN_PROGRESS
      :value: 'DELETE_IN_PROGRESS'




.. py:class:: EcsTaskStates


   Bases: :py:obj:`airflow.providers.amazon.aws.utils._StringCompareEnum`

   Contains the possible State values of an ECS Task.

   .. py:attribute:: PROVISIONING
      :value: 'PROVISIONING'



   .. py:attribute:: PENDING
      :value: 'PENDING'



   .. py:attribute:: ACTIVATING
      :value: 'ACTIVATING'



   .. py:attribute:: RUNNING
      :value: 'RUNNING'



   .. py:attribute:: DEACTIVATING
      :value: 'DEACTIVATING'



   .. py:attribute:: STOPPING
      :value: 'STOPPING'



   .. py:attribute:: DEPROVISIONING
      :value: 'DEPROVISIONING'



   .. py:attribute:: STOPPED
      :value: 'STOPPED'



   .. py:attribute:: NONE
      :value: 'NONE'




.. py:class:: EcsHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`

   Interact with Amazon Elastic Container Service (ECS).

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("ecs") <ECS.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       - `Amazon Elastic Container Service         <https://docs.aws.amazon.com/AmazonECS/latest/APIReference/Welcome.html>`__

   .. py:method:: get_cluster_state(cluster_name)

      Get ECS Cluster state.

      .. seealso::
          - :external+boto3:py:meth:`ECS.Client.describe_clusters`

      :param cluster_name: ECS Cluster name or full cluster Amazon Resource Name (ARN) entry.


   .. py:method:: get_task_definition_state(task_definition)

      Get ECS Task Definition state.

      .. seealso::
          - :external+boto3:py:meth:`ECS.Client.describe_task_definition`

      :param task_definition: The family for the latest ACTIVE revision,
          family and revision ( family:revision ) for a specific revision in the family,
          or full Amazon Resource Name (ARN) of the task definition to describe.


   .. py:method:: get_task_state(cluster, task)

      Get ECS Task state.

      .. seealso::
          - :external+boto3:py:meth:`ECS.Client.describe_tasks`

      :param cluster: The short name or full Amazon Resource Name (ARN)
          of the cluster that hosts the task or tasks to describe.
      :param task: Task ID or full ARN entry.



.. py:class:: EcsProtocol


   Bases: :py:obj:`airflow.typing_compat.Protocol`

   A structured Protocol for ``boto3.client('ecs')``.

   This is used for type hints on :py:meth:`.EcsOperator.client`.

   .. seealso::

       - https://mypy.readthedocs.io/en/latest/protocols.html
       - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html

   .. py:method:: run_task(**kwargs)

      Run a task.

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task


   .. py:method:: get_waiter(x)

      Get a waiter.

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.get_waiter


   .. py:method:: describe_tasks(cluster, tasks)

      Describe tasks.

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_tasks


   .. py:method:: stop_task(cluster, task, reason)

      Stop a task.

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.stop_task


   .. py:method:: describe_task_definition(taskDefinition)

      Describe a task definition.

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.describe_task_definition


   .. py:method:: list_tasks(cluster, launchType, desiredStatus, family)

      List tasks.

      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.list_tasks
