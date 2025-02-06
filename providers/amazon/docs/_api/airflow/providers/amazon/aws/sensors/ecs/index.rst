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

:py:mod:`airflow.providers.amazon.aws.sensors.ecs`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.ecs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.ecs.EcsBaseSensor
   airflow.providers.amazon.aws.sensors.ecs.EcsClusterStateSensor
   airflow.providers.amazon.aws.sensors.ecs.EcsTaskDefinitionStateSensor
   airflow.providers.amazon.aws.sensors.ecs.EcsTaskStateSensor




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.ecs.DEFAULT_CONN_ID


.. py:data:: DEFAULT_CONN_ID
   :type: str
   :value: 'aws_default'



.. py:class:: EcsBaseSensor(*, aws_conn_id = DEFAULT_CONN_ID, region = None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Contains general sensor behavior for Elastic Container Service.

   .. py:method:: hook()

      Create and return an EcsHook.


   .. py:method:: client()

      Create and return an EcsHook client.



.. py:class:: EcsClusterStateSensor(*, cluster_name, target_state = EcsClusterStates.ACTIVE, failure_states = None, **kwargs)


   Bases: :py:obj:`EcsBaseSensor`

   Poll the cluster state until it reaches a terminal state; raises AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/sensor:EcsClusterStateSensor`

   :param cluster_name: The name of your cluster.
   :param target_state: Success state to watch for. (Default: "ACTIVE")
   :param failure_states: Fail if any of these states are reached before the
        Success State. (Default: "FAILED" or "INACTIVE")

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'target_state', 'failure_states')



   .. py:method:: poke(context)

      Override when deriving this class.



.. py:class:: EcsTaskDefinitionStateSensor(*, task_definition, target_state = EcsTaskDefinitionStates.ACTIVE, **kwargs)


   Bases: :py:obj:`EcsBaseSensor`

   Poll task definition until it reaches a terminal state; raise AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/sensor:EcsTaskDefinitionStateSensor`

   :param task_definition: The family for the latest ACTIVE revision, family and
        revision (family:revision ) for a specific revision in the family, or full
        Amazon Resource Name (ARN) of the task definition.
   :param target_state: Success state to watch for. (Default: "ACTIVE")

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('task_definition', 'target_state', 'failure_states')



   .. py:method:: poke(context)

      Override when deriving this class.



.. py:class:: EcsTaskStateSensor(*, cluster, task, target_state = EcsTaskStates.RUNNING, failure_states = None, **kwargs)


   Bases: :py:obj:`EcsBaseSensor`

   Poll the task state until it reaches a terminal state; raises AirflowException with the failure reason.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/sensor:EcsTaskStateSensor`

   :param cluster: The short name or full Amazon Resource Name (ARN) of the cluster that hosts the task.
   :param task: The task ID or full ARN of the task to poll.
   :param target_state: Success state to watch for. (Default: "ACTIVE")
   :param failure_states: Fail if any of these states are reached before
        the Success State. (Default: "STOPPED")

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster', 'task', 'target_state', 'failure_states')



   .. py:method:: poke(context)

      Override when deriving this class.
