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

:py:mod:`airflow.providers.amazon.aws.operators.ecs`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.ecs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.ecs.EcsBaseOperator
   airflow.providers.amazon.aws.operators.ecs.EcsCreateClusterOperator
   airflow.providers.amazon.aws.operators.ecs.EcsDeleteClusterOperator
   airflow.providers.amazon.aws.operators.ecs.EcsDeregisterTaskDefinitionOperator
   airflow.providers.amazon.aws.operators.ecs.EcsRegisterTaskDefinitionOperator
   airflow.providers.amazon.aws.operators.ecs.EcsRunTaskOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.ecs.DEFAULT_CONN_ID


.. py:data:: DEFAULT_CONN_ID
   :value: 'aws_default'



.. py:class:: EcsBaseOperator(*, aws_conn_id = DEFAULT_CONN_ID, region = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This is the base operator for all Elastic Container Service operators.

   .. py:method:: hook()

      Create and return an EcsHook.


   .. py:method:: client()

      Create and return the EcsHook's client.


   .. py:method:: execute(context)
      :abstractmethod:

      Must overwrite in child classes.



.. py:class:: EcsCreateClusterOperator(*, cluster_name, create_cluster_kwargs = None, wait_for_completion = True, waiter_delay = 15, waiter_max_attempts = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`EcsBaseOperator`

   Creates an AWS ECS cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EcsCreateClusterOperator`

   :param cluster_name: The name of your cluster. If you don't specify a name for your
       cluster, you create a cluster that's named default.
   :param create_cluster_kwargs: Extra arguments for Cluster Creation.
   :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
   :param waiter_delay: The amount of time in seconds to wait between attempts,
       if not set then the default waiter value will be used.
   :param waiter_max_attempts: The maximum number of attempts to be made,
       if not set then the default waiter value will be used.
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'create_cluster_kwargs', 'wait_for_completion', 'deferrable')



   .. py:method:: execute(context)

      Must overwrite in child classes.



.. py:class:: EcsDeleteClusterOperator(*, cluster_name, wait_for_completion = True, waiter_delay = 15, waiter_max_attempts = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`EcsBaseOperator`

   Deletes an AWS ECS cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EcsDeleteClusterOperator`

   :param cluster_name: The short name or full Amazon Resource Name (ARN) of the cluster to delete.
   :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
   :param waiter_delay: The amount of time in seconds to wait between attempts,
       if not set then the default waiter value will be used.
   :param waiter_max_attempts: The maximum number of attempts to be made,
       if not set then the default waiter value will be used.
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'wait_for_completion', 'deferrable')



   .. py:method:: execute(context)

      Must overwrite in child classes.



.. py:class:: EcsDeregisterTaskDefinitionOperator(*, task_definition, **kwargs)


   Bases: :py:obj:`EcsBaseOperator`

   Deregister a task definition on AWS ECS.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EcsDeregisterTaskDefinitionOperator`

   :param task_definition: The family and revision (family:revision) or full Amazon Resource Name (ARN)
       of the task definition to deregister. If you use a family name, you must specify a revision.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('task_definition',)



   .. py:method:: execute(context)

      Must overwrite in child classes.



.. py:class:: EcsRegisterTaskDefinitionOperator(*, family, container_definitions, register_task_kwargs = None, **kwargs)


   Bases: :py:obj:`EcsBaseOperator`

   Register a task definition on AWS ECS.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EcsRegisterTaskDefinitionOperator`

   :param family: The family name of a task definition to create.
   :param container_definitions: A list of container definitions in JSON format that describe
       the different containers that make up your task.
   :param register_task_kwargs: Extra arguments for Register Task Definition.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('family', 'container_definitions', 'register_task_kwargs')



   .. py:method:: execute(context)

      Must overwrite in child classes.



.. py:class:: EcsRunTaskOperator(*, task_definition, cluster, overrides, launch_type = 'EC2', capacity_provider_strategy = None, group = None, placement_constraints = None, placement_strategy = None, platform_version = None, network_configuration = None, tags = None, awslogs_group = None, awslogs_region = None, awslogs_stream_prefix = None, awslogs_fetch_interval = timedelta(seconds=30), propagate_tags = None, quota_retry = None, reattach = False, number_logs_exception = 10, wait_for_completion = True, waiter_delay = 6, waiter_max_attempts = 1000000 * 365 * 24 * 60 * 10, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`EcsBaseOperator`

   Execute a task on AWS ECS (Elastic Container Service).

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EcsRunTaskOperator`

   :param task_definition: the task definition name on Elastic Container Service
   :param cluster: the cluster name on Elastic Container Service
   :param overrides: the same parameter that boto3 will receive (templated):
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
   :param aws_conn_id: connection id of AWS credentials / region name. If None,
       credential boto3 strategy will be used
       (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html).
   :param region: region name to use in AWS Hook.
       Override the region in connection (if provided)
   :param launch_type: the launch type on which to run your task ('EC2', 'EXTERNAL', or 'FARGATE')
   :param capacity_provider_strategy: the capacity provider strategy to use for the task.
       When capacity_provider_strategy is specified, the launch_type parameter is omitted.
       If no capacity_provider_strategy or launch_type is specified,
       the default capacity provider strategy for the cluster is used.
   :param group: the name of the task group associated with the task
   :param placement_constraints: an array of placement constraint objects to use for
       the task
   :param placement_strategy: an array of placement strategy objects to use for
       the task
   :param platform_version: the platform version on which your task is running
   :param network_configuration: the network configuration for the task
   :param tags: a dictionary of tags in the form of {'tagKey': 'tagValue'}.
   :param awslogs_group: the CloudWatch group where your ECS container logs are stored.
       Only required if you want logs to be shown in the Airflow UI after your job has
       finished.
   :param awslogs_region: the region in which your CloudWatch logs are stored.
       If None, this is the same as the `region` parameter. If that is also None,
       this is the default AWS region based on your connection settings.
   :param awslogs_stream_prefix: the stream prefix that is used for the CloudWatch logs.
       This is usually based on some custom name combined with the name of the container.
       Only required if you want logs to be shown in the Airflow UI after your job has
       finished.
   :param awslogs_fetch_interval: the interval that the ECS task log fetcher should wait
       in between each Cloudwatch logs fetches.
       If deferrable is set to True, that parameter is ignored and waiter_delay is used instead.
   :param quota_retry: Config if and how to retry the launch of a new ECS task, to handle
       transient errors.
   :param reattach: If set to True, will check if the task previously launched by the task_instance
       is already running. If so, the operator will attach to it instead of starting a new task.
       This is to avoid relaunching a new task when the connection drops between Airflow and ECS while
       the task is running (when the Airflow worker is restarted for example).
   :param number_logs_exception: Number of lines from the last Cloudwatch logs to return in the
       AirflowException if an ECS task is stopped (to receive Airflow alerts with the logs of what
       failed in the code running in ECS).
   :param wait_for_completion: If True, waits for creation of the cluster to complete. (default: True)
   :param waiter_delay: The amount of time in seconds to wait between attempts,
       if not set then the default waiter value will be used.
   :param waiter_max_attempts: The maximum number of attempts to be made,
       if not set then the default waiter value will be used.
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: ui_color
      :value: '#f0ede4'



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('task_definition', 'cluster', 'overrides', 'launch_type', 'capacity_provider_strategy',...



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Must overwrite in child classes.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.
