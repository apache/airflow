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

:py:mod:`airflow.providers.amazon.aws.triggers.ecs`
===================================================

.. py:module:: airflow.providers.amazon.aws.triggers.ecs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.ecs.ClusterActiveTrigger
   airflow.providers.amazon.aws.triggers.ecs.ClusterInactiveTrigger
   airflow.providers.amazon.aws.triggers.ecs.TaskDoneTrigger




.. py:class:: ClusterActiveTrigger(cluster_arn, waiter_delay, waiter_max_attempts, aws_conn_id, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Polls the status of a cluster until it's active.

   :param cluster_arn: ARN of the cluster to watch.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The number of times to ping for status.
       Will fail after that many unsuccessful attempts.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: The AWS region where the cluster is located.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: ClusterInactiveTrigger(cluster_arn, waiter_delay, waiter_max_attempts, aws_conn_id, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Polls the status of a cluster until it's inactive.

   :param cluster_arn: ARN of the cluster to watch.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The number of times to ping for status.
       Will fail after that many unsuccessful attempts.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: The AWS region where the cluster is located.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: TaskDoneTrigger(cluster, task_arn, waiter_delay, waiter_max_attempts, aws_conn_id, region, log_group = None, log_stream = None)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Waits for an ECS task to be done, while eventually polling logs.

   :param cluster: short name or full ARN of the cluster where the task is running.
   :param task_arn: ARN of the task to watch.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The number of times to ping for status.
       Will fail after that many unsuccessful attempts.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region: The AWS region where the cluster is located.

   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: run()
      :async:

      Run the trigger in an asynchronous context.

      The trigger should yield an Event whenever it wants to fire off
      an event, and return None if it is finished. Single-event triggers
      should thus yield and then immediately return.

      If it yields, it is likely that it will be resumed very quickly,
      but it may not be (e.g. if the workload is being moved to another
      triggerer process, or a multi-event trigger was being used for a
      single-event task defer).

      In either case, Trigger classes should assume they will be persisted,
      and then rely on cleanup() being called when they are no longer needed.
