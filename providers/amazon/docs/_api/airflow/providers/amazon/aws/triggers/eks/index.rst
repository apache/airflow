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

:py:mod:`airflow.providers.amazon.aws.triggers.eks`
===================================================

.. py:module:: airflow.providers.amazon.aws.triggers.eks


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.eks.EksCreateClusterTrigger
   airflow.providers.amazon.aws.triggers.eks.EksDeleteClusterTrigger
   airflow.providers.amazon.aws.triggers.eks.EksCreateFargateProfileTrigger
   airflow.providers.amazon.aws.triggers.eks.EksDeleteFargateProfileTrigger
   airflow.providers.amazon.aws.triggers.eks.EksCreateNodegroupTrigger
   airflow.providers.amazon.aws.triggers.eks.EksDeleteNodegroupTrigger




.. py:class:: EksCreateClusterTrigger(cluster_name, waiter_delay, waiter_max_attempts, aws_conn_id, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for EksCreateClusterOperator.

   The trigger will asynchronously wait for the cluster to be created.

   :param cluster_name: The name of the EKS cluster
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EksDeleteClusterTrigger(cluster_name, waiter_delay, waiter_max_attempts, aws_conn_id, region_name, force_delete_compute)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for EksDeleteClusterOperator.

   The trigger will asynchronously wait for the cluster to be deleted. If there are
   any nodegroups or fargate profiles associated with the cluster, they will be deleted
   before the cluster is deleted.

   :param cluster_name: The name of the EKS cluster
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: Which AWS region the connection should use.
        If this is None or empty then the default boto3 behaviour is used.
   :param force_delete_compute: If True, any nodegroups or fargate profiles associated
       with the cluster will be deleted before the cluster is deleted.

   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: hook()

      Override in subclasses to return the right hook.


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


   .. py:method:: delete_any_nodegroups(client)
      :async:

      Delete all EKS Nodegroups for a provided Amazon EKS Cluster.

      All the EKS Nodegroups are deleted simultaneously. We wait for
      all Nodegroups to be deleted before returning.


   .. py:method:: delete_any_fargate_profiles(client)
      :async:

      Delete all EKS Fargate profiles for a provided Amazon EKS Cluster.

      EKS Fargate profiles must be deleted one at a time, so we must wait
      for one to be deleted before sending the next delete command.



.. py:class:: EksCreateFargateProfileTrigger(cluster_name, fargate_profile_name, waiter_delay, waiter_max_attempts, aws_conn_id, region = None, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Asynchronously wait for the fargate profile to be created.

   :param cluster_name: The name of the EKS cluster
   :param fargate_profile_name: The name of the fargate profile
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EksDeleteFargateProfileTrigger(cluster_name, fargate_profile_name, waiter_delay, waiter_max_attempts, aws_conn_id, region = None, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Asynchronously wait for the fargate profile to be deleted.

   :param cluster_name: The name of the EKS cluster
   :param fargate_profile_name: The name of the fargate profile
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EksCreateNodegroupTrigger(cluster_name, nodegroup_name, waiter_delay, waiter_max_attempts, aws_conn_id, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for EksCreateNodegroupOperator.

   The trigger will asynchronously poll the boto3 API and wait for the
   nodegroup to be in the state specified by the waiter.

   :param waiter_name: Name of the waiter to use, for instance 'nodegroup_active' or 'nodegroup_deleted'
   :param cluster_name: The name of the EKS cluster associated with the node group.
   :param nodegroup_name: The name of the nodegroup to check.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: EksDeleteNodegroupTrigger(cluster_name, nodegroup_name, waiter_delay, waiter_max_attempts, aws_conn_id, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger for EksDeleteNodegroupOperator.

   The trigger will asynchronously poll the boto3 API and wait for the
   nodegroup to be in the state specified by the waiter.

   :param waiter_name: Name of the waiter to use, for instance 'nodegroup_active' or 'nodegroup_deleted'
   :param cluster_name: The name of the EKS cluster associated with the node group.
   :param nodegroup_name: The name of the nodegroup to check.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.

   .. py:method:: hook()

      Override in subclasses to return the right hook.
