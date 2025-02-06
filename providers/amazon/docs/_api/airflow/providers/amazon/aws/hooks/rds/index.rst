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

:py:mod:`airflow.providers.amazon.aws.hooks.rds`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.rds

.. autoapi-nested-parse::

   Interact with AWS RDS.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.rds.RdsHook




.. py:class:: RdsHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook`\ [\ :py:obj:`mypy_boto3_rds.RDSClient`\ ]

   Interact with Amazon Relational Database Service (RDS).

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("rds") <RDS.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       - `Amazon RDS and Aurora Documentation         <https://docs.aws.amazon.com/rds/index.html>`__

   .. py:method:: get_db_snapshot_state(snapshot_id)

      Get the current state of a DB instance snapshot.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_snapshots`

      :param snapshot_id: The ID of the target DB instance snapshot
      :return: Returns the status of the DB snapshot as a string (eg. "available")
      :raises AirflowNotFoundException: If the DB instance snapshot does not exist.


   .. py:method:: wait_for_db_snapshot_state(snapshot_id, target_state, check_interval = 30, max_attempts = 40)

      Poll DB Snapshots until target_state is reached; raise AirflowException after max_attempts.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_snapshots`

      :param snapshot_id: The ID of the target DB instance snapshot
      :param target_state: Wait until this state is reached
      :param check_interval: The amount of time in seconds to wait between attempts
      :param max_attempts: The maximum number of attempts to be made


   .. py:method:: get_db_cluster_snapshot_state(snapshot_id)

      Get the current state of a DB cluster snapshot.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_cluster_snapshots`

      :param snapshot_id: The ID of the target DB cluster.
      :return: Returns the status of the DB cluster snapshot as a string (eg. "available")
      :raises AirflowNotFoundException: If the DB cluster snapshot does not exist.


   .. py:method:: wait_for_db_cluster_snapshot_state(snapshot_id, target_state, check_interval = 30, max_attempts = 40)

      Poll DB Cluster Snapshots until target_state is reached; raise AirflowException after a max_attempts.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_cluster_snapshots`

      :param snapshot_id: The ID of the target DB cluster snapshot
      :param target_state: Wait until this state is reached
      :param check_interval: The amount of time in seconds to wait between attempts
      :param max_attempts: The maximum number of attempts to be made


   .. py:method:: get_export_task_state(export_task_id)

      Get the current state of an RDS snapshot export to Amazon S3.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_export_tasks`

      :param export_task_id: The identifier of the target snapshot export task.
      :return: Returns the status of the snapshot export task as a string (eg. "canceled")
      :raises AirflowNotFoundException: If the export task does not exist.


   .. py:method:: wait_for_export_task_state(export_task_id, target_state, check_interval = 30, max_attempts = 40)

      Poll export tasks until target_state is reached; raise AirflowException after max_attempts.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_export_tasks`

      :param export_task_id: The identifier of the target snapshot export task.
      :param target_state: Wait until this state is reached
      :param check_interval: The amount of time in seconds to wait between attempts
      :param max_attempts: The maximum number of attempts to be made


   .. py:method:: get_event_subscription_state(subscription_name)

      Get the current state of an RDS snapshot export to Amazon S3.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_event_subscriptions`

      :param subscription_name: The name of the target RDS event notification subscription.
      :return: Returns the status of the event subscription as a string (eg. "active")
      :raises AirflowNotFoundException: If the event subscription does not exist.


   .. py:method:: wait_for_event_subscription_state(subscription_name, target_state, check_interval = 30, max_attempts = 40)

      Poll Event Subscriptions until target_state is reached; raise AirflowException after max_attempts.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_event_subscriptions`

      :param subscription_name: The name of the target RDS event notification subscription.
      :param target_state: Wait until this state is reached
      :param check_interval: The amount of time in seconds to wait between attempts
      :param max_attempts: The maximum number of attempts to be made


   .. py:method:: get_db_instance_state(db_instance_id)

      Get the current state of a DB instance.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_instances`

      :param db_instance_id: The ID of the target DB instance.
      :return: Returns the status of the DB instance as a string (eg. "available")
      :raises AirflowNotFoundException: If the DB instance does not exist.


   .. py:method:: wait_for_db_instance_state(db_instance_id, target_state, check_interval = 30, max_attempts = 40)

      Poll DB Instances until target_state is reached; raise AirflowException after max_attempts.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_instances`

      :param db_instance_id: The ID of the target DB instance.
      :param target_state: Wait until this state is reached
      :param check_interval: The amount of time in seconds to wait between attempts
      :param max_attempts: The maximum number of attempts to be made


   .. py:method:: get_db_cluster_state(db_cluster_id)

      Get the current state of a DB cluster.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_clusters`

      :param db_cluster_id: The ID of the target DB cluster.
      :return: Returns the status of the DB cluster as a string (eg. "available")
      :raises AirflowNotFoundException: If the DB cluster does not exist.


   .. py:method:: wait_for_db_cluster_state(db_cluster_id, target_state, check_interval = 30, max_attempts = 40)

      Poll DB Clusters until target_state is reached; raise AirflowException after max_attempts.

      .. seealso::
          - :external+boto3:py:meth:`RDS.Client.describe_db_clusters`

      :param db_cluster_id: The ID of the target DB cluster.
      :param target_state: Wait until this state is reached
      :param check_interval: The amount of time in seconds to wait between attempts
      :param max_attempts: The maximum number of attempts to be made
