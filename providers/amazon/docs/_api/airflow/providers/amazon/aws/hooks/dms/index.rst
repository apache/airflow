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

:py:mod:`airflow.providers.amazon.aws.hooks.dms`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.dms


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.dms.DmsTaskWaiterStatus
   airflow.providers.amazon.aws.hooks.dms.DmsHook




.. py:class:: DmsTaskWaiterStatus


   Bases: :py:obj:`str`, :py:obj:`enum.Enum`

   Available AWS DMS Task Waiter statuses.

   .. py:attribute:: DELETED
      :value: 'deleted'



   .. py:attribute:: READY
      :value: 'ready'



   .. py:attribute:: RUNNING
      :value: 'running'



   .. py:attribute:: STOPPED
      :value: 'stopped'




.. py:class:: DmsHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Database Migration Service (DMS).

   Provide thin wrapper around
   :external+boto3:py:class:`boto3.client("dms") <DatabaseMigrationService.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: describe_replication_tasks(**kwargs)

      Describe replication tasks.

      .. seealso::
          - :external+boto3:py:meth:`DatabaseMigrationService.Client.describe_replication_tasks`

      :return: Marker and list of replication tasks


   .. py:method:: find_replication_tasks_by_arn(replication_task_arn, without_settings = False)

      Find and describe replication tasks by task ARN.

      .. seealso::
          - :external+boto3:py:meth:`DatabaseMigrationService.Client.describe_replication_tasks`

      :param replication_task_arn: Replication task arn
      :param without_settings: Indicates whether to return task information with settings.
      :return: list of replication tasks that match the ARN


   .. py:method:: get_task_status(replication_task_arn)

      Retrieve task status.

      :param replication_task_arn: Replication task ARN
      :return: Current task status


   .. py:method:: create_replication_task(replication_task_id, source_endpoint_arn, target_endpoint_arn, replication_instance_arn, migration_type, table_mappings, **kwargs)

      Create DMS replication task.

      .. seealso::
          - :external+boto3:py:meth:`DatabaseMigrationService.Client.create_replication_task`

      :param replication_task_id: Replication task id
      :param source_endpoint_arn: Source endpoint ARN
      :param target_endpoint_arn: Target endpoint ARN
      :param replication_instance_arn: Replication instance ARN
      :param table_mappings: Table mappings
      :param migration_type: Migration type ('full-load'|'cdc'|'full-load-and-cdc'), full-load by default.
      :return: Replication task ARN


   .. py:method:: start_replication_task(replication_task_arn, start_replication_task_type, **kwargs)

      Start replication task.

      .. seealso::
          - :external+boto3:py:meth:`DatabaseMigrationService.Client.start_replication_task`

      :param replication_task_arn: Replication task ARN
      :param start_replication_task_type: Replication task start type (default='start-replication')
          ('start-replication'|'resume-processing'|'reload-target')


   .. py:method:: stop_replication_task(replication_task_arn)

      Stop replication task.

      .. seealso::
          - :external+boto3:py:meth:`DatabaseMigrationService.Client.stop_replication_task`

      :param replication_task_arn: Replication task ARN


   .. py:method:: delete_replication_task(replication_task_arn)

      Start replication task deletion and waits for it to be deleted.

      .. seealso::
          - :external+boto3:py:meth:`DatabaseMigrationService.Client.delete_replication_task`

      :param replication_task_arn: Replication task ARN


   .. py:method:: wait_for_task_status(replication_task_arn, status)

      Wait for replication task to reach status; supported statuses: deleted, ready, running, stopped.

      :param status: Status to wait for
      :param replication_task_arn: Replication task ARN
