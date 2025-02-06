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

:py:mod:`airflow.providers.amazon.aws.operators.rds`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.rds


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.rds.RdsCreateDbSnapshotOperator
   airflow.providers.amazon.aws.operators.rds.RdsCopyDbSnapshotOperator
   airflow.providers.amazon.aws.operators.rds.RdsDeleteDbSnapshotOperator
   airflow.providers.amazon.aws.operators.rds.RdsStartExportTaskOperator
   airflow.providers.amazon.aws.operators.rds.RdsCancelExportTaskOperator
   airflow.providers.amazon.aws.operators.rds.RdsCreateEventSubscriptionOperator
   airflow.providers.amazon.aws.operators.rds.RdsDeleteEventSubscriptionOperator
   airflow.providers.amazon.aws.operators.rds.RdsCreateDbInstanceOperator
   airflow.providers.amazon.aws.operators.rds.RdsDeleteDbInstanceOperator
   airflow.providers.amazon.aws.operators.rds.RdsStartDbOperator
   airflow.providers.amazon.aws.operators.rds.RdsStopDbOperator




.. py:class:: RdsCreateDbSnapshotOperator(*, db_type, db_identifier, db_snapshot_identifier, tags = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Creates a snapshot of a DB instance or DB cluster.

   The source DB instance or cluster must be in the available or storage-optimization state.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsCreateDbSnapshotOperator`

   :param db_type: Type of the DB - either "instance" or "cluster"
   :param db_identifier: The identifier of the instance or cluster that you want to create the snapshot of
   :param db_snapshot_identifier: The identifier for the DB snapshot
   :param tags: A dictionary of tags or a list of tags in format `[{"Key": "...", "Value": "..."},]`
       `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
   :param wait_for_completion:  If True, waits for creation of the DB snapshot to complete. (default: True)

   .. py:attribute:: template_fields
      :value: ('db_snapshot_identifier', 'db_identifier', 'tags')



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsCopyDbSnapshotOperator(*, db_type, source_db_snapshot_identifier, target_db_snapshot_identifier, kms_key_id = '', tags = None, copy_tags = False, pre_signed_url = '', option_group_name = '', target_custom_availability_zone = '', source_region = '', wait_for_completion = True, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Copies the specified DB instance or DB cluster snapshot.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsCopyDbSnapshotOperator`

   :param db_type: Type of the DB - either "instance" or "cluster"
   :param source_db_snapshot_identifier: The identifier of the source snapshot
   :param target_db_snapshot_identifier: The identifier of the target snapshot
   :param kms_key_id: The AWS KMS key identifier for an encrypted DB snapshot
   :param tags: A dictionary of tags or a list of tags in format `[{"Key": "...", "Value": "..."},]`
       `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
   :param copy_tags: Whether to copy all tags from the source snapshot to the target snapshot (default False)
   :param pre_signed_url: The URL that contains a Signature Version 4 signed request
   :param option_group_name: The name of an option group to associate with the copy of the snapshot
       Only when db_type='instance'
   :param target_custom_availability_zone: The external custom Availability Zone identifier for the target
       Only when db_type='instance'
   :param source_region: The ID of the region that contains the snapshot to be copied
   :param wait_for_completion:  If True, waits for snapshot copy to complete. (default: True)

   .. py:attribute:: template_fields
      :value: ('source_db_snapshot_identifier', 'target_db_snapshot_identifier', 'tags', 'pre_signed_url',...



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsDeleteDbSnapshotOperator(*, db_type, db_snapshot_identifier, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Deletes a DB instance or cluster snapshot or terminating the copy operation.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsDeleteDbSnapshotOperator`

   :param db_type: Type of the DB - either "instance" or "cluster"
   :param db_snapshot_identifier: The identifier for the DB instance or DB cluster snapshot

   .. py:attribute:: template_fields
      :value: ('db_snapshot_identifier',)



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsStartExportTaskOperator(*, export_task_identifier, source_arn, s3_bucket_name, iam_role_arn, kms_key_id, s3_prefix = '', export_only = None, wait_for_completion = True, waiter_interval = 30, waiter_max_attempts = 40, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Starts an export of a snapshot to Amazon S3. The provided IAM role must have access to the S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsStartExportTaskOperator`

   :param export_task_identifier: A unique identifier for the snapshot export task.
   :param source_arn: The Amazon Resource Name (ARN) of the snapshot to export to Amazon S3.
   :param s3_bucket_name: The name of the Amazon S3 bucket to export the snapshot to.
   :param iam_role_arn: The name of the IAM role to use for writing to the Amazon S3 bucket.
   :param kms_key_id: The ID of the Amazon Web Services KMS key to use to encrypt the snapshot.
   :param s3_prefix: The Amazon S3 bucket prefix to use as the file name and path of the exported snapshot.
   :param export_only: The data to be exported from the snapshot.
   :param wait_for_completion:  If True, waits for the DB snapshot export to complete. (default: True)
   :param waiter_interval: The number of seconds to wait before checking the export status. (default: 30)
   :param waiter_max_attempts: The number of attempts to make before failing. (default: 40)

   .. py:attribute:: template_fields
      :value: ('export_task_identifier', 'source_arn', 's3_bucket_name', 'iam_role_arn', 'kms_key_id',...



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsCancelExportTaskOperator(*, export_task_identifier, wait_for_completion = True, check_interval = 30, max_attempts = 40, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Cancels an export task in progress that is exporting a snapshot to Amazon S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsCancelExportTaskOperator`

   :param export_task_identifier: The identifier of the snapshot export task to cancel
   :param wait_for_completion:  If True, waits for DB snapshot export to cancel. (default: True)
   :param check_interval: The amount of time in seconds to wait between attempts
   :param max_attempts: The maximum number of attempts to be made

   .. py:attribute:: template_fields
      :value: ('export_task_identifier',)



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsCreateEventSubscriptionOperator(*, subscription_name, sns_topic_arn, source_type = '', event_categories = None, source_ids = None, enabled = True, tags = None, wait_for_completion = True, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Creates an RDS event notification subscription.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsCreateEventSubscriptionOperator`

   :param subscription_name: The name of the subscription (must be less than 255 characters)
   :param sns_topic_arn: The ARN of the SNS topic created for event notification
   :param source_type: The type of source that is generating the events. Valid values: db-instance |
       db-cluster | db-parameter-group | db-security-group | db-snapshot | db-cluster-snapshot | db-proxy
   :param event_categories: A list of event categories for a source type that you want to subscribe to
       `USER Events <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.Messages.html>`__
   :param source_ids: The list of identifiers of the event sources for which events are returned
   :param enabled: A value that indicates whether to activate the subscription (default True)l
   :param tags: A dictionary of tags or a list of tags in format `[{"Key": "...", "Value": "..."},]`
       `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
   :param wait_for_completion:  If True, waits for creation of the subscription to complete. (default: True)

   .. py:attribute:: template_fields
      :value: ('subscription_name', 'sns_topic_arn', 'source_type', 'event_categories', 'source_ids', 'tags')



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsDeleteEventSubscriptionOperator(*, subscription_name, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Deletes an RDS event notification subscription.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsDeleteEventSubscriptionOperator`

   :param subscription_name: The name of the RDS event notification subscription you want to delete

   .. py:attribute:: template_fields
      :value: ('subscription_name',)



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.



.. py:class:: RdsCreateDbInstanceOperator(*, db_instance_identifier, db_instance_class, engine, rds_kwargs = None, wait_for_completion = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), waiter_delay = 30, waiter_max_attempts = 60, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Creates an RDS DB instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsCreateDbInstanceOperator`

   :param db_instance_identifier: The DB instance identifier, must start with a letter and
       contain from 1 to 63 letters, numbers, or hyphens
   :param db_instance_class: The compute and memory capacity of the DB instance, for example db.m5.large
   :param engine: The name of the database engine to be used for this instance
   :param rds_kwargs: Named arguments to pass to boto3 RDS client function ``create_db_instance``
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_instance
   :param wait_for_completion:  If True, waits for creation of the DB instance to complete. (default: True)
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check DB instance state
   :param waiter_max_attempts: The maximum number of attempts to check DB instance state
   :param deferrable: If True, the operator will wait asynchronously for the DB instance to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :value: ('db_instance_identifier', 'db_instance_class', 'engine', 'rds_kwargs')



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: RdsDeleteDbInstanceOperator(*, db_instance_identifier, rds_kwargs = None, wait_for_completion = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), waiter_delay = 30, waiter_max_attempts = 60, **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Deletes an RDS DB Instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsDeleteDbInstanceOperator`

   :param db_instance_identifier: The DB instance identifier for the DB instance to be deleted
   :param rds_kwargs: Named arguments to pass to boto3 RDS client function ``delete_db_instance``
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.delete_db_instance
   :param wait_for_completion:  If True, waits for deletion of the DB instance to complete. (default: True)
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check DB instance state
   :param waiter_max_attempts: The maximum number of attempts to check DB instance state
   :param deferrable: If True, the operator will wait asynchronously for the DB instance to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :value: ('db_instance_identifier', 'rds_kwargs')



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: RdsStartDbOperator(*, db_identifier, db_type = RdsDbType.INSTANCE, wait_for_completion = True, waiter_delay = 30, waiter_max_attempts = 40, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Starts an RDS DB instance / cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsStartDbOperator`

   :param db_identifier: The AWS identifier of the DB to start
   :param db_type: Type of the DB - either "instance" or "cluster" (default: "instance")
   :param wait_for_completion:  If True, waits for DB to start. (default: True)
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check DB instance state
   :param waiter_max_attempts: The maximum number of attempts to check DB instance state
   :param deferrable: If True, the operator will wait asynchronously for the DB instance to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.

   .. py:attribute:: template_fields
      :value: ('db_identifier', 'db_type')



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.


   .. py:method:: execute_complete(context, event = None)



.. py:class:: RdsStopDbOperator(*, db_identifier, db_type = RdsDbType.INSTANCE, db_snapshot_identifier = None, wait_for_completion = True, waiter_delay = 30, waiter_max_attempts = 40, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`RdsBaseOperator`

   Stops an RDS DB instance / cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:RdsStopDbOperator`

   :param db_identifier: The AWS identifier of the DB to stop
   :param db_type: Type of the DB - either "instance" or "cluster" (default: "instance")
   :param db_snapshot_identifier: The instance identifier of the DB Snapshot to create before
       stopping the DB instance. The default value (None) skips snapshot creation. This
       parameter is ignored when ``db_type`` is "cluster"
   :param wait_for_completion:  If True, waits for DB to stop. (default: True)
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check DB instance state
   :param waiter_max_attempts: The maximum number of attempts to check DB instance state
   :param deferrable: If True, the operator will wait asynchronously for the DB instance to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.

   .. py:attribute:: template_fields
      :value: ('db_identifier', 'db_snapshot_identifier', 'db_type')



   .. py:method:: execute(context)

      Different implementations for snapshots, tasks and events.


   .. py:method:: execute_complete(context, event = None)
