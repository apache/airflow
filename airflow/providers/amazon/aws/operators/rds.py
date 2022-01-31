#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import time
from typing import TYPE_CHECKING, List, Optional, Sequence

from mypy_boto3_rds.type_defs import TagTypeDef

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.utils.rds import RdsDbType

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RdsBaseOperator(BaseOperator):
    """Base operator that implements common functions for all operators"""

    ui_color = "#eeaa88"
    ui_fgcolor = "#ffffff"

    def __init__(self, *args, aws_conn_id: str = "aws_conn_id", hook_params: Optional[dict] = None, **kwargs):
        hook_params = hook_params or {}
        self.hook = RdsHook(aws_conn_id=aws_conn_id, **hook_params)
        super().__init__(*args, **kwargs)

        self._wait_interval = 60  # seconds

    def _describe_item(self, **kwargs) -> list:
        """Returns information about target item: snapshot, task or event"""
        raise NotImplementedError

    def _await_status(
        self,
        wait_statuses: Optional[List[str]] = None,
        ok_statuses: Optional[List[str]] = None,
        error_statuses: Optional[List[str]] = None,
        **kwargs,
    ) -> list:
        """
        Continuously gets item description from `_describe_item()` and waits until:
        - status is in `wait_statuses`
        - status not in `ok_statuses` and `error_statuses`
        - `_describe_item()` returns non-empty list
        """
        while True:
            items = self._describe_item(**kwargs)

            if len(items) == 0:
                break
            elif len(items) > 1:
                raise AirflowException(f"There is more than one item with the same identifier: {items}")

            if wait_statuses and items[0]['Status'] in wait_statuses:
                continue
            elif ok_statuses and items[0]['Status'] in ok_statuses:
                break
            elif error_statuses and items[0]['Status'] in error_statuses:
                raise AirflowException(f"Item has error status: {items}")

            time.sleep(self._wait_interval)

        return items

    def execute(self, context: 'Context') -> str:
        """Different implementations for snapshots, tasks and events"""
        raise NotImplementedError

    def on_kill(self) -> None:
        """Different implementations for snapshots, tasks and events"""
        raise NotImplementedError


class RdsCreateDbSnapshotOperator(RdsBaseOperator):
    """
    Creates a snapshot of a DB instance or DB cluster.
    The source DB instance or cluster must be in the available or storage-optimization state.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCreateDbSnapshotOperator`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :type db_type: RDSDbType
    :param db_identifier: The identifier of the instance or cluster that you want to create the snapshot of
    :type db_identifier: str
    :param db_snapshot_identifier: The identifier for the DB snapshot
    :type db_snapshot_identifier: str
    :param tags: A list of tags in format `[{"Key": "something", "Value": "something"},]
        `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
    :type tags: Sequence[TagTypeDef] or None
    """

    template_fields = ("db_snapshot_identifier", "db_instance_identifier", "tags")

    def __init__(
        self,
        *,
        db_type: str,
        db_identifier: str,
        db_snapshot_identifier: str,
        tags: Optional[Sequence[TagTypeDef]] = None,
        aws_conn_id: str = "aws_conn_id",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_type = RdsDbType(db_type)
        self.db_identifier = db_identifier
        self.db_snapshot_identifier = db_snapshot_identifier
        self.tags = tags or []

    def _describe_item(self, **kwargs) -> list:
        """Returns snapshot info"""
        if self.db_type.value == "instance":
            db_snapshots = self.hook.conn.describe_db_snapshots(
                DBInstanceIdentifier=self.db_identifier,
                DBSnapshotIdentifier=self.db_snapshot_identifier,
                **kwargs,
            )
            return db_snapshots['DBSnapshots']
        else:
            db_cluster_snapshots = self.hook.conn.describe_db_cluster_snapshots(
                DBClusterIdentifier=self.db_identifier,
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
                **kwargs,
            )
            return db_cluster_snapshots['DBClusterSnapshots']

    def execute(self, context: 'Context') -> str:
        self.log.info(
            "Starting to create snapshot of RDS %s '%s': %s",
            self.db_type,
            self.db_identifier,
            self.db_snapshot_identifier,
        )

        if self.db_type.value == "instance":
            create_db_snap = self.hook.conn.create_db_snapshot(
                DBInstanceIdentifier=self.db_identifier,
                DBSnapshotIdentifier=self.db_snapshot_identifier,
                Tags=self.tags,
            )
            create_response = json.dumps(create_db_snap, default=str)
        else:
            create_db_cluster_snap = self.hook.conn.create_db_cluster_snapshot(
                DBClusterIdentifier=self.db_identifier,
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
                Tags=self.tags,
            )
            create_response = json.dumps(create_db_cluster_snap, default=str)

        self._await_status(wait_statuses=['creating'], ok_statuses=['available'])

        return create_response


class RdsCopyDbSnapshotOperator(RdsBaseOperator):
    """
    Copies the specified DB instance or DB cluster snapshot

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCopyDbSnapshotOperator`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :type db_type: RDSDbType
    :param source_db_snapshot_identifier: The identifier of the source snapshot
    :type source_db_snapshot_identifier: str
    :param target_db_snapshot_identifier: The identifier of the target snapshot
    :type target_db_snapshot_identifier: str
    :param kms_key_id: The AWS KMS key identifier for an encrypted DB snapshot
    :type kms_key_id: str or None
    :param tags: A list of tags in format `[{"Key": "something", "Value": "something"},]
        `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
    :type tags: Sequence[TagTypeDef] or None
    :param copy_tags: Whether to copy all tags from the source snapshot to the target snapshot (default False)
    :type copy_tags: bool or None
    :param pre_signed_url: The URL that contains a Signature Version 4 signed request
    :type pre_signed_url: str or None
    :param option_group_name: The name of an option group to associate with the copy of the snapshot
        Only when db_type='instance'
    :type option_group_name: str or None
    :param target_custom_availability_zone: The external custom Availability Zone identifier for the target
        Only when db_type='instance'
    :type target_custom_availability_zone: str or None
    :param source_region: The ID of the region that contains the snapshot to be copied
    :type source_region: str or None
    """

    template_fields = (
        "source_db_snapshot_identifier",
        "target_db_snapshot_identifier",
        "tags",
        "pre_signed_url",
        "option_group_name",
    )

    def __init__(
        self,
        *,
        db_type: str,
        source_db_snapshot_identifier: str,
        target_db_snapshot_identifier: str,
        kms_key_id: str = "",
        tags: Optional[Sequence[TagTypeDef]] = None,
        copy_tags: bool = False,
        pre_signed_url: str = "",
        option_group_name: str = "",
        target_custom_availability_zone: str = "",
        source_region: str = "",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.db_type = RdsDbType(db_type)
        self.source_db_snapshot_identifier = source_db_snapshot_identifier
        self.target_db_snapshot_identifier = target_db_snapshot_identifier
        self.kms_key_id = kms_key_id
        self.tags = tags or []
        self.copy_tags = copy_tags
        self.pre_signed_url = pre_signed_url
        self.option_group_name = option_group_name
        self.target_custom_availability_zone = target_custom_availability_zone
        self.source_region = source_region

    def _describe_item(self, **kwargs) -> list:
        """Returns existing snapshots"""
        if self.db_type.value == "instance":
            db_snapshots = self.hook.conn.describe_db_snapshots(
                DBSnapshotIdentifier=self.target_db_snapshot_identifier, **kwargs
            )
            return db_snapshots['DBSnapshots']
        else:
            db_cluster_snapshots = self.hook.conn.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=self.target_db_snapshot_identifier, **kwargs
            )
            return db_cluster_snapshots['DBClusterSnapshots']

    def execute(self, context: 'Context') -> str:
        self.log.info(
            "Starting to copy snapshot '%s' as '%s'",
            self.source_db_snapshot_identifier,
            self.target_db_snapshot_identifier,
        )

        if self.db_type.value == "instance":
            copy_db_snap = self.hook.conn.copy_db_snapshot(
                SourceDBSnapshotIdentifier=self.source_db_snapshot_identifier,
                TargetDBSnapshotIdentifier=self.target_db_snapshot_identifier,
                KmsKeyId=self.kms_key_id,
                Tags=self.tags,
                CopyTags=self.copy_tags,
                PreSignedUrl=self.pre_signed_url,
                OptionGroupName=self.option_group_name,
                TargetCustomAvailabilityZone=self.target_custom_availability_zone,
                SourceRegion=self.source_region,
            )
            copy_response = json.dumps(copy_db_snap, default=str)
        else:
            copy_db_cluster_snap = self.hook.conn.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=self.source_db_snapshot_identifier,
                TargetDBClusterSnapshotIdentifier=self.target_db_snapshot_identifier,
                KmsKeyId=self.kms_key_id,
                Tags=self.tags,
                CopyTags=self.copy_tags,
                PreSignedUrl=self.pre_signed_url,
                SourceRegion=self.source_region,
            )
            copy_response = json.dumps(copy_db_cluster_snap, default=str)

        self._await_status(wait_statuses=['copying'], ok_statuses=['available'])

        return copy_response


class RdsDeleteDbSnapshotOperator(RdsBaseOperator):
    """
    Deletes a DB instance or cluster snapshot or terminating the copy operation

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsDeleteDbSnapshotOperator`

    :param db_snapshot_identifier: The identifier for the DB instance or DB cluster snapshot
    :type db_snapshot_identifier: str
    """

    template_fields = ("db_snapshot_identifier",)

    def __init__(
        self,
        *,
        db_type: str,
        db_snapshot_identifier: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.db_type = RdsDbType(db_type)
        self.db_snapshot_identifier = db_snapshot_identifier

    def _describe_item(self, **kwargs):
        """Returns existing snapshots"""
        if self.db_type.value == "instance":
            db_snapshots = self.hook.conn.describe_db_snapshots(
                DBSnapshotIdentifier=self.db_snapshot_identifier, **kwargs
            )
            return db_snapshots['DBSnapshots']
        else:
            db_cluster_snapshots = self.hook.conn.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier, **kwargs
            )
            return db_cluster_snapshots['DBClusterSnapshots']

    def execute(self, context: 'Context') -> str:
        self.log.info("Starting to delete snapshot '%s'", self.db_snapshot_identifier)

        if self.db_type.value == "instance":
            delete_db_snap = self.hook.conn.delete_db_snapshot(
                DBSnapshotIdentifier=self.db_snapshot_identifier,
            )
            delete_response = json.dumps(delete_db_snap, default=str)
        else:
            delete_db_cluster_snap = self.hook.conn.delete_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
            )
            delete_response = json.dumps(delete_db_cluster_snap, default=str)

        return delete_response


class RdsStartExportTaskOperator(RdsBaseOperator):
    """
    Starts an export of a snapshot to Amazon S3. The provided IAM role must have access to the S3 bucket.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsStartExportTaskOperator`

    :param export_task_identifier: A unique identifier for the snapshot export task.
    :type export_task_identifier: str
    :param source_arn: The Amazon Resource Name (ARN) of the snapshot to export to Amazon S3.
    :type source_arn: str
    :param s3_bucket_name: The name of the Amazon S3 bucket to export the snapshot to.
    :type s3_bucket_name: str
    :param iam_role_arn: The name of the IAM role to use for writing to the Amazon S3 bucket.
    :type iam_role_arn: str
    :param kms_key_id: The ID of the Amazon Web Services KMS key to use to encrypt the snapshot.
    :type kms_key_id: str
    :param s3_prefix: The Amazon S3 bucket prefix to use as the file name and path of the exported snapshot.
    :type s3_prefix: str or None
    :param export_only: The data to be exported from the snapshot.
    :type export_only: List[str] or None
    """

    template_fields = (
        "export_task_identifier",
        "source_arn",
        "s3_bucket_name",
        "iam_role_arn",
        "kms_key_id",
        "s3_prefix",
        "export_only",
    )

    def __init__(
        self,
        *,
        export_task_identifier: str,
        source_arn: str,
        s3_bucket_name: str,
        iam_role_arn: str,
        kms_key_id: str,
        s3_prefix: str = '',
        export_only: Optional[List[str]] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.export_task_identifier = export_task_identifier
        self.source_arn = source_arn
        self.s3_bucket_name = s3_bucket_name
        self.iam_role_arn = iam_role_arn
        self.kms_key_id = kms_key_id
        self.s3_prefix = s3_prefix
        self.export_only = export_only or []

    def _describe_item(self, **kwargs) -> list:
        response = self.hook.conn.describe_export_tasks(
            ExportTaskIdentifier=self.export_task_identifier,
            SourceArn=self.source_arn,
        )
        return response['ExportTasks']

    def execute(self, context: 'Context') -> str:
        self.log.info("Starting export task %s for snapshot %s", self.export_task_identifier, self.source_arn)

        start_response = self.hook.conn.start_export_task(
            ExportTaskIdentifier=self.export_task_identifier,
            SourceArn=self.source_arn,
            S3BucketName=self.s3_bucket_name,
            IamRoleArn=self.iam_role_arn,
            KmsKeyId=self.kms_key_id,
            S3Prefix=self.s3_prefix,
            ExportOnly=self.export_only,
        )

        self._await_status(
            wait_statuses=['starting', 'in_progress'],
            ok_statuses=['available', 'complete'],
        )

        return json.dumps(start_response, default=str)


class RdsCancelExportTaskOperator(RdsBaseOperator):
    """
    Cancels an export task in progress that is exporting a snapshot to Amazon S3

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCancelExportTaskOperator`

    :param export_task_identifier: The identifier of the snapshot export task to cancel
    :type export_task_identifier: str
    """

    template_fields = ("export_task_identifier",)

    def __init__(
        self,
        *,
        export_task_identifier: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.export_task_identifier = export_task_identifier

    def _describe_item(self, **kwargs) -> list:
        response = self.hook.conn.describe_export_tasks(
            ExportTaskIdentifier=self.export_task_identifier,
        )
        return response['ExportTasks']

    def execute(self, context: 'Context') -> str:
        self.log.info("Canceling export task %s", self.export_task_identifier)

        cancel_response = self.hook.conn.cancel_export_task(
            ExportTaskIdentifier=self.export_task_identifier,
        )
        self._await_status(wait_statuses=['canceling'], ok_statuses=['canceled'])

        return json.dumps(cancel_response, default=str)


class RdsCreateEventSubscriptionOperator(RdsBaseOperator):
    """
    Creates an RDS event notification subscription

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCreateEventSubscriptionOperator`

    :param subscription_name: The name of the subscription (must be less than 255 characters)
    :type subscription_name: str
    :param sns_topic_arn: The ARN of the SNS topic created for event notification
    :type sns_topic_arn: str
    :param source_type: The type of source that is generating the events. Valid values: db-instance |
        db-cluster | db-parameter-group | db-security-group | db-snapshot | db-cluster-snapshot | db-proxy
    :type source_type: str or None
    :param event_categories: A list of event categories for a source type that you want to subscribe to
        `USER Events <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.Messages.html>`__
    :type event_categories: Sequence[str] or None
    :param source_ids: The list of identifiers of the event sources for which events are returned
    :type source_ids: Sequence[str] or None
    :param enabled: A value that indicates whether to activate the subscription (default True)
    :type enabled: bool
    :param tags: A list of tags in format `[{"Key": "something", "Value": "something"},]
        `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
    :type tags: Sequence[TagTypeDef] or None
    """

    template_fields = (
        "subscription_name",
        "sns_topic_arn",
        "source_type",
        "event_categories",
        "source_ids",
        "tags",
    )

    def __init__(
        self,
        *,
        subscription_name: str,
        sns_topic_arn: str,
        source_type: str = "",
        event_categories: Optional[Sequence[str]] = None,
        source_ids: Optional[Sequence[str]] = None,
        enabled: bool = True,
        tags: Optional[Sequence[TagTypeDef]] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.subscription_name = subscription_name
        self.sns_topic_arn = sns_topic_arn
        self.source_type = source_type
        self.event_categories = event_categories or []
        self.source_ids = source_ids or []
        self.enabled = enabled
        self.tags = tags or []

    def _describe_item(self, **kwargs) -> list:
        response = self.hook.conn.describe_event_subscriptions(
            SubscriptionName=self.subscription_name,
        )
        return response['EventSubscriptionsList']

    def execute(self, context: 'Context') -> str:
        self.log.info("Creating event subscription '%s' to '%s'", self.subscription_name, self.sns_topic_arn)

        create_response = self.hook.conn.create_event_subscription(
            SubscriptionName=self.subscription_name,
            SnsTopicArn=self.sns_topic_arn,
            SourceType=self.source_type,
            EventCategories=self.event_categories,
            SourceIds=self.source_ids,
            Enabled=self.enabled,
            Tags=self.tags,
        )
        self._await_status(
            wait_statuses=['creating'],
            ok_statuses=['created', 'available'],
        )

        return json.dumps(create_response, default=str)


class RdsDeleteEventSubscriptionOperator(RdsBaseOperator):
    """
    Deletes an RDS event notification subscription

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsDeleteEventSubscriptionOperator`

    :param subscription_name: The name of the RDS event notification subscription you want to delete
    :type subscription_name: str
    """

    template_fields = ("subscription_name",)

    def __init__(
        self,
        *,
        subscription_name: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.subscription_name = subscription_name

    def _describe_item(self, **kwargs) -> list:
        response = self.hook.conn.describe_event_subscriptions(
            SubscriptionName=self.subscription_name,
        )
        return response['EventSubscriptionsList']

    def execute(self, context: 'Context') -> str:
        self.log.info(
            "Deleting event subscription %s",
            self.subscription_name,
        )

        cancel_response = self.hook.conn.delete_event_subscription(
            SubscriptionName=self.subscription_name,
        )

        return json.dumps(cancel_response, default=str)


__all__ = [
    "RdsCreateDbSnapshotOperator",
    "RdsCopyDbSnapshotOperator",
    "RdsDeleteDbSnapshotOperator",
    "RdsCreateEventSubscriptionOperator",
    "RdsDeleteEventSubscriptionOperator",
    "RdsStartExportTaskOperator",
    "RdsCancelExportTaskOperator",
]
