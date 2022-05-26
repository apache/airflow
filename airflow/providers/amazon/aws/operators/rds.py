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
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from mypy_boto3_rds.type_defs import ProcessorFeatureTypeDef, TagTypeDef

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

        self._await_interval = 60  # seconds

    def _describe_item(self, item_type: str, item_name: str) -> list:

        if item_type == 'instance_snapshot':
            db_snaps = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=item_name)
            return db_snaps['DBSnapshots']
        elif item_type == 'cluster_snapshot':
            cl_snaps = self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=item_name)
            return cl_snaps['DBClusterSnapshots']
        elif item_type == 'export_task':
            exports = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=item_name)
            return exports['ExportTasks']
        elif item_type == 'event_subscription':
            subscriptions = self.hook.conn.describe_event_subscriptions(SubscriptionName=item_name)
            return subscriptions['EventSubscriptionsList']
        else:
            raise AirflowException(f"Method for {item_type} is not implemented")

    def _await_status(
        self,
        item_type: str,
        item_name: str,
        wait_statuses: Optional[List[str]] = None,
        ok_statuses: Optional[List[str]] = None,
        error_statuses: Optional[List[str]] = None,
    ) -> None:
        """
        Continuously gets item description from `_describe_item()` and waits until:
        - status is in `wait_statuses`
        - status not in `ok_statuses` and `error_statuses`
        """
        while True:
            items = self._describe_item(item_type, item_name)

            if len(items) == 0:
                raise AirflowException(f"There is no {item_type} with identifier {item_name}")
            if len(items) > 1:
                raise AirflowException(f"There are {len(items)} {item_type} with identifier {item_name}")

            if wait_statuses and items[0]['Status'].lower() in wait_statuses:
                time.sleep(self._await_interval)
                continue
            elif ok_statuses and items[0]['Status'].lower() in ok_statuses:
                break
            elif error_statuses and items[0]['Status'].lower() in error_statuses:
                raise AirflowException(f"Item has error status ({error_statuses}): {items[0]}")
            else:
                raise AirflowException(f"Item has uncertain status: {items[0]}")

        return None

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
    :param db_identifier: The identifier of the instance or cluster that you want to create the snapshot of
    :param db_snapshot_identifier: The identifier for the DB snapshot
    :param tags: A list of tags in format `[{"Key": "something", "Value": "something"},]
        `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
    """

    template_fields = ("db_snapshot_identifier", "db_identifier", "tags")

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

    def execute(self, context: 'Context') -> str:
        self.log.info(
            "Starting to create snapshot of RDS %s '%s': %s",
            self.db_type,
            self.db_identifier,
            self.db_snapshot_identifier,
        )

        if self.db_type.value == "instance":
            create_instance_snap = self.hook.conn.create_db_snapshot(
                DBInstanceIdentifier=self.db_identifier,
                DBSnapshotIdentifier=self.db_snapshot_identifier,
                Tags=self.tags,
            )
            create_response = json.dumps(create_instance_snap, default=str)
            self._await_status(
                'instance_snapshot',
                self.db_snapshot_identifier,
                wait_statuses=['creating'],
                ok_statuses=['available'],
            )
        else:
            create_cluster_snap = self.hook.conn.create_db_cluster_snapshot(
                DBClusterIdentifier=self.db_identifier,
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
                Tags=self.tags,
            )
            create_response = json.dumps(create_cluster_snap, default=str)
            self._await_status(
                'cluster_snapshot',
                self.db_snapshot_identifier,
                wait_statuses=['creating'],
                ok_statuses=['available'],
            )

        return create_response


class RdsCopyDbSnapshotOperator(RdsBaseOperator):
    """
    Copies the specified DB instance or DB cluster snapshot

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCopyDbSnapshotOperator`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :param source_db_snapshot_identifier: The identifier of the source snapshot
    :param target_db_snapshot_identifier: The identifier of the target snapshot
    :param kms_key_id: The AWS KMS key identifier for an encrypted DB snapshot
    :param tags: A list of tags in format `[{"Key": "something", "Value": "something"},]
        `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
    :param copy_tags: Whether to copy all tags from the source snapshot to the target snapshot (default False)
    :param pre_signed_url: The URL that contains a Signature Version 4 signed request
    :param option_group_name: The name of an option group to associate with the copy of the snapshot
        Only when db_type='instance'
    :param target_custom_availability_zone: The external custom Availability Zone identifier for the target
        Only when db_type='instance'
    :param source_region: The ID of the region that contains the snapshot to be copied
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

    def execute(self, context: 'Context') -> str:
        self.log.info(
            "Starting to copy snapshot '%s' as '%s'",
            self.source_db_snapshot_identifier,
            self.target_db_snapshot_identifier,
        )

        if self.db_type.value == "instance":
            copy_instance_snap = self.hook.conn.copy_db_snapshot(
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
            copy_response = json.dumps(copy_instance_snap, default=str)
            self._await_status(
                'instance_snapshot',
                self.target_db_snapshot_identifier,
                wait_statuses=['creating'],
                ok_statuses=['available'],
            )
        else:
            copy_cluster_snap = self.hook.conn.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=self.source_db_snapshot_identifier,
                TargetDBClusterSnapshotIdentifier=self.target_db_snapshot_identifier,
                KmsKeyId=self.kms_key_id,
                Tags=self.tags,
                CopyTags=self.copy_tags,
                PreSignedUrl=self.pre_signed_url,
                SourceRegion=self.source_region,
            )
            copy_response = json.dumps(copy_cluster_snap, default=str)
            self._await_status(
                'cluster_snapshot',
                self.target_db_snapshot_identifier,
                wait_statuses=['copying'],
                ok_statuses=['available'],
            )

        return copy_response


class RdsDeleteDbSnapshotOperator(RdsBaseOperator):
    """
    Deletes a DB instance or cluster snapshot or terminating the copy operation

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsDeleteDbSnapshotOperator`

    :param db_type: Type of the DB - either "instance" or "cluster"
    :param db_snapshot_identifier: The identifier for the DB instance or DB cluster snapshot
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

    def execute(self, context: 'Context') -> str:
        self.log.info("Starting to delete snapshot '%s'", self.db_snapshot_identifier)

        if self.db_type.value == "instance":
            delete_instance_snap = self.hook.conn.delete_db_snapshot(
                DBSnapshotIdentifier=self.db_snapshot_identifier,
            )
            delete_response = json.dumps(delete_instance_snap, default=str)
        else:
            delete_cluster_snap = self.hook.conn.delete_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
            )
            delete_response = json.dumps(delete_cluster_snap, default=str)

        return delete_response


class RdsStartExportTaskOperator(RdsBaseOperator):
    """
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

    def execute(self, context: 'Context') -> str:
        self.log.info("Starting export task %s for snapshot %s", self.export_task_identifier, self.source_arn)

        start_export = self.hook.conn.start_export_task(
            ExportTaskIdentifier=self.export_task_identifier,
            SourceArn=self.source_arn,
            S3BucketName=self.s3_bucket_name,
            IamRoleArn=self.iam_role_arn,
            KmsKeyId=self.kms_key_id,
            S3Prefix=self.s3_prefix,
            ExportOnly=self.export_only,
        )

        self._await_status(
            'export_task',
            self.export_task_identifier,
            wait_statuses=['starting', 'in_progress'],
            ok_statuses=['complete'],
            error_statuses=['canceling', 'canceled'],
        )

        return json.dumps(start_export, default=str)


class RdsCancelExportTaskOperator(RdsBaseOperator):
    """
    Cancels an export task in progress that is exporting a snapshot to Amazon S3

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCancelExportTaskOperator`

    :param export_task_identifier: The identifier of the snapshot export task to cancel
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

    def execute(self, context: 'Context') -> str:
        self.log.info("Canceling export task %s", self.export_task_identifier)

        cancel_export = self.hook.conn.cancel_export_task(
            ExportTaskIdentifier=self.export_task_identifier,
        )
        self._await_status(
            'export_task',
            self.export_task_identifier,
            wait_statuses=['canceling'],
            ok_statuses=['canceled'],
        )

        return json.dumps(cancel_export, default=str)


class RdsCreateEventSubscriptionOperator(RdsBaseOperator):
    """
    Creates an RDS event notification subscription

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
    :param tags: A list of tags in format `[{"Key": "something", "Value": "something"},]
        `USER Tagging <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Tagging.html>`__
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

    def execute(self, context: 'Context') -> str:
        self.log.info("Creating event subscription '%s' to '%s'", self.subscription_name, self.sns_topic_arn)

        create_subscription = self.hook.conn.create_event_subscription(
            SubscriptionName=self.subscription_name,
            SnsTopicArn=self.sns_topic_arn,
            SourceType=self.source_type,
            EventCategories=self.event_categories,
            SourceIds=self.source_ids,
            Enabled=self.enabled,
            Tags=self.tags,
        )
        self._await_status(
            'event_subscription',
            self.subscription_name,
            wait_statuses=['creating'],
            ok_statuses=['active'],
        )

        return json.dumps(create_subscription, default=str)


class RdsDeleteEventSubscriptionOperator(RdsBaseOperator):
    """
    Deletes an RDS event notification subscription

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsDeleteEventSubscriptionOperator`

    :param subscription_name: The name of the RDS event notification subscription you want to delete
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

    def execute(self, context: 'Context') -> str:
        self.log.info(
            "Deleting event subscription %s",
            self.subscription_name,
        )

        delete_subscription = self.hook.conn.delete_event_subscription(
            SubscriptionName=self.subscription_name,
        )

        return json.dumps(delete_subscription, default=str)


class RdsCreateDbInstanceOperator(RdsBaseOperator):
    """
    Creates an RDS DB instance

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCreateDbInstanceOperator`

    :param db_instance_identifier: The DB instance identifier, must start with a letter and
        contain from 1 to 63 letters, numbers, or hyphens
    :param db_instance_class: The compute and memory capacity of the DB instance, for example db.m5.large
    :param engine: The name of the database engine to be used for this instance
    :param db_name: The name of the database to create when the DB instance is created
    :param allocated_storage: The amount of storage in GiB to allocate for the DB instance
    :param master_username: The name for the master user
    :param master_user_password: The password for the master user
    :param db_security_groups: A list of DB security groups to associate with this DB instance
    :param vpc_security_group_ids: A list of Amazon EC2 VPC security groups to associate with this DB instance
    :param availability_zone: The Availability Zone (AZ) where the database will be created
    :param db_subnet_group_name: A DB subnet group to associate with this DB instance
    :param preferred_maintenance_window: The time range each week in format during
        which system maintenance can occur
    :param db_parameter_group_name: The name of the DB parameter group to associate with this DB instance
    :param backup_retention_period: The number of days for which automated backups are retained
    :param preferred_backup_window: The daily time range during which automated backups are created
        if automated backups are enabled
    :param port: The port number on which the database accepts connections
    :param multi_az: A value that indicates whether the DB instance is a Multi-AZ deployment
    :param engine_version: The version number of the database engine to use
    :param auto_minor_version_upgrade: A value that indicates whether
        minor engine upgrades are applied automatically to the DB instance during the maintenance window
    :param license_model: License model information for this DB instance
    :param iops: The amount of Provisioned IOPS to be initially allocated for the DB instance
    :param option_group_name: A value that indicates that
        the DB instance should be associated with the specified option group
    :param character_set_name: For supported engines, this value indicates
        that the DB instance should be associated with the specified CharacterSet
    :param nchar_character_set_name: The name of the NCHAR character set for the Oracle DB instance
    :param publicly_accessible: A value that indicates whether the DB instance is publicly accessible
    :param tags: Tags to assign to the DB instance
    :param db_cluster_identifier: The identifier of the DB cluster that the instance will belong to
    :param storage_type: Specifies the storage type to be associated with the DB instance
    :param tde_credential_arn: The ARN from the key store
        with which to associate the instance for TDE encryption
    :param tde_credential_password: The password for the given ARN from the key store
        in order to access the device
    :param storage_encrypted: A value that indicates whether the DB instance is encrypted
    :param kms_key_id: The AWS KMS key identifier for an encrypted DB instance
    :param domain: The Active Directory directory ID to create the DB instance in
    :param copy_tags_to_snapshot: A value that indicates whether to copy tags
        from the DB instance to snapshots of the DB instance
    :param monitoring_interval: The interval, in seconds,
        between points when Enhanced Monitoring metrics are collected for the DB instance
    :param monitoring_role_arn: The ARN for the IAM role
        that permits RDS to send enhanced monitoring metrics to Amazon CloudWatch Logs
    :param domain_iam_role_name: Specify the name of the IAM role to be used
        when making API calls to the Directory Service
    :param promotion_tier: A value that specifies the order in which
        an Aurora Replica is promoted to the primary instance after a failure of the existing primary instance
    :param timezone: The time zone of the DB instance
    :param enable_iam_database_authentication: A value that indicates whether to enable
        mapping of IAM accounts to database accounts
    :param enable_performance_insights: A value that indicates whether to enable
        Performance Insights for the DB instance
    :param performance_insights_kms_key_id: The AWS KMS key identifier for
        encryption of Performance Insights data
    :param performance_insights_retention_period: The amount of time, in days, to retain
        Performance Insights data, valid values are 7 or 731 (2 years)
    :param enable_cloudwatch_logs_exports: The list of log types that need to be enabled
        for exporting to CloudWatch Logs
    :param processor_features: The number of CPU cores and the number of threads
        per core for the DB instance class of the DB instance
    :param deletion_protection: A value that indicates whether the DB instance has deletion protection enabled
    :param max_allocated_storage: The upper limit in GiB to
        which Amazon RDS can automatically scale the storage of the DB instance
    :param enable_customer_owned_ip: A value that indicates whether to
        enable a customer-owned IP address (CoIP) for an RDS on Outposts DB instance
    :param custom_iam_instance_profile: The instance profile associated with
        the underlying Amazon EC2 instance of an RDS Custom DB instance
    :param backup_target: Specifies where automated backups and manual snapshots are stored
    :param network_type: The network type of the DB instance
    """

    def __init__(
        self,
        *,
        db_instance_identifier: str,
        db_instance_class: str,
        engine: str,
        db_name: Optional[str] = None,
        allocated_storage: Optional[int] = None,
        master_username: Optional[str] = None,
        master_user_password: Optional[str] = None,
        db_security_groups: Optional[Sequence[str]] = None,
        vpc_security_group_ids: Optional[Sequence[str]] = None,
        availability_zone: Optional[str] = None,
        db_subnet_group_name: Optional[str] = None,
        preferred_maintenance_window: Optional[str] = None,
        db_parameter_group_name: Optional[str] = None,
        backup_retention_period: Optional[int] = None,
        preferred_backup_window: Optional[str] = None,
        port: Optional[int] = None,
        multi_az: Optional[bool] = None,
        engine_version: Optional[str] = None,
        auto_minor_version_upgrade: Optional[bool] = None,
        license_model: Optional[str] = None,
        iops: Optional[int] = None,
        option_group_name: Optional[str] = None,
        character_set_name: Optional[str] = None,
        nchar_character_set_name: Optional[str] = None,
        publicly_accessible: Optional[bool] = None,
        tags: Optional[Sequence[TagTypeDef]] = None,
        db_cluster_identifier: Optional[str] = None,
        storage_type: Optional[str] = None,
        tde_credential_arn: Optional[str] = None,
        tde_credential_password: Optional[str] = None,
        storage_encrypted: Optional[bool] = None,
        kms_key_id: Optional[str] = None,
        domain: Optional[str] = None,
        copy_tags_to_snapshot: Optional[bool] = None,
        monitoring_interval: Optional[int] = None,
        monitoring_role_arn: Optional[str] = None,
        domain_iam_role_name: Optional[str] = None,
        promotion_tier: Optional[int] = None,
        timezone: Optional[str] = None,
        enable_iam_database_authentication: Optional[bool] = None,
        enable_performance_insights: Optional[bool] = None,
        performance_insights_kms_key_id: Optional[str] = None,
        performance_insights_retention_period: Optional[int] = None,
        enable_cloudwatch_logs_exports: Optional[Sequence[str]] = None,
        processor_features: Optional[Sequence[ProcessorFeatureTypeDef]] = None,
        deletion_protection: Optional[bool] = None,
        max_allocated_storage: Optional[int] = None,
        enable_customer_owned_ip: Optional[bool] = None,
        custom_iam_instance_profile: Optional[str] = None,
        backup_target: Optional[str] = None,
        network_type: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.db_instance_identifier = db_instance_identifier
        self.db_instance_class = db_instance_class
        self.engine = engine
        self.db_name = db_name
        self.allocated_storage = allocated_storage
        self.master_username = master_username
        self.master_user_password = master_user_password
        self.db_security_groups = db_security_groups or []
        self.vpc_security_group_ids = vpc_security_group_ids or []
        self.availability_zone = availability_zone
        self.db_subnet_group_name = db_subnet_group_name
        self.preferred_maintenance_window = preferred_maintenance_window
        self.db_parameter_group_name = db_parameter_group_name
        self.backup_retention_period = backup_retention_period
        self.preferred_backup_window = preferred_backup_window
        self.port = port
        self.multi_az = multi_az
        self.engine_version = engine_version
        self.auto_minor_version_upgrade = auto_minor_version_upgrade
        self.license_model = license_model
        self.iops = iops
        self.option_group_name = option_group_name
        self.character_set_name = character_set_name
        self.nchar_character_set_name = nchar_character_set_name
        self.publicly_accessible = publicly_accessible
        self.tags = tags or []
        self.db_cluster_identifier = db_cluster_identifier
        self.storage_type = storage_type
        self.tde_credential_arn = tde_credential_arn
        self.tde_credential_password = tde_credential_password
        self.storage_encrypted = storage_encrypted
        self.kms_key_id = kms_key_id
        self.domain = domain
        self.copy_tags_to_snapshot = copy_tags_to_snapshot
        self.monitoring_interval = monitoring_interval
        self.monitoring_role_arn = monitoring_role_arn
        self.domain_iam_role_name = domain_iam_role_name
        self.promotion_tier = promotion_tier
        self.timezone = timezone
        self.enable_iam_database_authentication = enable_iam_database_authentication
        self.enable_performance_insights = enable_performance_insights
        self.performance_insights_kms_key_id = performance_insights_kms_key_id
        self.performance_insights_retention_period = performance_insights_retention_period
        self.enable_cloudwatch_logs_exports = enable_cloudwatch_logs_exports or []
        self.processor_features = processor_features or []
        self.deletion_protection = deletion_protection
        self.max_allocated_storage = max_allocated_storage
        self.enable_customer_owned_ip = enable_customer_owned_ip
        self.custom_iam_instance_profile = custom_iam_instance_profile
        self.backup_target = backup_target
        self.network_type = network_type

    def execute(self, context: 'Context') -> str:
        self.log.info(f"Creating new DB instance {self.db_instance_identifier}")
        params: Dict[str, Any] = {}
        if self.db_name:
            params["DBName"] = self.db_name
        if self.allocated_storage:
            params["AllocatedStorage"] = self.allocated_storage
        if self.master_username:
            params["MasterUsername"] = self.master_username
        if self.master_user_password:
            params["MasterUserPassword"] = self.master_user_password
        if self.db_security_groups:
            params["DBSecurityGroups"] = self.db_security_groups
        if self.vpc_security_group_ids:
            params["VpcSecurityGroupIds"] = self.vpc_security_group_ids
        if self.availability_zone:
            params["AvailabilityZone"] = self.availability_zone
        if self.db_subnet_group_name:
            params["DBSubnetGroupName"] = self.db_subnet_group_name
        if self.preferred_maintenance_window:
            params["PreferredMaintenanceWindow"] = self.preferred_maintenance_window
        if self.db_parameter_group_name:
            params["DBParameterGroupName"] = self.db_parameter_group_name
        if self.backup_retention_period:
            params["BackupRetentionPeriod"] = self.backup_retention_period
        if self.preferred_backup_window:
            params["PreferredBackupWindow"] = self.preferred_backup_window
        if self.port:
            params["Port"] = self.port
        if self.multi_az:
            params["MultiAZ"] = self.multi_az
        if self.engine_version:
            params["EngineVersion"] = self.engine_version
        if self.auto_minor_version_upgrade:
            params["AutoMinorVersionUpgrade"] = self.auto_minor_version_upgrade
        if self.license_model:
            params["LicenseModel"] = self.license_model
        if self.iops:
            params["Iops"] = self.iops
        if self.option_group_name:
            params["OptionGroupName"] = self.option_group_name
        if self.character_set_name:
            params["CharacterSetName"] = self.character_set_name
        if self.nchar_character_set_name:
            params["NcharCharacterSetName"] = self.nchar_character_set_name
        if self.publicly_accessible:
            params["PubliclyAccessible"] = self.publicly_accessible
        if self.tags:
            params["Tags"] = self.tags
        if self.db_cluster_identifier:
            params["DBClusterIdentifier"] = self.db_cluster_identifier
        if self.storage_type:
            params["StorageType"] = self.storage_type
        if self.tde_credential_arn:
            params["TdeCredentialArn"] = self.tde_credential_arn
        if self.tde_credential_password:
            params["TdeCredentialPassword"] = self.tde_credential_password
        if self.storage_encrypted:
            params["StorageEncrypted"] = self.storage_encrypted
        if self.kms_key_id:
            params["KmsKeyId"] = self.kms_key_id
        if self.domain:
            params["Domain"] = self.domain
        if self.copy_tags_to_snapshot:
            params["CopyTagsToSnapshot"] = self.copy_tags_to_snapshot
        if self.monitoring_interval:
            params["MonitoringInterval"] = self.monitoring_interval
        if self.monitoring_role_arn:
            params["MonitoringRoleArn"] = self.monitoring_role_arn
        if self.domain_iam_role_name:
            params["DomainIAMRoleName"] = self.domain_iam_role_name
        if self.promotion_tier:
            params["PromotionTier"] = self.promotion_tier
        if self.timezone:
            params["Timezone"] = self.timezone
        if self.enable_iam_database_authentication:
            params["EnableIAMDatabaseAuthentication"] = self.enable_iam_database_authentication
        if self.enable_performance_insights:
            params["EnablePerformanceInsights"] = self.enable_performance_insights
        if self.performance_insights_kms_key_id:
            params["PerformanceInsightsKMSKeyId"] = self.performance_insights_kms_key_id
        if self.performance_insights_retention_period:
            params["PerformanceInsightsRetentionPeriod"] = self.performance_insights_retention_period
        if self.enable_cloudwatch_logs_exports:
            params["EnableCloudwatchLogsExports"] = self.enable_cloudwatch_logs_exports
        if self.processor_features:
            params["ProcessorFeatures"] = self.processor_features
        if self.deletion_protection:
            params["DeletionProtection"] = self.deletion_protection
        if self.max_allocated_storage:
            params["MaxAllocatedStorage"] = self.max_allocated_storage
        if self.enable_customer_owned_ip:
            params["EnableCustomerOwnedIp"] = self.enable_customer_owned_ip
        if self.custom_iam_instance_profile:
            params["CustomIamInstanceProfile"] = self.custom_iam_instance_profile
        if self.backup_target:
            params["BackupTarget"] = self.backup_target
        if self.network_type:
            params["NetworkType"] = self.network_type

        create_db_instance = self.hook.conn.create_db_instance(
            DBInstanceIdentifier=self.db_instance_identifier,
            DBInstanceClass=self.db_instance_class,
            Engine=self.engine,
            **params,
        )
        self.hook.conn.get_waiter("db_instance_available").wait(
            DBInstanceIdentifier=self.db_instance_identifier
        )

        return json.dumps(create_db_instance, default=str)


__all__ = [
    "RdsCreateDbSnapshotOperator",
    "RdsCopyDbSnapshotOperator",
    "RdsDeleteDbSnapshotOperator",
    "RdsCreateEventSubscriptionOperator",
    "RdsDeleteEventSubscriptionOperator",
    "RdsStartExportTaskOperator",
    "RdsCancelExportTaskOperator",
    "RdsCreateDbInstanceOperator",
]
