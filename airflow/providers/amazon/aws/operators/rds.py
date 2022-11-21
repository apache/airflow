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
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Sequence

from mypy_boto3_rds.type_defs import TagTypeDef

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.utils.rds import RdsDbType

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RdsBaseOperator(BaseOperator):
    """Base operator that implements common functions for all operators"""

    ui_color = "#eeaa88"
    ui_fgcolor = "#ffffff"

    def __init__(self, *args, aws_conn_id: str = "aws_conn_id", hook_params: dict | None = None, **kwargs):
        hook_params = hook_params or {}
        self.hook = RdsHook(aws_conn_id=aws_conn_id, **hook_params)
        super().__init__(*args, **kwargs)

        self._await_interval = 60  # seconds

    def execute(self, context: Context) -> str:
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
    :param wait_for_completion:  If True, waits for creation of the DB snapshot to complete. (default: True)
    """

    template_fields = ("db_snapshot_identifier", "db_identifier", "tags")

    def __init__(
        self,
        *,
        db_type: str,
        db_identifier: str,
        db_snapshot_identifier: str,
        tags: Sequence[TagTypeDef] | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_conn_id",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_type = RdsDbType(db_type)
        self.db_identifier = db_identifier
        self.db_snapshot_identifier = db_snapshot_identifier
        self.tags = tags or []
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
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
            if self.wait_for_completion:
                self.hook.wait_for_db_snapshot_state(self.db_snapshot_identifier, target_state="available")
        else:
            create_cluster_snap = self.hook.conn.create_db_cluster_snapshot(
                DBClusterIdentifier=self.db_identifier,
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
                Tags=self.tags,
            )
            create_response = json.dumps(create_cluster_snap, default=str)
            if self.wait_for_completion:
                self.hook.wait_for_db_cluster_snapshot_state(
                    self.db_snapshot_identifier, target_state="available"
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
    :param wait_for_completion:  If True, waits for snapshot copy to complete. (default: True)
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
        tags: Sequence[TagTypeDef] | None = None,
        copy_tags: bool = False,
        pre_signed_url: str = "",
        option_group_name: str = "",
        target_custom_availability_zone: str = "",
        source_region: str = "",
        wait_for_completion: bool = True,
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
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
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
            if self.wait_for_completion:
                self.hook.wait_for_db_snapshot_state(
                    self.target_db_snapshot_identifier, target_state="available"
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
            if self.wait_for_completion:
                self.hook.wait_for_db_cluster_snapshot_state(
                    self.target_db_snapshot_identifier, target_state="available"
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
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.db_type = RdsDbType(db_type)
        self.db_snapshot_identifier = db_snapshot_identifier
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.log.info("Starting to delete snapshot '%s'", self.db_snapshot_identifier)

        if self.db_type.value == "instance":
            delete_instance_snap = self.hook.conn.delete_db_snapshot(
                DBSnapshotIdentifier=self.db_snapshot_identifier,
            )
            delete_response = json.dumps(delete_instance_snap, default=str)
            if self.wait_for_completion:
                self.hook.wait_for_db_snapshot_state(self.db_snapshot_identifier, target_state="deleted")
        else:
            delete_cluster_snap = self.hook.conn.delete_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=self.db_snapshot_identifier,
            )
            delete_response = json.dumps(delete_cluster_snap, default=str)
            if self.wait_for_completion:
                self.hook.wait_for_db_cluster_snapshot_state(
                    self.db_snapshot_identifier, target_state="deleted"
                )

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
    :param wait_for_completion:  If True, waits for the DB snapshot export to complete. (default: True)
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
        s3_prefix: str = "",
        export_only: list[str] | None = None,
        wait_for_completion: bool = True,
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
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
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

        if self.wait_for_completion:
            self.hook.wait_for_export_task_state(self.export_task_identifier, target_state="complete")
        return json.dumps(start_export, default=str)


class RdsCancelExportTaskOperator(RdsBaseOperator):
    """
    Cancels an export task in progress that is exporting a snapshot to Amazon S3

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsCancelExportTaskOperator`

    :param export_task_identifier: The identifier of the snapshot export task to cancel
    :param wait_for_completion:  If True, waits for DB snapshot export to cancel. (default: True)
    """

    template_fields = ("export_task_identifier",)

    def __init__(
        self,
        *,
        export_task_identifier: str,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.export_task_identifier = export_task_identifier
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.log.info("Canceling export task %s", self.export_task_identifier)

        cancel_export = self.hook.conn.cancel_export_task(
            ExportTaskIdentifier=self.export_task_identifier,
        )

        if self.wait_for_completion:
            self.hook.wait_for_export_task_state(self.export_task_identifier, target_state="canceled")
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
    :param wait_for_completion:  If True, waits for creation of the subscription to complete. (default: True)
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
        event_categories: Sequence[str] | None = None,
        source_ids: Sequence[str] | None = None,
        enabled: bool = True,
        tags: Sequence[TagTypeDef] | None = None,
        wait_for_completion: bool = True,
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
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
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

        if self.wait_for_completion:
            self.hook.wait_for_event_subscription_state(self.subscription_name, target_state="active")
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

    def execute(self, context: Context) -> str:
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
    :param rds_kwargs: Named arguments to pass to boto3 RDS client function ``create_db_instance``
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_instance
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param wait_for_completion:  If True, waits for creation of the DB instance to complete. (default: True)
    """

    template_fields = ("db_instance_identifier", "db_instance_class", "engine", "rds_kwargs")

    def __init__(
        self,
        *,
        db_instance_identifier: str,
        db_instance_class: str,
        engine: str,
        rds_kwargs: dict | None = None,
        aws_conn_id: str = "aws_default",
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

        self.db_instance_identifier = db_instance_identifier
        self.db_instance_class = db_instance_class
        self.engine = engine
        self.rds_kwargs = rds_kwargs or {}
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.log.info("Creating new DB instance %s", self.db_instance_identifier)

        create_db_instance = self.hook.conn.create_db_instance(
            DBInstanceIdentifier=self.db_instance_identifier,
            DBInstanceClass=self.db_instance_class,
            Engine=self.engine,
            **self.rds_kwargs,
        )

        if self.wait_for_completion:
            self.hook.wait_for_db_instance_state(self.db_instance_identifier, target_state="available")
        return json.dumps(create_db_instance, default=str)


class RdsDeleteDbInstanceOperator(RdsBaseOperator):
    """
    Deletes an RDS DB Instance

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsDeleteDbInstanceOperator`

    :param db_instance_identifier: The DB instance identifier for the DB instance to be deleted
    :param rds_kwargs: Named arguments to pass to boto3 RDS client function ``delete_db_instance``
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.delete_db_instance
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param wait_for_completion:  If True, waits for deletion of the DB instance to complete. (default: True)
    """

    template_fields = ("db_instance_identifier", "rds_kwargs")

    def __init__(
        self,
        *,
        db_instance_identifier: str,
        rds_kwargs: dict | None = None,
        aws_conn_id: str = "aws_default",
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_instance_identifier = db_instance_identifier
        self.rds_kwargs = rds_kwargs or {}
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.log.info("Deleting DB instance %s", self.db_instance_identifier)

        delete_db_instance = self.hook.conn.delete_db_instance(
            DBInstanceIdentifier=self.db_instance_identifier,
            **self.rds_kwargs,
        )

        if self.wait_for_completion:
            self.hook.wait_for_db_instance_state(self.db_instance_identifier, target_state="deleted")
        return json.dumps(delete_db_instance, default=str)


class RdsStartDbOperator(RdsBaseOperator):
    """
    Starts an RDS DB instance / cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsStartDbOperator`

    :param db_identifier: The AWS identifier of the DB to start
    :param db_type: Type of the DB - either "instance" or "cluster" (default: "instance")
    :param aws_conn_id: The Airflow connection used for AWS credentials. (default: "aws_default")
    :param wait_for_completion:  If True, waits for DB to start. (default: True)
    """

    template_fields = ("db_identifier", "db_type")

    def __init__(
        self,
        *,
        db_identifier: str,
        db_type: RdsDbType | str = RdsDbType.INSTANCE,
        aws_conn_id: str = "aws_default",
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_identifier = db_identifier
        self.db_type = db_type
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.db_type = RdsDbType(self.db_type)
        start_db_response = self._start_db()
        if self.wait_for_completion:
            self._wait_until_db_available()
        return json.dumps(start_db_response, default=str)

    def _start_db(self):
        self.log.info("Starting DB %s '%s'", self.db_type.value, self.db_identifier)
        if self.db_type == RdsDbType.INSTANCE:
            response = self.hook.conn.start_db_instance(DBInstanceIdentifier=self.db_identifier)
        else:
            response = self.hook.conn.start_db_cluster(DBClusterIdentifier=self.db_identifier)
        return response

    def _wait_until_db_available(self):
        self.log.info("Waiting for DB %s to reach 'available' state", self.db_type.value)
        if self.db_type == RdsDbType.INSTANCE:
            self.hook.wait_for_db_instance_state(self.db_identifier, target_state="available")
        else:
            self.hook.wait_for_db_cluster_state(self.db_identifier, target_state="available")


class RdsStopDbOperator(RdsBaseOperator):
    """
    Stops an RDS DB instance / cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RdsStopDbOperator`

    :param db_identifier: The AWS identifier of the DB to stop
    :param db_type: Type of the DB - either "instance" or "cluster" (default: "instance")
    :param db_snapshot_identifier: The instance identifier of the DB Snapshot to create before
        stopping the DB instance. The default value (None) skips snapshot creation. This
        parameter is ignored when ``db_type`` is "cluster"
    :param aws_conn_id: The Airflow connection used for AWS credentials. (default: "aws_default")
    :param wait_for_completion:  If True, waits for DB to stop. (default: True)
    """

    template_fields = ("db_identifier", "db_snapshot_identifier", "db_type")

    def __init__(
        self,
        *,
        db_identifier: str,
        db_type: RdsDbType | str = RdsDbType.INSTANCE,
        db_snapshot_identifier: str | None = None,
        aws_conn_id: str = "aws_default",
        wait_for_completion: bool = True,
        **kwargs,
    ):
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)
        self.db_identifier = db_identifier
        self.db_type = db_type
        self.db_snapshot_identifier = db_snapshot_identifier
        self.wait_for_completion = wait_for_completion

    def execute(self, context: Context) -> str:
        self.db_type = RdsDbType(self.db_type)
        stop_db_response = self._stop_db()
        if self.wait_for_completion:
            self._wait_until_db_stopped()
        return json.dumps(stop_db_response, default=str)

    def _stop_db(self):
        self.log.info("Stopping DB %s '%s'", self.db_type.value, self.db_identifier)
        if self.db_type == RdsDbType.INSTANCE:
            conn_params = {"DBInstanceIdentifier": self.db_identifier}
            # The db snapshot parameter is optional, but the AWS SDK raises an exception
            # if passed a null value. Only set snapshot id if value is present.
            if self.db_snapshot_identifier:
                conn_params["DBSnapshotIdentifier"] = self.db_snapshot_identifier
            response = self.hook.conn.stop_db_instance(**conn_params)
        else:
            if self.db_snapshot_identifier:
                self.log.warning(
                    "'db_snapshot_identifier' does not apply to db clusters. "
                    "Remove it to silence this warning."
                )
            response = self.hook.conn.stop_db_cluster(DBClusterIdentifier=self.db_identifier)
        return response

    def _wait_until_db_stopped(self):
        self.log.info("Waiting for DB %s to reach 'stopped' state", self.db_type.value)
        if self.db_type == RdsDbType.INSTANCE:
            self.hook.wait_for_db_instance_state(self.db_identifier, target_state="stopped")
        else:
            self.hook.wait_for_db_cluster_state(self.db_identifier, target_state="stopped")


__all__ = [
    "RdsCreateDbSnapshotOperator",
    "RdsCopyDbSnapshotOperator",
    "RdsDeleteDbSnapshotOperator",
    "RdsCreateEventSubscriptionOperator",
    "RdsDeleteEventSubscriptionOperator",
    "RdsStartExportTaskOperator",
    "RdsCancelExportTaskOperator",
    "RdsCreateDbInstanceOperator",
    "RdsDeleteDbInstanceOperator",
    "RdsStartDbOperator",
    "RdsStopDbOperator",
]
