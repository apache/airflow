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

import time
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftCreateClusterOperator(BaseOperator):
    """Creates a new cluster with the specified parameters.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftCreateClusterOperator`

    :param cluster_identifier:  A unique identifier for the cluster.
    :param node_type: The node type to be provisioned for the cluster.
            Valid Values: ``ds2.xlarge``, ``ds2.8xlarge``, ``dc1.large``,
            ``dc1.8xlarge``, ``dc2.large``, ``dc2.8xlarge``, ``ra3.xlplus``,
            ``ra3.4xlarge``, and ``ra3.16xlarge``.
    :param master_username: The username associated with the admin user account for
        the cluster that is being created.
    :param master_user_password: The password associated with the admin user account for
        the cluster that is being created.
    :param cluster_type: The type of the cluster ``single-node`` or ``multi-node``.
        The default value is ``multi-node``.
    :param db_name: The name of the first database to be created when the cluster is created.
    :param number_of_nodes: The number of compute nodes in the cluster.
        This param require when ``cluster_type`` is ``multi-node``.
    :param cluster_security_groups: A list of security groups to be associated with this cluster.
    :param vpc_security_group_ids: A list of  VPC security groups to be associated with the cluster.
    :param cluster_subnet_group_name: The name of a cluster subnet group to be associated with this cluster.
    :param availability_zone: The EC2 Availability Zone (AZ).
    :param preferred_maintenance_window: The time range (in UTC) during which automated cluster
        maintenance can occur.
    :param cluster_parameter_group_name: The name of the parameter group to be associated with this cluster.
    :param automated_snapshot_retention_period: The number of days that automated snapshots are retained.
        The default value is ``1``.
    :param manual_snapshot_retention_period: The default number of days to retain a manual snapshot.
    :param port: The port number on which the cluster accepts incoming connections.
        The Default value is ``5439``.
    :param cluster_version: The version of a Redshift engine software that you want to deploy on the cluster.
    :param allow_version_upgrade: Whether major version upgrades can be applied during the maintenance window.
        The Default value is ``True``.
    :parma publicly_accessible: Whether cluster can be accessed from a public network.
    :parma encrypted: Whether data in the cluster is encrypted at rest.
        The default value is ``False``.
    :parma hsm_client_certificate_identifier: Name of the HSM client certificate
        the Amazon Redshift cluster uses to retrieve the data.
    :parma hsm_configuration_identifier: Name of the HSM configuration
    :parma elastic_ip: The Elastic IP (EIP) address for the cluster.
    :parma tags: A list of tag instances
    :parma kms_key_id: KMS key id of encryption key.
    :param enhanced_vpc_routing: Whether to create the cluster with enhanced VPC routing enabled
        Default value is ``False``.
    :param additional_info: Reserved
    :param iam_roles: A list of IAM roles that can be used by the cluster to access other AWS services.
    :param maintenance_track_name: Name of the maintenance track for the cluster.
    :param snapshot_schedule_identifier: A  unique identifier for the snapshot schedule.
    :param availability_zone_relocation: Enable relocation for a Redshift cluster
        between Availability Zones after the cluster is created.
    :param aqua_configuration_status: The cluster is configured to use AQUA .
    :param default_iam_role_arn: ARN for the IAM role.
    :param aws_conn_id: str = The Airflow connection used for AWS credentials.
        The default connection id is ``aws_default``.
    """

    template_fields: Sequence[str] = (
        "cluster_identifier",
        "cluster_type",
        "node_type",
        "number_of_nodes",
        "vpc_security_group_ids",
    )
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        node_type: str,
        master_username: str,
        master_user_password: str,
        cluster_type: str = "multi-node",
        db_name: str = "dev",
        number_of_nodes: int = 1,
        cluster_security_groups: list[str] | None = None,
        vpc_security_group_ids: list[str] | None = None,
        cluster_subnet_group_name: str | None = None,
        availability_zone: str | None = None,
        preferred_maintenance_window: str | None = None,
        cluster_parameter_group_name: str | None = None,
        automated_snapshot_retention_period: int = 1,
        manual_snapshot_retention_period: int | None = None,
        port: int = 5439,
        cluster_version: str = "1.0",
        allow_version_upgrade: bool = True,
        publicly_accessible: bool = True,
        encrypted: bool = False,
        hsm_client_certificate_identifier: str | None = None,
        hsm_configuration_identifier: str | None = None,
        elastic_ip: str | None = None,
        tags: list[Any] | None = None,
        kms_key_id: str | None = None,
        enhanced_vpc_routing: bool = False,
        additional_info: str | None = None,
        iam_roles: list[str] | None = None,
        maintenance_track_name: str | None = None,
        snapshot_schedule_identifier: str | None = None,
        availability_zone_relocation: bool | None = None,
        aqua_configuration_status: str | None = None,
        default_iam_role_arn: str | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.node_type = node_type
        self.master_username = master_username
        self.master_user_password = master_user_password
        self.cluster_type = cluster_type
        self.db_name = db_name
        self.number_of_nodes = number_of_nodes
        self.cluster_security_groups = cluster_security_groups
        self.vpc_security_group_ids = vpc_security_group_ids
        self.cluster_subnet_group_name = cluster_subnet_group_name
        self.availability_zone = availability_zone
        self.preferred_maintenance_window = preferred_maintenance_window
        self.cluster_parameter_group_name = cluster_parameter_group_name
        self.automated_snapshot_retention_period = automated_snapshot_retention_period
        self.manual_snapshot_retention_period = manual_snapshot_retention_period
        self.port = port
        self.cluster_version = cluster_version
        self.allow_version_upgrade = allow_version_upgrade
        self.publicly_accessible = publicly_accessible
        self.encrypted = encrypted
        self.hsm_client_certificate_identifier = hsm_client_certificate_identifier
        self.hsm_configuration_identifier = hsm_configuration_identifier
        self.elastic_ip = elastic_ip
        self.tags = tags
        self.kms_key_id = kms_key_id
        self.enhanced_vpc_routing = enhanced_vpc_routing
        self.additional_info = additional_info
        self.iam_roles = iam_roles
        self.maintenance_track_name = maintenance_track_name
        self.snapshot_schedule_identifier = snapshot_schedule_identifier
        self.availability_zone_relocation = availability_zone_relocation
        self.aqua_configuration_status = aqua_configuration_status
        self.default_iam_role_arn = default_iam_role_arn
        self.aws_conn_id = aws_conn_id
        self.kwargs = kwargs

    def execute(self, context: Context):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        self.log.info("Creating Redshift cluster %s", self.cluster_identifier)
        params: dict[str, Any] = {}
        if self.db_name:
            params["DBName"] = self.db_name
        if self.cluster_type:
            params["ClusterType"] = self.cluster_type
            if self.cluster_type == "multi-node":
                params["NumberOfNodes"] = self.number_of_nodes
        if self.cluster_security_groups:
            params["ClusterSecurityGroups"] = self.cluster_security_groups
        if self.vpc_security_group_ids:
            params["VpcSecurityGroupIds"] = self.vpc_security_group_ids
        if self.cluster_subnet_group_name:
            params["ClusterSubnetGroupName"] = self.cluster_subnet_group_name
        if self.availability_zone:
            params["AvailabilityZone"] = self.availability_zone
        if self.preferred_maintenance_window:
            params["PreferredMaintenanceWindow"] = self.preferred_maintenance_window
        if self.cluster_parameter_group_name:
            params["ClusterParameterGroupName"] = self.cluster_parameter_group_name
        if self.automated_snapshot_retention_period:
            params["AutomatedSnapshotRetentionPeriod"] = self.automated_snapshot_retention_period
        if self.manual_snapshot_retention_period:
            params["ManualSnapshotRetentionPeriod"] = self.manual_snapshot_retention_period
        if self.port:
            params["Port"] = self.port
        if self.cluster_version:
            params["ClusterVersion"] = self.cluster_version
        if self.allow_version_upgrade:
            params["AllowVersionUpgrade"] = self.allow_version_upgrade
        if self.publicly_accessible:
            params["PubliclyAccessible"] = self.publicly_accessible
        if self.encrypted:
            params["Encrypted"] = self.encrypted
        if self.hsm_client_certificate_identifier:
            params["HsmClientCertificateIdentifier"] = self.hsm_client_certificate_identifier
        if self.hsm_configuration_identifier:
            params["HsmConfigurationIdentifier"] = self.hsm_configuration_identifier
        if self.elastic_ip:
            params["ElasticIp"] = self.elastic_ip
        if self.tags:
            params["Tags"] = self.tags
        if self.kms_key_id:
            params["KmsKeyId"] = self.kms_key_id
        if self.enhanced_vpc_routing:
            params["EnhancedVpcRouting"] = self.enhanced_vpc_routing
        if self.additional_info:
            params["AdditionalInfo"] = self.additional_info
        if self.iam_roles:
            params["IamRoles"] = self.iam_roles
        if self.maintenance_track_name:
            params["MaintenanceTrackName"] = self.maintenance_track_name
        if self.snapshot_schedule_identifier:
            params["SnapshotScheduleIdentifier"] = self.snapshot_schedule_identifier
        if self.availability_zone_relocation:
            params["AvailabilityZoneRelocation"] = self.availability_zone_relocation
        if self.aqua_configuration_status:
            params["AquaConfigurationStatus"] = self.aqua_configuration_status
        if self.default_iam_role_arn:
            params["DefaultIamRoleArn"] = self.default_iam_role_arn

        cluster = redshift_hook.create_cluster(
            self.cluster_identifier,
            self.node_type,
            self.master_username,
            self.master_user_password,
            params,
        )
        self.log.info("Created Redshift cluster %s", self.cluster_identifier)
        self.log.info(cluster)


class RedshiftCreateClusterSnapshotOperator(BaseOperator):
    """
    Creates a manual snapshot of the specified cluster. The cluster must be in the available state

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftCreateClusterSnapshotOperator`

    :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
    :param cluster_identifier: The cluster identifier for which you want a snapshot
    :param retention_period: The number of days that a manual snapshot is retained.
        If the value is -1, the manual snapshot is retained indefinitely.
    :param wait_for_completion: Whether wait for the cluster snapshot to be in ``available`` state
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check state
    :param max_attempt: The maximum number of attempts to be made to check the state
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        The default connection id is ``aws_default``
    """

    template_fields: Sequence[str] = (
        "cluster_identifier",
        "snapshot_identifier",
    )

    def __init__(
        self,
        *,
        snapshot_identifier: str,
        cluster_identifier: str,
        retention_period: int = -1,
        wait_for_completion: bool = False,
        poll_interval: int = 15,
        max_attempt: int = 20,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.snapshot_identifier = snapshot_identifier
        self.cluster_identifier = cluster_identifier
        self.retention_period = retention_period
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.redshift_hook = RedshiftHook(aws_conn_id=aws_conn_id)

    def execute(self, context: Context) -> Any:
        cluster_state = self.redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state != "available":
            raise AirflowException(
                "Redshift cluster must be in available state. "
                f"Redshift cluster current state is {cluster_state}"
            )

        self.redshift_hook.create_cluster_snapshot(
            cluster_identifier=self.cluster_identifier,
            snapshot_identifier=self.snapshot_identifier,
            retention_period=self.retention_period,
        )

        if self.wait_for_completion:
            self.redshift_hook.get_conn().get_waiter("snapshot_available").wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={
                    "Delay": self.poll_interval,
                    "MaxAttempts": self.max_attempt,
                },
            )


class RedshiftDeleteClusterSnapshotOperator(BaseOperator):
    """
    Deletes the specified manual snapshot

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftDeleteClusterSnapshotOperator`

    :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
    :param cluster_identifier: The unique identifier of the cluster the snapshot was created from
    :param wait_for_completion: Whether wait for cluster deletion or not
        The default value is ``True``
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        The default connection id is ``aws_default``
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check snapshot state
    """

    template_fields: Sequence[str] = (
        "cluster_identifier",
        "snapshot_identifier",
    )

    def __init__(
        self,
        *,
        snapshot_identifier: str,
        cluster_identifier: str,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        poll_interval: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.snapshot_identifier = snapshot_identifier
        self.cluster_identifier = cluster_identifier
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.redshift_hook = RedshiftHook(aws_conn_id=aws_conn_id)

    def execute(self, context: Context) -> Any:
        self.redshift_hook.get_conn().delete_cluster_snapshot(
            SnapshotClusterIdentifier=self.cluster_identifier,
            SnapshotIdentifier=self.snapshot_identifier,
        )

        if self.wait_for_completion:
            while self.get_status() is not None:
                time.sleep(self.poll_interval)

    def get_status(self) -> str:
        return self.redshift_hook.get_cluster_snapshot_status(
            snapshot_identifier=self.snapshot_identifier,
        )


class RedshiftResumeClusterOperator(BaseOperator):
    """
    Resume a paused AWS Redshift Cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftResumeClusterOperator`

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.aws_conn_id = aws_conn_id
        # These parameters are added to address an issue with the boto3 API where the API
        # prematurely reports the cluster as available to receive requests. This causes the cluster
        # to reject initial attempts to resume the cluster despite reporting the correct state.
        self._attempts = 10
        self._attempt_interval = 15

    def execute(self, context: Context):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)

        while self._attempts >= 1:
            try:
                redshift_hook.get_conn().resume_cluster(ClusterIdentifier=self.cluster_identifier)
                return
            except redshift_hook.get_conn().exceptions.InvalidClusterStateFault as error:
                self._attempts = self._attempts - 1

                if self._attempts > 0:
                    self.log.error("Unable to resume cluster. %d attempts remaining.", self._attempts)
                    time.sleep(self._attempt_interval)
                else:
                    raise error


class RedshiftPauseClusterOperator(BaseOperator):
    """
    Pause an AWS Redshift Cluster if it has status `available`.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftPauseClusterOperator`

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.aws_conn_id = aws_conn_id
        # These parameters are added to address an issue with the boto3 API where the API
        # prematurely reports the cluster as available to receive requests. This causes the cluster
        # to reject initial attempts to pause the cluster despite reporting the correct state.
        self._attempts = 10
        self._attempt_interval = 15

    def execute(self, context: Context):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)

        while self._attempts >= 1:
            try:
                redshift_hook.get_conn().pause_cluster(ClusterIdentifier=self.cluster_identifier)
                return
            except redshift_hook.get_conn().exceptions.InvalidClusterStateFault as error:
                self._attempts = self._attempts - 1

                if self._attempts > 0:
                    self.log.error("Unable to pause cluster. %d attempts remaining.", self._attempts)
                    time.sleep(self._attempt_interval)
                else:
                    raise error


class RedshiftDeleteClusterOperator(BaseOperator):
    """
    Delete an AWS Redshift cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftDeleteClusterOperator`

    :param cluster_identifier: unique identifier of a cluster
    :param skip_final_cluster_snapshot: determines cluster snapshot creation
    :param final_cluster_snapshot_identifier: name of final cluster snapshot
    :param wait_for_completion: Whether wait for cluster deletion or not
        The default value is ``True``
    :param aws_conn_id: aws connection to use
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check cluster state
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: str | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = "aws_default",
        poll_interval: float = 30.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.skip_final_cluster_snapshot = skip_final_cluster_snapshot
        self.final_cluster_snapshot_identifier = final_cluster_snapshot_identifier
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        # These parameters are added to keep trying if there is a running operation in the cluster
        # If there is a running operation in the cluster while trying to delete it, a InvalidClusterStateFault
        # is thrown. In such case, retrying
        self._attempts = 10
        self._attempt_interval = 15
        self.redshift_hook = RedshiftHook(aws_conn_id=aws_conn_id)

    def execute(self, context: Context):
        while self._attempts >= 1:
            try:
                self.redshift_hook.delete_cluster(
                    cluster_identifier=self.cluster_identifier,
                    skip_final_cluster_snapshot=self.skip_final_cluster_snapshot,
                    final_cluster_snapshot_identifier=self.final_cluster_snapshot_identifier,
                )
                break
            except self.redshift_hook.get_conn().exceptions.InvalidClusterStateFault:
                self._attempts = self._attempts - 1

                if self._attempts > 0:
                    self.log.error("Unable to delete cluster. %d attempts remaining.", self._attempts)
                    time.sleep(self._attempt_interval)
                else:
                    raise

        if self.wait_for_completion:
            waiter = self.redshift_hook.get_conn().get_waiter("cluster_deleted")
            waiter.wait(
                ClusterIdentifier=self.cluster_identifier,
                WaiterConfig={"Delay": self.poll_interval, "MaxAttempts": 30},
            )
