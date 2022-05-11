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
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

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
        cluster_security_groups: Optional[List[str]] = None,
        vpc_security_group_ids: Optional[List[str]] = None,
        cluster_subnet_group_name: Optional[str] = None,
        availability_zone: Optional[str] = None,
        preferred_maintenance_window: Optional[str] = None,
        cluster_parameter_group_name: Optional[str] = None,
        automated_snapshot_retention_period: int = 1,
        manual_snapshot_retention_period: Optional[int] = None,
        port: int = 5439,
        cluster_version: str = "1.0",
        allow_version_upgrade: bool = True,
        publicly_accessible: bool = True,
        encrypted: bool = False,
        hsm_client_certificate_identifier: Optional[str] = None,
        hsm_configuration_identifier: Optional[str] = None,
        elastic_ip: Optional[str] = None,
        tags: Optional[List[Any]] = None,
        kms_key_id: Optional[str] = None,
        enhanced_vpc_routing: bool = False,
        additional_info: Optional[str] = None,
        iam_roles: Optional[List[str]] = None,
        maintenance_track_name: Optional[str] = None,
        snapshot_schedule_identifier: Optional[str] = None,
        availability_zone_relocation: Optional[bool] = None,
        aqua_configuration_status: Optional[str] = None,
        default_iam_role_arn: Optional[str] = None,
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

    def execute(self, context: 'Context'):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        self.log.info("Creating Redshift cluster %s", self.cluster_identifier)
        params: Dict[str, Any] = {}
        if self.db_name:
            params["DBName"] = self.db_name
        if self.cluster_type:
            params["ClusterType"] = self.cluster_type
        if self.number_of_nodes:
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

    def execute(self, context: 'Context'):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == 'paused':
            self.log.info("Starting Redshift cluster %s", self.cluster_identifier)
            redshift_hook.get_conn().resume_cluster(ClusterIdentifier=self.cluster_identifier)
        else:
            self.log.warning(
                "Unable to resume cluster since cluster is currently in status: %s", cluster_state
            )


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

    def execute(self, context: 'Context'):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == 'available':
            self.log.info("Pausing Redshift cluster %s", self.cluster_identifier)
            redshift_hook.get_conn().pause_cluster(ClusterIdentifier=self.cluster_identifier)
        else:
            self.log.warning(
                "Unable to pause cluster since cluster is currently in status: %s", cluster_state
            )


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
        final_cluster_snapshot_identifier: Optional[str] = None,
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
        self.redshift_hook = RedshiftHook(aws_conn_id=aws_conn_id)
        self.poll_interval = poll_interval

    def execute(self, context: 'Context'):
        self.delete_cluster()

        if self.wait_for_completion:
            cluster_status: str = self.check_status()
            while cluster_status != "cluster_not_found":
                self.log.info(
                    "cluster status is %s. Sleeping for %s seconds.", cluster_status, self.poll_interval
                )
                time.sleep(self.poll_interval)
                cluster_status = self.check_status()

    def delete_cluster(self) -> None:
        self.redshift_hook.delete_cluster(
            cluster_identifier=self.cluster_identifier,
            skip_final_cluster_snapshot=self.skip_final_cluster_snapshot,
            final_cluster_snapshot_identifier=self.final_cluster_snapshot_identifier,
        )

    def check_status(self) -> str:
        return self.redshift_hook.cluster_status(self.cluster_identifier)
