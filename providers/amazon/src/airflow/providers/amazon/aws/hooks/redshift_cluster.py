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

from collections.abc import Sequence
from typing import Any

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class RedshiftHook(AwsBaseHook):
    """
    Interact with Amazon Redshift.

    This is a thin wrapper around
    :external+boto3:py:class:`boto3.client("redshift") <Redshift.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    template_fields: Sequence[str] = ("cluster_identifier",)

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift"
        super().__init__(*args, **kwargs)

    def create_cluster(
        self,
        cluster_identifier: str,
        node_type: str,
        master_username: str,
        master_user_password: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a new cluster with the specified parameters.

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.create_cluster`

        :param cluster_identifier: A unique identifier for the cluster.
        :param node_type: The node type to be provisioned for the cluster. Refer
            https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-clusters.html#rs-node-type-info
            for the list of available node types.
        :param master_username: The username associated with the admin user account
            for the cluster that is being created.
        :param master_user_password: password associated with the admin user account
            for the cluster that is being created.
        :param params: Remaining AWS Create cluster API params.
        """
        response = self.conn.create_cluster(
            ClusterIdentifier=cluster_identifier,
            NodeType=node_type,
            MasterUsername=master_username,
            MasterUserPassword=master_user_password,
            **params,
        )
        return response

    # TODO: Wrap create_cluster_snapshot
    def cluster_status(self, cluster_identifier: str) -> str | None:
        """
        Get status of a cluster.

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.describe_clusters`

        :param cluster_identifier: unique identifier of a cluster
        """
        try:
            response = self.conn.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"]
            return response[0]["ClusterStatus"] if response else None
        except self.conn.exceptions.ClusterNotFoundFault:
            return "cluster_not_found"

    async def cluster_status_async(self, cluster_identifier: str) -> str | None:
        async with await self.get_async_conn() as client:
            response = await client.describe_clusters(ClusterIdentifier=cluster_identifier)
            return response["Clusters"][0]["ClusterStatus"] if response else None

    def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: str | None = None,
    ):
        """
        Delete a cluster and optionally create a snapshot.

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.delete_cluster`

        :param cluster_identifier: unique identifier of a cluster
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        """
        final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ""

        response = self.conn.delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
            FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
        )
        return response["Cluster"] if response["Cluster"] else None

    def describe_cluster_snapshots(self, cluster_identifier: str) -> list[str] | None:
        """
        List snapshots for a cluster.

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.describe_cluster_snapshots`

        :param cluster_identifier: unique identifier of a cluster
        """
        response = self.conn.describe_cluster_snapshots(ClusterIdentifier=cluster_identifier)
        if "Snapshots" not in response:
            return None
        snapshots = response["Snapshots"]
        snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
        snapshots.sort(key=lambda x: x["SnapshotCreateTime"], reverse=True)
        return snapshots

    def restore_from_cluster_snapshot(self, cluster_identifier: str, snapshot_identifier: str) -> dict | None:
        """
        Restore a cluster from its snapshot.

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.restore_from_cluster_snapshot`

        :param cluster_identifier: unique identifier of a cluster
        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        """
        response = self.conn.restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier, SnapshotIdentifier=snapshot_identifier
        )
        return response["Cluster"] if response["Cluster"] else None

    def create_cluster_snapshot(
        self,
        snapshot_identifier: str,
        cluster_identifier: str,
        retention_period: int = -1,
        tags: list[Any] | None = None,
    ) -> dict | None:
        """
        Create a snapshot of a cluster.

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.create_cluster_snapshot`

        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :param cluster_identifier: unique identifier of a cluster
        :param retention_period: The number of days that a manual snapshot is retained.
            If the value is -1, the manual snapshot is retained indefinitely.
        :param tags: A list of tag instances
        """
        if tags is None:
            tags = []
        response = self.conn.create_cluster_snapshot(
            SnapshotIdentifier=snapshot_identifier,
            ClusterIdentifier=cluster_identifier,
            ManualSnapshotRetentionPeriod=retention_period,
            Tags=tags,
        )
        return response["Snapshot"] if response["Snapshot"] else None

    def get_cluster_snapshot_status(self, snapshot_identifier: str):
        """
        Get Redshift cluster snapshot status.

        If cluster snapshot not found, *None* is returned.

        :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
        """
        try:
            response = self.conn.describe_cluster_snapshots(
                SnapshotIdentifier=snapshot_identifier,
            )
            snapshot = response.get("Snapshots")[0]
            snapshot_status: str = snapshot.get("Status")
            return snapshot_status
        except self.conn.exceptions.ClusterSnapshotNotFoundFault:
            return None
