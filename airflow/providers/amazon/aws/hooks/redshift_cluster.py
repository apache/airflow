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

import asyncio
from typing import Any, Sequence

import botocore.exceptions
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseAsyncHook, AwsBaseHook


class RedshiftHook(AwsBaseHook):
    """
    Interact with Amazon Redshift.
    Provide thin wrapper around :external+boto3:py:class:`boto3.client("redshift") <Redshift.Client>`.

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
        Creates a new cluster with the specified parameters

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.create_cluster`

        :param cluster_identifier: A unique identifier for the cluster.
        :param node_type: The node type to be provisioned for the cluster.
            Valid Values: ``ds2.xlarge``, ``ds2.8xlarge``, ``dc1.large``,
            ``dc1.8xlarge``, ``dc2.large``, ``dc2.8xlarge``, ``ra3.xlplus``,
            ``ra3.4xlarge``, and ``ra3.16xlarge``.
        :param master_username: The username associated with the admin user account
            for the cluster that is being created.
        :param master_user_password: password associated with the admin user account
            for the cluster that is being created.
        :param params: Remaining AWS Create cluster API params.
        """
        try:
            response = self.get_conn().create_cluster(
                ClusterIdentifier=cluster_identifier,
                NodeType=node_type,
                MasterUsername=master_username,
                MasterUserPassword=master_user_password,
                **params,
            )
            return response
        except ClientError as e:
            raise e

    # TODO: Wrap create_cluster_snapshot
    def cluster_status(self, cluster_identifier: str) -> str:
        """
        Return status of a cluster

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.describe_clusters`

        :param cluster_identifier: unique identifier of a cluster
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :param final_cluster_snapshot_identifier: Optional[str]
        """
        try:
            response = self.get_conn().describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"]
            return response[0]["ClusterStatus"] if response else None
        except self.get_conn().exceptions.ClusterNotFoundFault:
            return "cluster_not_found"

    def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: str | None = None,
    ):
        """
        Delete a cluster and optionally create a snapshot

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.delete_cluster`

        :param cluster_identifier: unique identifier of a cluster
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        """
        final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ""

        response = self.get_conn().delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
            FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
        )
        return response["Cluster"] if response["Cluster"] else None

    def describe_cluster_snapshots(self, cluster_identifier: str) -> list[str] | None:
        """
        Gets a list of snapshots for a cluster

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.describe_cluster_snapshots`

        :param cluster_identifier: unique identifier of a cluster
        """
        response = self.get_conn().describe_cluster_snapshots(ClusterIdentifier=cluster_identifier)
        if "Snapshots" not in response:
            return None
        snapshots = response["Snapshots"]
        snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
        snapshots.sort(key=lambda x: x["SnapshotCreateTime"], reverse=True)
        return snapshots

    def restore_from_cluster_snapshot(self, cluster_identifier: str, snapshot_identifier: str) -> str:
        """
        Restores a cluster from its snapshot

        .. seealso::
            - :external+boto3:py:meth:`Redshift.Client.restore_from_cluster_snapshot`

        :param cluster_identifier: unique identifier of a cluster
        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        """
        response = self.get_conn().restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier, SnapshotIdentifier=snapshot_identifier
        )
        return response["Cluster"] if response["Cluster"] else None

    def create_cluster_snapshot(
        self,
        snapshot_identifier: str,
        cluster_identifier: str,
        retention_period: int = -1,
        tags: list[Any] | None = None,
    ) -> str:
        """
        Creates a snapshot of a cluster

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
        response = self.get_conn().create_cluster_snapshot(
            SnapshotIdentifier=snapshot_identifier,
            ClusterIdentifier=cluster_identifier,
            ManualSnapshotRetentionPeriod=retention_period,
            Tags=tags,
        )
        return response["Snapshot"] if response["Snapshot"] else None

    def get_cluster_snapshot_status(self, snapshot_identifier: str):
        """
        Return Redshift cluster snapshot status. If cluster snapshot not found return ``None``

        :param snapshot_identifier: A unique identifier for the snapshot that you are requesting
        """
        try:
            response = self.get_conn().describe_cluster_snapshots(
                SnapshotIdentifier=snapshot_identifier,
            )
            snapshot = response.get("Snapshots")[0]
            snapshot_status: str = snapshot.get("Status")
            return snapshot_status
        except self.get_conn().exceptions.ClusterSnapshotNotFoundFault:
            return None


class RedshiftAsyncHook(AwsBaseAsyncHook):
    """Interact with AWS Redshift using aiobotocore library"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "redshift"
        super().__init__(*args, **kwargs)

    async def cluster_status(self, cluster_identifier: str, delete_operation: bool = False) -> dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and get the status
        and returns the status of the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param delete_operation: whether the method has been called as part of delete cluster operation
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_clusters(ClusterIdentifier=cluster_identifier)
                cluster_state = (
                    response["Clusters"][0]["ClusterStatus"] if response and response["Clusters"] else None
                )
                return {"status": "success", "cluster_state": cluster_state}
            except botocore.exceptions.ClientError as error:
                if delete_operation and error.response.get("Error", {}).get("Code", "") == "ClusterNotFound":
                    return {"status": "success", "cluster_state": "cluster_not_found"}
                return {"status": "error", "message": str(error)}

    async def pause_cluster(self, cluster_identifier: str, poll_interval: float = 5.0) -> dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        pause the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param poll_interval: polling period in seconds to check for the status
        """
        try:
            async with await self.get_client_async() as client:
                response = await client.pause_cluster(ClusterIdentifier=cluster_identifier)
                status = response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
                if status == "pausing":
                    flag = asyncio.Event()
                    while True:
                        expected_response = await asyncio.create_task(
                            self.get_cluster_status(cluster_identifier, "paused", flag)
                        )
                        await asyncio.sleep(poll_interval)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error)}

    async def resume_cluster(
        self,
        cluster_identifier: str,
        polling_period_seconds: float = 5.0,
    ) -> dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        resume the cluster for the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param polling_period_seconds: polling period in seconds to check for the status
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.resume_cluster(ClusterIdentifier=cluster_identifier)
                status = response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
                if status == "resuming":
                    flag = asyncio.Event()
                    while True:
                        expected_response = await asyncio.create_task(
                            self.get_cluster_status(cluster_identifier, "available", flag)
                        )
                        await asyncio.sleep(polling_period_seconds)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}

    async def get_cluster_status(
        self,
        cluster_identifier: str,
        expected_state: str,
        flag: asyncio.Event,
        delete_operation: bool = False,
    ) -> dict[str, Any]:
        """
        check for expected Redshift cluster state

        :param cluster_identifier: unique identifier of a cluster
        :param expected_state: expected_state example("available", "pausing", "paused"")
        :param flag: asyncio even flag set true if success and if any error
        :param delete_operation: whether the method has been called as part of delete cluster operation
        """
        try:
            response = await self.cluster_status(cluster_identifier, delete_operation=delete_operation)
            if ("cluster_state" in response and response["cluster_state"] == expected_state) or response[
                "status"
            ] == "error":
                flag.set()
            return response
        except botocore.exceptions.ClientError as error:
            flag.set()
            return {"status": "error", "message": str(error)}
