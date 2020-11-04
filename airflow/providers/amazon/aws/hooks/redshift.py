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
"""Interact with AWS Redshift, using the boto3 library."""

from typing import List, Optional

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class RedshiftHook(AwsBaseHook):
    """
    Interact with AWS Redshift, using the boto3 library

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift"
        super().__init__(*args, **kwargs)

    # TODO: Wrap create_cluster_snapshot
    def cluster_status(self, cluster_identifier: str) -> str:
        """
        Return status of a cluster

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        try:
            response = self.get_conn().describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters']
            return response[0]['ClusterStatus'] if response else None
        except self.get_conn().exceptions.ClusterNotFoundFault:
            return 'cluster_not_found'

    def delete_cluster(  # pylint: disable=invalid-name
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: Optional[str] = None,
    ):
        """
        Delete a cluster and optionally create a snapshot

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :type skip_final_cluster_snapshot: bool
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        :type final_cluster_snapshot_identifier: str
        """
        final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ''

        response = self.get_conn().delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
            FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
        )
        return response['Cluster'] if response['Cluster'] else None

    def describe_cluster_snapshots(self, cluster_identifier: str) -> Optional[List[str]]:
        """
        Gets a list of snapshots for a cluster

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        response = self.get_conn().describe_cluster_snapshots(ClusterIdentifier=cluster_identifier)
        if 'Snapshots' not in response:
            return None
        snapshots = response['Snapshots']
        snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
        snapshots.sort(key=lambda x: x['SnapshotCreateTime'], reverse=True)
        return snapshots

    def restore_from_cluster_snapshot(self, cluster_identifier: str, snapshot_identifier: str) -> str:
        """
        Restores a cluster from its snapshot

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :type snapshot_identifier: str
        """
        response = self.get_conn().restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier, SnapshotIdentifier=snapshot_identifier
        )
        return response['Cluster'] if response['Cluster'] else None

    def create_cluster_snapshot(self, snapshot_identifier: str, cluster_identifier: str) -> str:
        """
        Creates a snapshot of a cluster

        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :type snapshot_identifier: str
        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        response = self.get_conn().create_cluster_snapshot(
            SnapshotIdentifier=snapshot_identifier,
            ClusterIdentifier=cluster_identifier,
        )
        return response['Snapshot'] if response['Snapshot'] else None
