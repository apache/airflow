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
"""
Interact with AWS Redshift, using the boto3 library.
"""

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class RedshiftHook(AwsBaseHook):
    """
    Interact with AWS Redshift, using the boto3 library
    """
    def get_conn(self):
        return self.get_client_type('redshift')

    # TODO: Wrap create_cluster_snapshot
    def cluster_status(self, cluster_identifier):
        """
        Return status of a cluster

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        conn = self.get_conn()
        try:
            response = conn.describe_clusters(
                ClusterIdentifier=cluster_identifier)['Clusters']
            return response[0]['ClusterStatus'] if response else None
        except conn.exceptions.ClusterNotFoundFault:
            return 'cluster_not_found'

    def delete_cluster(  # pylint: disable=invalid-name
            self,
            cluster_identifier,
            skip_final_cluster_snapshot=True,
            final_cluster_snapshot_identifier=''):
        """
        Delete a cluster and optionally create a snapshot

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :type skip_final_cluster_snapshot: bool
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        :type final_cluster_snapshot_identifier: str
        """
        response = self.get_conn().delete_cluster(
            ClusterIdentifier=cluster_identifier,
            SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
            FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier
        )
        return response['Cluster'] if response['Cluster'] else None

    def describe_cluster_snapshots(self, cluster_identifier):
        """
        Gets a list of snapshots for a cluster

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        response = self.get_conn().describe_cluster_snapshots(
            ClusterIdentifier=cluster_identifier
        )
        if 'Snapshots' not in response:
            return None
        snapshots = response['Snapshots']
        snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
        snapshots.sort(key=lambda x: x['SnapshotCreateTime'], reverse=True)
        return snapshots

    def restore_from_cluster_snapshot(self, cluster_identifier, snapshot_identifier):
        """
        Restores a cluster from its snapshot

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param snapshot_identifier: unique identifier for a snapshot of a cluster
        :type snapshot_identifier: str
        """
        response = self.get_conn().restore_from_cluster_snapshot(
            ClusterIdentifier=cluster_identifier,
            SnapshotIdentifier=snapshot_identifier
        )
        return response['Cluster'] if response['Cluster'] else None

    def create_cluster_snapshot(self, snapshot_identifier, cluster_identifier):
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
