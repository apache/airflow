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
This module contains a Google Cloud Bigtable Hook.
"""
import enum
import warnings
from typing import Dict, List, Optional, Sequence, Union

from google.cloud.bigtable import Client
from google.cloud.bigtable.cluster import Cluster
from google.cloud.bigtable.column_family import ColumnFamily, GarbageCollectionRule
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import ClusterState, Table
from google.cloud.bigtable_admin_v2 import enums

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class BigtableHook(GoogleBaseHook):
    """
    Hook for Google Cloud Bigtable APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to, impersonation_chain=impersonation_chain,
        )
        self._client = None

    def _get_client(self, project_id: str):
        if not self._client:
            self._client = Client(
                project=project_id,
                credentials=self._get_credentials(),
                client_info=self.client_info,
                admin=True,
            )
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def get_instance(self, instance_id: str, project_id: str) -> Instance:
        """
        Retrieves and returns the specified Cloud Bigtable instance if it exists.
        Otherwise, returns None.

        :param instance_id: The ID of the Cloud Bigtable instance.
        :type instance_id: str
        :param project_id: Optional, Google Cloud  project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :type project_id: str
        """
        instance = self._get_client(project_id=project_id).instance(instance_id)
        if not instance.exists():
            return None
        return instance

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_instance(self, instance_id: str, project_id: str) -> None:
        """
        Deletes the specified Cloud Bigtable instance.
        Raises google.api_core.exceptions.NotFound if the Cloud Bigtable instance does
        not exist.

        :param project_id: Optional, Google Cloud project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param instance_id: The ID of the Cloud Bigtable instance.
        :type instance_id: str
        """
        instance = self.get_instance(instance_id=instance_id, project_id=project_id)
        if instance:
            instance.delete()
        else:
            self.log.warning(
                "The instance '%s' does not exist in project '%s'. Exiting", instance_id, project_id
            )

    @GoogleBaseHook.fallback_to_default_project_id
    def create_instance(
        self,
        instance_id: str,
        main_cluster_id: str,
        main_cluster_zone: str,
        project_id: str,
        replica_clusters: Optional[List[Dict[str, str]]] = None,
        replica_cluster_id: Optional[str] = None,
        replica_cluster_zone: Optional[str] = None,
        instance_display_name: Optional[str] = None,
        instance_type: enums.Instance.Type = enums.Instance.Type.TYPE_UNSPECIFIED,
        instance_labels: Optional[Dict] = None,
        cluster_nodes: Optional[int] = None,
        cluster_storage_type: enums.StorageType = enums.StorageType.STORAGE_TYPE_UNSPECIFIED,
        timeout: Optional[float] = None,
    ) -> Instance:
        """
        Creates new instance.

        :type instance_id: str
        :param instance_id: The ID for the new instance.
        :type main_cluster_id: str
        :param main_cluster_id: The ID for main cluster for the new instance.
        :type main_cluster_zone: str
        :param main_cluster_zone: The zone for main cluster.
            See https://cloud.google.com/bigtable/docs/locations for more details.
        :type project_id: str
        :param project_id: Optional, Google Cloud project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :type replica_clusters: List[Dict[str, str]]
        :param replica_clusters: (optional) A list of replica clusters for the new
            instance. Each cluster dictionary contains an id and a zone.
            Example: [{"id": "replica-1", "zone": "us-west1-a"}]
        :type replica_cluster_id: str
        :param replica_cluster_id: (deprecated) The ID for replica cluster for the new
            instance.
        :type replica_cluster_zone: str
        :param replica_cluster_zone: (deprecated)  The zone for replica cluster.
        :type instance_type: enums.Instance.Type
        :param instance_type: (optional) The type of the instance.
        :type instance_display_name: str
        :param instance_display_name: (optional) Human-readable name of the instance.
                Defaults to ``instance_id``.
        :type instance_labels: dict
        :param instance_labels: (optional) Dictionary of labels to associate with the
            instance.
        :type cluster_nodes: int
        :param cluster_nodes: (optional) Number of nodes for cluster.
        :type cluster_storage_type: enums.StorageType
        :param cluster_storage_type: (optional) The type of storage.
        :type timeout: int
        :param timeout: (optional) timeout (in seconds) for instance creation.
                        If None is not specified, Operator will wait indefinitely.
        """
        cluster_storage_type = enums.StorageType(cluster_storage_type)
        instance_type = enums.Instance.Type(instance_type)

        instance = Instance(
            instance_id,
            self._get_client(project_id=project_id),
            instance_display_name,
            instance_type,
            instance_labels,
        )

        clusters = [instance.cluster(main_cluster_id, main_cluster_zone, cluster_nodes, cluster_storage_type)]
        if replica_cluster_id and replica_cluster_zone:
            warnings.warn(
                "The replica_cluster_id and replica_cluster_zone parameter have been deprecated."
                "You should pass the replica_clusters parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            clusters.append(
                instance.cluster(
                    replica_cluster_id, replica_cluster_zone, cluster_nodes, cluster_storage_type
                )
            )
        if replica_clusters:
            for replica_cluster in replica_clusters:
                if "id" in replica_cluster and "zone" in replica_cluster:
                    clusters.append(
                        instance.cluster(
                            replica_cluster["id"],
                            replica_cluster["zone"],
                            cluster_nodes,
                            cluster_storage_type,
                        )
                    )
        operation = instance.create(clusters=clusters)
        operation.result(timeout)
        return instance

    @GoogleBaseHook.fallback_to_default_project_id
    def update_instance(
        self,
        instance_id: str,
        project_id: str,
        instance_display_name: Optional[str] = None,
        instance_type: Optional[Union[enums.Instance.Type, enum.IntEnum]] = None,
        instance_labels: Optional[Dict] = None,
        timeout: Optional[float] = None,
    ) -> Instance:
        """
        Update an existing instance.

        :type instance_id: str
        :param instance_id: The ID for the existing instance.
        :type project_id: str
        :param project_id: Optional, Google Cloud project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        :type instance_display_name: str
        :param instance_display_name: (optional) Human-readable name of the instance.
        :type instance_type: enums.Instance.Type or enum.IntEnum
        :param instance_type: (optional) The type of the instance.
        :type instance_labels: dict
        :param instance_labels: (optional) Dictionary of labels to associate with the
            instance.
        :type timeout: int
        :param timeout: (optional) timeout (in seconds) for instance update.
            If None is not specified, Operator will wait indefinitely.
        """
        instance_type = enums.Instance.Type(instance_type)

        instance = Instance(
            instance_id=instance_id,
            client=self._get_client(project_id=project_id),
            display_name=instance_display_name,
            instance_type=instance_type,
            labels=instance_labels,
        )

        operation = instance.update()
        operation.result(timeout)

        return instance

    @staticmethod
    def create_table(
        instance: Instance,
        table_id: str,
        initial_split_keys: Optional[List] = None,
        column_families: Optional[Dict[str, GarbageCollectionRule]] = None,
    ) -> None:
        """
        Creates the specified Cloud Bigtable table.
        Raises ``google.api_core.exceptions.AlreadyExists`` if the table exists.

        :type instance: Instance
        :param instance: The Cloud Bigtable instance that owns the table.
        :type table_id: str
        :param table_id: The ID of the table to create in Cloud Bigtable.
        :type initial_split_keys: list
        :param initial_split_keys: (Optional) A list of row keys in bytes to use to
            initially split the table.
        :type column_families: dict
        :param column_families: (Optional) A map of columns to create. The key is the
            column_id str, and the value is a
            :class:`google.cloud.bigtable.column_family.GarbageCollectionRule`.
        """
        if column_families is None:
            column_families = {}
        if initial_split_keys is None:
            initial_split_keys = []
        table = Table(table_id, instance)
        table.create(initial_split_keys, column_families)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_table(self, instance_id: str, table_id: str, project_id: str) -> None:
        """
        Deletes the specified table in Cloud Bigtable.
        Raises google.api_core.exceptions.NotFound if the table does not exist.

        :type instance_id: str
        :param instance_id: The ID of the Cloud Bigtable instance.
        :type table_id: str
        :param table_id: The ID of the table in Cloud Bigtable.
        :type project_id: str
        :param project_id: Optional, Google Cloud project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the Google Cloud connection is used.
        """
        table = self.get_instance(instance_id=instance_id, project_id=project_id).table(table_id=table_id)
        table.delete()

    @staticmethod
    def update_cluster(instance: Instance, cluster_id: str, nodes: int) -> None:
        """
        Updates number of nodes in the specified Cloud Bigtable cluster.
        Raises google.api_core.exceptions.NotFound if the cluster does not exist.

        :type instance: Instance
        :param instance: The Cloud Bigtable instance that owns the cluster.
        :type cluster_id: str
        :param cluster_id: The ID of the cluster.
        :type nodes: int
        :param nodes: The desired number of nodes.
        """
        cluster = Cluster(cluster_id, instance)
        cluster.serve_nodes = nodes
        cluster.update()

    @staticmethod
    def get_column_families_for_table(instance: Instance, table_id: str) -> Dict[str, ColumnFamily]:
        """
        Fetches Column Families for the specified table in Cloud Bigtable.

        :type instance: Instance
        :param instance: The Cloud Bigtable instance that owns the table.
        :type table_id: str
        :param table_id: The ID of the table in Cloud Bigtable to fetch Column Families
            from.
        """

        table = Table(table_id, instance)
        return table.list_column_families()

    @staticmethod
    def get_cluster_states_for_table(instance: Instance, table_id: str) -> Dict[str, ClusterState]:
        """
        Fetches Cluster States for the specified table in Cloud Bigtable.
        Raises google.api_core.exceptions.NotFound if the table does not exist.

        :type instance: Instance
        :param instance: The Cloud Bigtable instance that owns the table.
        :type table_id: str
        :param table_id: The ID of the table in Cloud Bigtable to fetch Cluster States
            from.
        """

        table = Table(table_id, instance)
        return table.get_cluster_states()
