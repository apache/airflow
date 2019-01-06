# -*- coding: utf-8 -*-
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

from google.cloud.bigtable import Client
from google.cloud.bigtable.cluster import Cluster
from google.cloud.bigtable.instance import Instance
from google.cloud.bigtable.table import Table
from google.cloud.bigtable_admin_v2 import enums
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class BigtableHook(GoogleCloudBaseHook):
    """
    Hook for Google Cloud Bigtable APIs.
    """

    _client = None

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        super(BigtableHook, self).__init__(gcp_conn_id, delegate_to)

    def _get_client(self, project_id):
        if not self._client:
            self._client = Client(project=project_id, credentials=self._get_credentials(),
                                  admin=True)
        return self._client

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_instance(self, instance_id, project_id=None):
        """
        Retrieves and returns the specified Cloud Bigtable instance if it exists.
        Otherwise, returns None.
        Must be called with keyword arguments rather than positional.

        :param instance_id: The ID of the Cloud Bigtable instance.
        :type instance_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        """

        instance = self._get_client(project_id=project_id).instance(instance_id)
        if not instance.exists():
            return None
        return instance

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_instance(self, instance_id, project_id=None):
        """
        Deletes the specified Cloud Bigtable instance.
        Must be called with keyword arguments rather than positional.
        Raises google.api_core.exceptions.NotFound if the Cloud Bigtable instance does
        not exist.

        :param project_id: Optional, Google Cloud Platform project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type project_id: str
        :param instance_id: The ID of the Cloud Bigtable instance.
        :type instance_id: str
        """
        instance = self.get_instance(instance_id=instance_id, project_id=project_id)
        if instance:
            instance.delete()
        else:
            self.log.info("The instance '%s' does not exist in project '%s'. Exiting", instance_id,
                          project_id)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def create_instance(self,
                        instance_id,
                        main_cluster_id,
                        main_cluster_zone,
                        project_id=None,
                        replica_cluster_id=None,
                        replica_cluster_zone=None,
                        instance_display_name=None,
                        instance_type=enums.Instance.Type.TYPE_UNSPECIFIED,
                        instance_labels=None,
                        cluster_nodes=None,
                        cluster_storage_type=enums.StorageType.STORAGE_TYPE_UNSPECIFIED,
                        timeout=None):
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
        :param project_id: Optional, Google Cloud Platform project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        :type replica_cluster_id: str
        :param replica_cluster_id: (optional) The ID for replica cluster for the new
            instance.
        :type replica_cluster_zone: str
        :param replica_cluster_zone: (optional)  The zone for replica cluster.
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

        clusters = [
            instance.cluster(
                main_cluster_id,
                main_cluster_zone,
                cluster_nodes,
                cluster_storage_type
            )
        ]
        if replica_cluster_id and replica_cluster_zone:
            clusters.append(instance.cluster(
                replica_cluster_id,
                replica_cluster_zone,
                cluster_nodes,
                cluster_storage_type
            ))
        operation = instance.create(
            clusters=clusters
        )
        operation.result(timeout)
        return instance

    @staticmethod
    def create_table(instance,
                     table_id,
                     initial_split_keys=None,
                     column_families=None):
        """
        Creates the specified Cloud Bigtable table.
        Raises google.api_core.exceptions.AlreadyExists if the table exists.

        :type instance: Instance
        :param instance: The Cloud Bigtable instance that owns the table.
        :type table_id: str
        :param table_id: The ID of the table to create in Cloud Bigtable.
        :type initial_split_keys: list
        :param initial_split_keys: (Optional) A list of row keys in bytes to use to
            initially split the table.
        :type column_families: dict
        :param column_families: (Optional) A map of columns to create. The key is the
            column_id str, and the
        value is a GarbageCollectionRule.
        """
        if column_families is None:
            column_families = {}
        if initial_split_keys is None:
            initial_split_keys = []
        table = Table(table_id, instance)
        table.create(initial_split_keys, column_families)

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def delete_table(self, instance_id, table_id, project_id=None):
        """
        Deletes the specified table in Cloud Bigtable.
        Raises google.api_core.exceptions.NotFound if the table does not exist.

        :type instance_id: str
        :param instance_id: The ID of the Cloud Bigtable instance.
        :type table_id: str
        :param table_id: The ID of the table in Cloud Bigtable.
        :type project_id: str
        :param project_id: Optional, Google Cloud Platform project ID where the
            BigTable exists. If set to None or missing,
            the default project_id from the GCP connection is used.
        """
        table = self.get_instance(instance_id=instance_id, project_id=project_id).table(table_id=table_id)
        table.delete()

    @staticmethod
    def update_cluster(instance, cluster_id, nodes):
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
    def get_column_families_for_table(instance, table_id):
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
    def get_cluster_states_for_table(instance, table_id):
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
