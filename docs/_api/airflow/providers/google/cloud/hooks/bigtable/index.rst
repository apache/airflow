:mod:`airflow.providers.google.cloud.hooks.bigtable`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.bigtable

.. autoapi-nested-parse::

   This module contains a Google Cloud Bigtable Hook.



Module Contents
---------------

.. py:class:: BigtableHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Bigtable APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: _get_client(self, project_id: str)



   
   .. method:: get_instance(self, instance_id: str, project_id: str)

      Retrieves and returns the specified Cloud Bigtable instance if it exists.
      Otherwise, returns None.

      :param instance_id: The ID of the Cloud Bigtable instance.
      :type instance_id: str
      :param project_id: Optional, Google Cloud  project ID where the
          BigTable exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str



   
   .. method:: delete_instance(self, instance_id: str, project_id: str)

      Deletes the specified Cloud Bigtable instance.
      Raises google.api_core.exceptions.NotFound if the Cloud Bigtable instance does
      not exist.

      :param project_id: Optional, Google Cloud project ID where the
          BigTable exists. If set to None or missing,
          the default project_id from the Google Cloud connection is used.
      :type project_id: str
      :param instance_id: The ID of the Cloud Bigtable instance.
      :type instance_id: str



   
   .. method:: create_instance(self, instance_id: str, main_cluster_id: str, main_cluster_zone: str, project_id: str, replica_clusters: Optional[List[Dict[str, str]]] = None, replica_cluster_id: Optional[str] = None, replica_cluster_zone: Optional[str] = None, instance_display_name: Optional[str] = None, instance_type: enums.Instance.Type = enums.Instance.Type.TYPE_UNSPECIFIED, instance_labels: Optional[Dict] = None, cluster_nodes: Optional[int] = None, cluster_storage_type: enums.StorageType = enums.StorageType.STORAGE_TYPE_UNSPECIFIED, timeout: Optional[float] = None)

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



   
   .. method:: update_instance(self, instance_id: str, project_id: str, instance_display_name: Optional[str] = None, instance_type: Optional[Union[enums.Instance.Type, enum.IntEnum]] = None, instance_labels: Optional[Dict] = None, timeout: Optional[float] = None)

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



   
   .. staticmethod:: create_table(instance: Instance, table_id: str, initial_split_keys: Optional[List] = None, column_families: Optional[Dict[str, GarbageCollectionRule]] = None)

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



   
   .. method:: delete_table(self, instance_id: str, table_id: str, project_id: str)

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



   
   .. staticmethod:: update_cluster(instance: Instance, cluster_id: str, nodes: int)

      Updates number of nodes in the specified Cloud Bigtable cluster.
      Raises google.api_core.exceptions.NotFound if the cluster does not exist.

      :type instance: Instance
      :param instance: The Cloud Bigtable instance that owns the cluster.
      :type cluster_id: str
      :param cluster_id: The ID of the cluster.
      :type nodes: int
      :param nodes: The desired number of nodes.



   
   .. staticmethod:: get_column_families_for_table(instance: Instance, table_id: str)

      Fetches Column Families for the specified table in Cloud Bigtable.

      :type instance: Instance
      :param instance: The Cloud Bigtable instance that owns the table.
      :type table_id: str
      :param table_id: The ID of the table in Cloud Bigtable to fetch Column Families
          from.



   
   .. staticmethod:: get_cluster_states_for_table(instance: Instance, table_id: str)

      Fetches Cluster States for the specified table in Cloud Bigtable.
      Raises google.api_core.exceptions.NotFound if the table does not exist.

      :type instance: Instance
      :param instance: The Cloud Bigtable instance that owns the table.
      :type table_id: str
      :param table_id: The ID of the table in Cloud Bigtable to fetch Cluster States
          from.




