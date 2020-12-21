:mod:`airflow.providers.google.cloud.operators.bigtable`
========================================================

.. py:module:: airflow.providers.google.cloud.operators.bigtable

.. autoapi-nested-parse::

   This module contains Google Cloud Bigtable operators.



Module Contents
---------------

.. py:class:: BigtableValidationMixin

   Common class for Cloud Bigtable operators for validating required fields.

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = []

      

   
   .. method:: _validate_inputs(self)




.. py:class:: BigtableCreateInstanceOperator(*, instance_id: str, main_cluster_id: str, main_cluster_zone: str, project_id: Optional[str] = None, replica_clusters: Optional[List[Dict[str, str]]] = None, replica_cluster_id: Optional[str] = None, replica_cluster_zone: Optional[str] = None, instance_display_name: Optional[str] = None, instance_type: Optional[enums.Instance.Type] = None, instance_labels: Optional[Dict] = None, cluster_nodes: Optional[int] = None, cluster_storage_type: Optional[enums.StorageType] = None, timeout: Optional[float] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Creates a new Cloud Bigtable instance.
   If the Cloud Bigtable instance with the given ID exists, the operator does not
   compare its configuration
   and immediately succeeds. No changes are made to the existing instance.

   For more details about instance creation have a look at the reference:
   https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.create

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableCreateInstanceOperator`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance to create.
   :type main_cluster_id: str
   :param main_cluster_id: The ID for main cluster for the new instance.
   :type main_cluster_zone: str
   :param main_cluster_zone: The zone for main cluster
       See https://cloud.google.com/bigtable/docs/locations for more details.
   :type project_id: str
   :param project_id: Optional, the ID of the Google Cloud project.  If set to None or missing,
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
   :type instance_type: enum.IntEnum
   :param instance_type: (optional) The type of the instance.
   :type instance_display_name: str
   :param instance_display_name: (optional) Human-readable name of the instance. Defaults
       to ``instance_id``.
   :type instance_labels: dict
   :param instance_labels: (optional) Dictionary of labels to associate
       with the instance.
   :type cluster_nodes: int
   :param cluster_nodes: (optional) Number of nodes for cluster.
   :type cluster_storage_type: enum.IntEnum
   :param cluster_storage_type: (optional) The type of storage.
   :type timeout: int
   :param timeout: (optional) timeout (in seconds) for instance creation.
                   If None is not specified, Operator will wait indefinitely.
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = ['instance_id', 'main_cluster_id', 'main_cluster_zone']

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['project_id', 'instance_id', 'main_cluster_id', 'main_cluster_zone', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: BigtableUpdateInstanceOperator(*, instance_id: str, project_id: Optional[str] = None, instance_display_name: Optional[str] = None, instance_type: Optional[Union[enums.Instance.Type, enum.IntEnum]] = None, instance_labels: Optional[Dict] = None, timeout: Optional[float] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Updates an existing Cloud Bigtable instance.

   For more details about instance creation have a look at the reference:
   https://googleapis.dev/python/bigtable/latest/instance.html#google.cloud.bigtable.instance.Instance.update

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableUpdateInstanceOperator`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance to update.
   :type project_id: str
   :param project_id: Optional, the ID of the Google Cloud project. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type instance_display_name: str
   :param instance_display_name: (optional) Human-readable name of the instance.
   :type instance_type: enums.Instance.Type or enum.IntEnum
   :param instance_type: (optional) The type of the instance.
   :type instance_labels: dict
   :param instance_labels: (optional) Dictionary of labels to associate
       with the instance.
   :type timeout: int
   :param timeout: (optional) timeout (in seconds) for instance update.
                   If None is not specified, Operator will wait indefinitely.
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = ['instance_id']

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['project_id', 'instance_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: BigtableDeleteInstanceOperator(*, instance_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Deletes the Cloud Bigtable instance, including its clusters and all related tables.

   For more details about deleting instance have a look at the reference:
   https://googleapis.github.io/google-cloud-python/latest/bigtable/instance.html#google.cloud.bigtable.instance.Instance.delete

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableDeleteInstanceOperator`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance to delete.
   :param project_id: Optional, the ID of the Google Cloud project.  If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = ['instance_id']

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['project_id', 'instance_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: BigtableCreateTableOperator(*, instance_id: str, table_id: str, project_id: Optional[str] = None, initial_split_keys: Optional[List] = None, column_families: Optional[Dict[str, GarbageCollectionRule]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Creates the table in the Cloud Bigtable instance.

   For more details about creating table have a look at the reference:
   https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.create

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableCreateTableOperator`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance that will
       hold the new table.
   :type table_id: str
   :param table_id: The ID of the table to be created.
   :type project_id: str
   :param project_id: Optional, the ID of the Google Cloud project. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type initial_split_keys: list
   :param initial_split_keys: (Optional) list of row keys in bytes that will be used to
       initially split the table into several tablets.
   :type column_families: dict
   :param column_families: (Optional) A map columns to create.
                           The key is the column_id str and the value is a
                           :class:`google.cloud.bigtable.column_family.GarbageCollectionRule`
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = ['instance_id', 'table_id']

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['project_id', 'instance_id', 'table_id', 'impersonation_chain']

      

   
   .. method:: _compare_column_families(self, hook, instance)



   
   .. method:: execute(self, context)




.. py:class:: BigtableDeleteTableOperator(*, instance_id: str, table_id: str, project_id: Optional[str] = None, app_profile_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Deletes the Cloud Bigtable table.

   For more details about deleting table have a look at the reference:
   https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html#google.cloud.bigtable.table.Table.delete

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableDeleteTableOperator`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance.
   :type table_id: str
   :param table_id: The ID of the table to be deleted.
   :type project_id: str
   :param project_id: Optional, the ID of the Google Cloud project. If set to None or missing,
           the default project_id from the Google Cloud connection is used.
   :type app_profile_id: str
   :param app_profile_id: Application profile.
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = ['instance_id', 'table_id']

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['project_id', 'instance_id', 'table_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: BigtableUpdateClusterOperator(*, instance_id: str, cluster_id: str, nodes: int, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`, :class:`airflow.providers.google.cloud.operators.bigtable.BigtableValidationMixin`

   Updates a Cloud Bigtable cluster.

   For more details about updating a Cloud Bigtable cluster,
   have a look at the reference:
   https://googleapis.github.io/google-cloud-python/latest/bigtable/cluster.html#google.cloud.bigtable.cluster.Cluster.update

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigtableUpdateClusterOperator`

   :type instance_id: str
   :param instance_id: The ID of the Cloud Bigtable instance.
   :type cluster_id: str
   :param cluster_id: The ID of the Cloud Bigtable cluster to update.
   :type nodes: int
   :param nodes: The desired number of nodes for the Cloud Bigtable cluster.
   :type project_id: str
   :param project_id: Optional, the ID of the Google Cloud project.
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: REQUIRED_ATTRIBUTES
      :annotation: :Iterable[str] = ['instance_id', 'cluster_id', 'nodes']

      

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['project_id', 'instance_id', 'cluster_id', 'nodes', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




