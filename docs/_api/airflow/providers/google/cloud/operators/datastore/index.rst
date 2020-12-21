:mod:`airflow.providers.google.cloud.operators.datastore`
=========================================================

.. py:module:: airflow.providers.google.cloud.operators.datastore

.. autoapi-nested-parse::

   This module contains Google Datastore operators.



Module Contents
---------------

.. py:class:: CloudDatastoreExportEntitiesOperator(*, bucket: str, namespace: Optional[str] = None, datastore_conn_id: str = 'google_cloud_default', cloud_storage_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, entity_filter: Optional[dict] = None, labels: Optional[dict] = None, polling_interval_in_seconds: int = 10, overwrite_existing: bool = False, project_id: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Export entities from Google Cloud Datastore to Cloud Storage

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreExportEntitiesOperator`

   :param bucket: name of the cloud storage bucket to backup data
   :type bucket: str
   :param namespace: optional namespace path in the specified Cloud Storage bucket
       to backup data. If this namespace does not exist in GCS, it will be created.
   :type namespace: str
   :param datastore_conn_id: the name of the Datastore connection id to use
   :type datastore_conn_id: str
   :param cloud_storage_conn_id: the name of the cloud storage connection id to
       force-write backup
   :type cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param entity_filter: description of what data from the project is included in the
       export, refer to
       https://cloud.google.com/datastore/docs/reference/rest/Shared.Types/EntityFilter
   :type entity_filter: dict
   :param labels: client-assigned labels for cloud storage
   :type labels: dict
   :param polling_interval_in_seconds: number of seconds to wait before polling for
       execution status again
   :type polling_interval_in_seconds: int
   :param overwrite_existing: if the storage bucket + namespace is not empty, it will be
       emptied prior to exports. This enables overwriting existing backups.
   :type overwrite_existing: bool
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'namespace', 'entity_filter', 'labels', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreImportEntitiesOperator(*, bucket: str, file: str, namespace: Optional[str] = None, entity_filter: Optional[dict] = None, labels: Optional[dict] = None, datastore_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, polling_interval_in_seconds: float = 10, project_id: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Import entities from Cloud Storage to Google Cloud Datastore

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreImportEntitiesOperator`

   :param bucket: container in Cloud Storage to store data
   :type bucket: str
   :param file: path of the backup metadata file in the specified Cloud Storage bucket.
       It should have the extension .overall_export_metadata
   :type file: str
   :param namespace: optional namespace of the backup metadata file in
       the specified Cloud Storage bucket.
   :type namespace: str
   :param entity_filter: description of what data from the project is included in
       the export, refer to
       https://cloud.google.com/datastore/docs/reference/rest/Shared.Types/EntityFilter
   :type entity_filter: dict
   :param labels: client-assigned labels for cloud storage
   :type labels: dict
   :param datastore_conn_id: the name of the connection id to use
   :type datastore_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param polling_interval_in_seconds: number of seconds to wait before polling for
       execution status again
   :type polling_interval_in_seconds: float
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['bucket', 'file', 'namespace', 'entity_filter', 'labels', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreAllocateIdsOperator(*, partial_keys: List, project_id: Optional[str] = None, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Allocate IDs for incomplete keys. Return list of keys.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreAllocateIdsOperator`

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/rest/v1/projects/allocateIds

   :param partial_keys: a list of partial keys.
   :type partial_keys: list
   :param project_id: Google Cloud project ID against which to make the request.
   :type project_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['partial_keys', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreBeginTransactionOperator(*, transaction_options: Dict[str, Any], project_id: Optional[str] = None, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Begins a new transaction. Returns a transaction handle.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreBeginTransactionOperator`

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/rest/v1/projects/beginTransaction

   :param transaction_options: Options for a new transaction.
   :type transaction_options: Dict[str, Any]
   :param project_id: Google Cloud project ID against which to make the request.
   :type project_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['transaction_options', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreCommitOperator(*, body: Dict[str, Any], project_id: Optional[str] = None, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Commit a transaction, optionally creating, deleting or modifying some entities.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreCommitOperator`

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/rest/v1/projects/commit

   :param body: the body of the commit request.
   :type body: dict
   :param project_id: Google Cloud project ID against which to make the request.
   :type project_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['body', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreRollbackOperator(*, transaction: str, project_id: Optional[str] = None, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Roll back a transaction.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreRollbackOperator`

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/rest/v1/projects/rollback

   :param transaction: the transaction to roll back.
   :type transaction: str
   :param project_id: Google Cloud project ID against which to make the request.
   :type project_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['transaction', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreRunQueryOperator(*, body: Dict[str, Any], project_id: Optional[str] = None, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Run a query for entities. Returns the batch of query results.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDatastoreRunQueryOperator`

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/rest/v1/projects/runQuery

   :param body: the body of the query request.
   :type body: dict
   :param project_id: Google Cloud project ID against which to make the request.
   :type project_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['body', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreGetOperationOperator(*, name: str, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets the latest state of a long-running operation.

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get

   :param name: the name of the operation resource.
   :type name: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['name', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDatastoreDeleteOperationOperator(*, name: str, delegate_to: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes the long-running operation.

   .. seealso::
       https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/delete

   :param name: the name of the operation resource.
   :type name: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
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

   .. attribute:: template_fields
      :annotation: = ['name', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




