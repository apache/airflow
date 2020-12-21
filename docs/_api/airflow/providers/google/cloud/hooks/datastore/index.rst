:mod:`airflow.providers.google.cloud.hooks.datastore`
=====================================================

.. py:module:: airflow.providers.google.cloud.hooks.datastore

.. autoapi-nested-parse::

   This module contains Google Datastore hook.



Module Contents
---------------

.. py:class:: DatastoreHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, api_version: str = 'v1', datastore_conn_id: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Interact with Google Cloud Datastore. This hook uses the Google Cloud connection.

   This object is not threads safe. If you want to make multiple requests
   simultaneously, you will need to create a hook per thread.

   :param api_version: The version of the API it is going to connect to.
   :type api_version: str

   
   .. method:: get_conn(self)

      Establishes a connection to the Google API.

      :return: a Google Cloud Datastore service object.
      :rtype: Resource



   
   .. method:: allocate_ids(self, partial_keys: list, project_id: str)

      Allocate IDs for incomplete keys.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/rest/v1/projects/allocateIds

      :param partial_keys: a list of partial keys.
      :type partial_keys: list
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :return: a list of full keys.
      :rtype: list



   
   .. method:: begin_transaction(self, project_id: str, transaction_options: Dict[str, Any])

      Begins a new transaction.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/rest/v1/projects/beginTransaction

      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :param transaction_options: Options for a new transaction.
      :type transaction_options: Dict[str, Any]
      :return: a transaction handle.
      :rtype: str



   
   .. method:: commit(self, body: dict, project_id: str)

      Commit a transaction, optionally creating, deleting or modifying some entities.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/rest/v1/projects/commit

      :param body: the body of the commit request.
      :type body: dict
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :return: the response body of the commit request.
      :rtype: dict



   
   .. method:: lookup(self, keys: list, project_id: str, read_consistency: Optional[str] = None, transaction: Optional[str] = None)

      Lookup some entities by key.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/rest/v1/projects/lookup

      :param keys: the keys to lookup.
      :type keys: list
      :param read_consistency: the read consistency to use. default, strong or eventual.
                               Cannot be used with a transaction.
      :type read_consistency: str
      :param transaction: the transaction to use, if any.
      :type transaction: str
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :return: the response body of the lookup request.
      :rtype: dict



   
   .. method:: rollback(self, transaction: str, project_id: str)

      Roll back a transaction.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/rest/v1/projects/rollback

      :param transaction: the transaction to roll back.
      :type transaction: str
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str



   
   .. method:: run_query(self, body: dict, project_id: str)

      Run a query for entities.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/rest/v1/projects/runQuery

      :param body: the body of the query request.
      :type body: dict
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :return: the batch of query results.
      :rtype: dict



   
   .. method:: get_operation(self, name: str)

      Gets the latest state of a long-running operation.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/get

      :param name: the name of the operation resource.
      :type name: str
      :return: a resource operation instance.
      :rtype: dict



   
   .. method:: delete_operation(self, name: str)

      Deletes the long-running operation.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects.operations/delete

      :param name: the name of the operation resource.
      :type name: str
      :return: none if successful.
      :rtype: dict



   
   .. method:: poll_operation_until_done(self, name: str, polling_interval_in_seconds: int)

      Poll backup operation state until it's completed.

      :param name: the name of the operation resource
      :type name: str
      :param polling_interval_in_seconds: The number of seconds to wait before calling another request.
      :type polling_interval_in_seconds: int
      :return: a resource operation instance.
      :rtype: dict



   
   .. method:: export_to_storage_bucket(self, bucket: str, project_id: str, namespace: Optional[str] = None, entity_filter: Optional[dict] = None, labels: Optional[Dict[str, str]] = None)

      Export entities from Cloud Datastore to Cloud Storage for backup.

      .. note::
          Keep in mind that this requests the Admin API not the Data API.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/export

      :param bucket: The name of the Cloud Storage bucket.
      :type bucket: str
      :param namespace: The Cloud Storage namespace path.
      :type namespace: str
      :param entity_filter: Description of what data from the project is included in the export.
      :type entity_filter: dict
      :param labels: Client-assigned labels.
      :type labels: dict of str
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :return: a resource operation instance.
      :rtype: dict



   
   .. method:: import_from_storage_bucket(self, bucket: str, file: str, project_id: str, namespace: Optional[str] = None, entity_filter: Optional[dict] = None, labels: Optional[Union[dict, str]] = None)

      Import a backup from Cloud Storage to Cloud Datastore.

      .. note::
          Keep in mind that this requests the Admin API not the Data API.

      .. seealso::
          https://cloud.google.com/datastore/docs/reference/admin/rest/v1/projects/import

      :param bucket: The name of the Cloud Storage bucket.
      :type bucket: str
      :param file: the metadata file written by the projects.export operation.
      :type file: str
      :param namespace: The Cloud Storage namespace path.
      :type namespace: str
      :param entity_filter: specify which kinds/namespaces are to be imported.
      :type entity_filter: dict
      :param labels: Client-assigned labels.
      :type labels: dict of str
      :param project_id: Google Cloud project ID against which to make the request.
      :type project_id: str
      :return: a resource operation instance.
      :rtype: dict




