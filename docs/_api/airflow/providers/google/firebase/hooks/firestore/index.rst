:mod:`airflow.providers.google.firebase.hooks.firestore`
========================================================

.. py:module:: airflow.providers.google.firebase.hooks.firestore

.. autoapi-nested-parse::

   Hook for Google Cloud Firestore service



Module Contents
---------------

.. data:: TIME_TO_SLEEP_IN_SECONDS
   :annotation: = 5

   

.. py:class:: CloudFirestoreHook(api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for the Google Firestore APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   :param api_version: API version used (for example v1 or v1beta1).
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: _conn
      :annotation: :Optional[Any]

      

   
   .. method:: get_conn(self)

      Retrieves the connection to Cloud Firestore.

      :return: Google Cloud Firestore services object.



   
   .. method:: export_documents(self, body: Dict, database_id: str = '(default)', project_id: Optional[str] = None)

      Starts a export with the specified configuration.

      :param database_id: The Database ID.
      :type database_id: str
      :param body: The request body.
          See:
          https://firebase.google.com/docs/firestore/reference/rest/v1beta1/projects.databases/exportDocuments
      :type body: dict
      :param project_id: Optional, Google Cloud Project project_id where the database belongs.
          If set to None or missing, the default project_id from the Google Cloud connection is used.
      :type project_id: str



   
   .. method:: _wait_for_operation_to_complete(self, operation_name: str)

      Waits for the named operation to complete - checks status of the
      asynchronous call.

      :param operation_name: The name of the operation.
      :type operation_name: str
      :return: The response returned by the operation.
      :rtype: dict
      :exception: AirflowException in case error is returned.




