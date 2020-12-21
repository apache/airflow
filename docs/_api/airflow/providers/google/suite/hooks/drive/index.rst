:mod:`airflow.providers.google.suite.hooks.drive`
=================================================

.. py:module:: airflow.providers.google.suite.hooks.drive

.. autoapi-nested-parse::

   Hook for Google Drive service



Module Contents
---------------

.. py:class:: GoogleDriveHook(api_version: str = 'v3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for the Google Drive APIs.

   :param api_version: API version used (for example v3).
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
      :annotation: :Optional[Resource]

      

   
   .. method:: get_conn(self)

      Retrieves the connection to Google Drive.

      :return: Google Drive services object.



   
   .. method:: _ensure_folders_exists(self, path: str)



   
   .. method:: upload_file(self, local_location: str, remote_location: str)

      Uploads a file that is available locally to a Google Drive service.

      :param local_location: The path where the file is available.
      :type local_location: str
      :param remote_location: The path where the file will be send
      :type remote_location: str
      :return: File ID
      :rtype: str




