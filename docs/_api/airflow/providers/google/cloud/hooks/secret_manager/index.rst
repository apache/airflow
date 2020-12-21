:mod:`airflow.providers.google.cloud.hooks.secret_manager`
==========================================================

.. py:module:: airflow.providers.google.cloud.hooks.secret_manager

.. autoapi-nested-parse::

   Hook for Secrets Manager service



Module Contents
---------------

.. py:class:: SecretsManagerHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for the Google Secret Manager API.

   See https://cloud.google.com/secret-manager

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

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

   
   .. method:: get_conn(self)

      Retrieves the connection to Secret Manager.

      :return: Secret Manager client.
      :rtype: airflow.providers.google.cloud._internal_client.secret_manager_client._SecretManagerClient



   
   .. method:: get_secret(self, secret_id: str, secret_version: str = 'latest', project_id: Optional[str] = None)

      Get secret value from the Secret Manager.

      :param secret_id: Secret Key
      :type secret_id: str
      :param secret_version: version of the secret (default is 'latest')
      :type secret_version: str
      :param project_id: Project id (if you want to override the project_id from credentials)
      :type project_id: str




