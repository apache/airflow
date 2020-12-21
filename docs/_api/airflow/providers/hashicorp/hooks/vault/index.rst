:mod:`airflow.providers.hashicorp.hooks.vault`
==============================================

.. py:module:: airflow.providers.hashicorp.hooks.vault

.. autoapi-nested-parse::

   Hook for HashiCorp Vault



Module Contents
---------------

.. py:class:: VaultHook(vault_conn_id: str, auth_type: Optional[str] = None, auth_mount_point: Optional[str] = None, kv_engine_version: Optional[int] = None, role_id: Optional[str] = None, kubernetes_role: Optional[str] = None, kubernetes_jwt_path: Optional[str] = None, token_path: Optional[str] = None, gcp_key_path: Optional[str] = None, gcp_scopes: Optional[str] = None, azure_tenant_id: Optional[str] = None, azure_resource: Optional[str] = None, radius_host: Optional[str] = None, radius_port: Optional[int] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Hook to Interact with HashiCorp Vault KeyValue Secret engine.

   HashiCorp hvac documentation:
      * https://hvac.readthedocs.io/en/stable/

   You connect to the host specified as host in the connection. The login/password from the connection
   are used as credentials usually and you can specify different authentication parameters
   via init params or via corresponding extras in the connection.

   The mount point should be placed as a path in the URL - similarly to Vault's URL schema:
   This indicates the "path" the secret engine is mounted on. Default id not specified is "secret".
   Note that this ``mount_point`` is not used for authentication if authentication is done via a
   different engines. Each engine uses it's own engine-specific authentication mount_point.

   The extras in the connection are named the same as the parameters ('kv_engine_version', 'auth_type', ...).

   You can also use gcp_keyfile_dict extra to pass json-formatted dict in case of 'gcp' authentication.

   The URL schemas supported are "vault", "http" (using http to connect to the vault) or
   "vaults" and "https" (using https to connect to the vault).

   Example URL:

   .. code-block::

       vault://user:password@host:port/mount_point?kv_engine_version=1&auth_type=github


   Login/Password are used as credentials:

       * approle: password -> secret_id
       * github: password -> token
       * token: password -> token
       * aws_iam: login -> key_id, password -> secret_id
       * azure: login -> client_id, password -> client_secret
       * ldap: login -> username,   password -> password
       * userpass: login -> username, password -> password
       * radius: password -> radius_secret

   :param vault_conn_id: The id of the connection to use
   :type vault_conn_id: str
   :param auth_type: Authentication Type for the Vault. Default is ``token``. Available values are:
       ('approle', 'github', 'gcp', 'kubernetes', 'ldap', 'token', 'userpass')
   :type auth_type: str
   :param auth_mount_point: It can be used to define mount_point for authentication chosen
         Default depends on the authentication method used.
   :type auth_mount_point: str
   :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``). Defaults to
         version defined in connection or ``2`` if not defined in connection.
   :type kv_engine_version: int
   :param role_id: Role ID for Authentication (for ``approle``, ``aws_iam`` auth_types)
   :type role_id: str
   :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type)
   :type kubernetes_role: str
   :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
       ``/var/run/secrets/kubernetes.io/serviceaccount/token``)
   :type kubernetes_jwt_path: str
   :param token_path: path to file containing authentication token to include in requests sent to Vault
       (for ``token`` and ``github`` auth_type).
   :type token_path: str
   :param gcp_key_path: Path to Google Cloud Service Account key file (JSON) (for ``gcp`` auth_type)
          Mutually exclusive with gcp_keyfile_dict
   :type gcp_key_path: str
   :param gcp_scopes: Comma-separated string containing OAuth2  scopes (for ``gcp`` auth_type)
   :type gcp_scopes: str
   :param azure_tenant_id: The tenant id for the Azure Active Directory (for ``azure`` auth_type)
   :type azure_tenant_id: str
   :param azure_resource: The configured URL for the application registered in Azure Active Directory
          (for ``azure`` auth_type)
   :type azure_resource: str
   :param radius_host: Host for radius (for ``radius`` auth_type)
   :type radius_host: str
   :param radius_port: Port for radius (for ``radius`` auth_type)
   :type radius_port: int

   
   .. method:: _get_kubernetes_parameters_from_connection(self, kubernetes_jwt_path: Optional[str], kubernetes_role: Optional[str])



   
   .. method:: _get_gcp_parameters_from_connection(self, gcp_key_path: Optional[str], gcp_scopes: Optional[str])



   
   .. method:: _get_azure_parameters_from_connection(self, azure_resource: Optional[str], azure_tenant_id: Optional[str])



   
   .. method:: _get_radius_parameters_from_connection(self, radius_host: Optional[str], radius_port: Optional[int])



   
   .. method:: get_conn(self)

      Retrieves connection to Vault.

      :rtype: hvac.Client
      :return: connection used.



   
   .. method:: get_secret(self, secret_path: str, secret_version: Optional[int] = None)

      Get secret value from the engine.

      :param secret_path: Path of the secret
      :type secret_path: str
      :param secret_version: Optional version of key to read - can only be used in case of version 2 of KV
      :type secret_version: int

      See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
      and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

      :param secret_path: Path of the secret
      :type secret_path: str
      :rtype: dict
      :return: secret stored in the vault as a dictionary



   
   .. method:: get_secret_metadata(self, secret_path: str)

      Reads secret metadata (including versions) from the engine. It is only valid for KV version 2.

      :param secret_path: Path to read from
      :type secret_path: str
      :rtype: dict
      :return: secret metadata. This is a Dict containing metadata for the secret.

      See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.



   
   .. method:: get_secret_including_metadata(self, secret_path: str, secret_version: Optional[int] = None)

      Reads secret including metadata. It is only valid for KV version 2.

      See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

      :param secret_path: Path of the secret
      :type secret_path: str
      :param secret_version: Optional version of key to read - can only be used in case of version 2 of KV
      :type secret_version: int
      :rtype: dict
      :return: key info. This is a Dict with "data" mapping keeping secret
          and "metadata" mapping keeping metadata of the secret.



   
   .. method:: create_or_update_secret(self, secret_path: str, secret: dict, method: Optional[str] = None, cas: Optional[int] = None)

      Creates or updates secret.

      :param secret_path: Path to read from
      :type secret_path: str
      :param secret: Secret to create or update for the path specified
      :type secret: dict
      :param method: Optional parameter to explicitly request a POST (create) or PUT (update) request to
          the selected kv secret engine. If no argument is provided for this parameter, hvac attempts to
          intelligently determine which method is appropriate. Only valid for KV engine version 1
      :type method: str
      :param cas: Set the "cas" value to use a Check-And-Set operation. If not set the write will be
          allowed. If set to 0 a write will only be allowed if the key doesn't exist.
          If the index is non-zero the write will only be allowed if the key's current version
          matches the version specified in the cas parameter. Only valid for KV engine version 2.
      :type cas: int
      :rtype: requests.Response
      :return: The response of the create_or_update_secret request.

      See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
      and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.




