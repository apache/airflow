:mod:`airflow.providers.hashicorp.secrets.vault`
================================================

.. py:module:: airflow.providers.hashicorp.secrets.vault

.. autoapi-nested-parse::

   Objects relating to sourcing connections & variables from Hashicorp Vault



Module Contents
---------------

.. py:class:: VaultBackend(connections_path: str = 'connections', variables_path: str = 'variables', config_path: str = 'config', url: Optional[str] = None, auth_type: str = 'token', auth_mount_point: Optional[str] = None, mount_point: str = 'secret', kv_engine_version: int = 2, token: Optional[str] = None, token_path: Optional[str] = None, username: Optional[str] = None, password: Optional[str] = None, key_id: Optional[str] = None, secret_id: Optional[str] = None, role_id: Optional[str] = None, kubernetes_role: Optional[str] = None, kubernetes_jwt_path: str = '/var/run/secrets/kubernetes.io/serviceaccount/token', gcp_key_path: Optional[str] = None, gcp_keyfile_dict: Optional[dict] = None, gcp_scopes: Optional[str] = None, azure_tenant_id: Optional[str] = None, azure_resource: Optional[str] = None, radius_host: Optional[str] = None, radius_secret: Optional[str] = None, radius_port: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.secrets.BaseSecretsBackend`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connections and Variables from Hashicorp Vault.

   Configurable via ``airflow.cfg`` as follows:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
       backend_kwargs = {
           "connections_path": "connections",
           "url": "http://127.0.0.1:8200",
           "mount_point": "airflow"
           }

   For example, if your keys are under ``connections`` path in ``airflow`` mount_point, this
   would be accessible if you provide ``{"connections_path": "connections"}`` and request
   conn_id ``smtp_default``.

   :param connections_path: Specifies the path of the secret to read to get Connections.
       (default: 'connections'). If set to None (null), requests for connections will not be sent to Vault.
   :type connections_path: str
   :param variables_path: Specifies the path of the secret to read to get Variable.
       (default: 'variables'). If set to None (null), requests for variables will not be sent to Vault.
   :type variables_path: str
   :param config_path: Specifies the path of the secret to read Airflow Configurations
       (default: 'configs'). If set to None (null), requests for configurations will not be sent to Vault.
   :type config_path: str
   :param url: Base URL for the Vault instance being addressed.
   :type url: str
   :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are:
       ('approle', 'aws_iam', 'azure', 'github', 'gcp', 'kubernetes', 'ldap', 'radius', 'token', 'userpass')
   :type auth_type: str
   :param auth_mount_point: It can be used to define mount_point for authentication chosen
         Default depends on the authentication method used.
   :type auth_mount_point: str
   :param mount_point: The "path" the secret engine was mounted on. Default is "secret". Note that
        this mount_point is not used for authentication if authentication is done via a
        different engine. For authentication mount_points see, auth_mount_point.
   :type mount_point: str
   :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``, default: ``2``).
   :type kv_engine_version: int
   :param token: Authentication token to include in requests sent to Vault.
       (for ``token`` and ``github`` auth_type)
   :type token: str
   :param token_path: path to file containing authentication token to include in requests sent to Vault
       (for ``token`` and ``github`` auth_type).
   :type token_path: str
   :param username: Username for Authentication (for ``ldap`` and ``userpass`` auth_type).
   :type username: str
   :param password: Password for Authentication (for ``ldap`` and ``userpass`` auth_type).
   :type password: str
   :param key_id: Key ID for Authentication (for ``aws_iam`` and ''azure`` auth_type).
   :type key_id: str
   :param secret_id: Secret ID for Authentication (for ``approle``, ``aws_iam`` and ``azure`` auth_types).
   :type secret_id: str
   :param role_id: Role ID for Authentication (for ``approle``, ``aws_iam`` auth_types).
   :type role_id: str
   :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type).
   :type kubernetes_role: str
   :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
       ``/var/run/secrets/kubernetes.io/serviceaccount/token``).
   :type kubernetes_jwt_path: str
   :param gcp_key_path: Path to Google Cloud Service Account key file (JSON) (for ``gcp`` auth_type).
          Mutually exclusive with gcp_keyfile_dict.
   :type gcp_key_path: str
   :param gcp_keyfile_dict: Dictionary of keyfile parameters. (for ``gcp`` auth_type).
          Mutually exclusive with gcp_key_path.
   :type gcp_keyfile_dict: dict
   :param gcp_scopes: Comma-separated string containing OAuth2 scopes (for ``gcp`` auth_type).
   :type gcp_scopes: str
   :param azure_tenant_id: The tenant id for the Azure Active Directory (for ``azure`` auth_type).
   :type azure_tenant_id: str
   :param azure_resource: The configured URL for the application registered in Azure Active Directory
          (for ``azure`` auth_type).
   :type azure_resource: str
   :param radius_host: Host for radius (for ``radius`` auth_type).
   :type radius_host: str
   :param radius_secret: Secret for radius (for ``radius`` auth_type).
   :type radius_secret: str
   :param radius_port: Port for radius (for ``radius`` auth_type).
   :type radius_port: str

   
   .. method:: get_conn_uri(self, conn_id: str)

      Get secret value from Vault. Store the secret in the form of URI

      :param conn_id: The connection id
      :type conn_id: str
      :rtype: str
      :return: The connection uri retrieved from the secret



   
   .. method:: get_variable(self, key: str)

      Get Airflow Variable

      :param key: Variable Key
      :type key: str
      :rtype: str
      :return: Variable Value retrieved from the vault



   
   .. method:: get_config(self, key: str)

      Get Airflow Configuration

      :param key: Configuration Option Key
      :type key: str
      :rtype: str
      :return: Configuration Option Value retrieved from the vault




