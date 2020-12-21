:mod:`airflow.providers.google.cloud.secrets.secret_manager`
============================================================

.. py:module:: airflow.providers.google.cloud.secrets.secret_manager

.. autoapi-nested-parse::

   Objects relating to sourcing connections from Google Cloud Secrets Manager



Module Contents
---------------

.. data:: SECRET_ID_PATTERN
   :annotation: = ^[a-zA-Z0-9-_]*$

   

.. py:class:: CloudSecretManagerBackend(connections_prefix: str = 'airflow-connections', variables_prefix: str = 'airflow-variables', config_prefix: str = 'airflow-config', gcp_keyfile_dict: Optional[dict] = None, gcp_key_path: Optional[str] = None, gcp_scopes: Optional[str] = None, project_id: Optional[str] = None, sep: str = '-', **kwargs)

   Bases: :class:`airflow.secrets.BaseSecretsBackend`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connection object from Google Cloud Secrets Manager

   Configurable via ``airflow.cfg`` as follows:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
       backend_kwargs = {"connections_prefix": "airflow-connections", "sep": "-"}

   For example, if the Secrets Manager secret id is ``airflow-connections-smtp_default``, this would be
   accessible if you provide ``{"connections_prefix": "airflow-connections", "sep": "-"}`` and request
   conn_id ``smtp_default``.

   If the Secrets Manager secret id is ``airflow-variables-hello``, this would be
   accessible if you provide ``{"variables_prefix": "airflow-variables", "sep": "-"}`` and request
   Variable Key ``hello``.

   The full secret id should follow the pattern "[a-zA-Z0-9-_]".

   :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
   :type connections_prefix: str
   :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
   :type variables_prefix: str
   :param config_prefix: Specifies the prefix of the secret to read to get Airflow Configurations
       containing secrets.
   :type config_prefix: str
   :param gcp_key_path: Path to Google Cloud Service Account key file (JSON). Mutually exclusive with
       gcp_keyfile_dict. use default credentials in the current environment if not provided.
   :type gcp_key_path: str
   :param gcp_keyfile_dict: Dictionary of keyfile parameters. Mutually exclusive with gcp_key_path.
   :type gcp_keyfile_dict: dict
   :param gcp_scopes: Comma-separated string containing OAuth2 scopes
   :type gcp_scopes: str
   :param project_id: Project ID to read the secrets from. If not passed, the project ID from credentials
       will be used.
   :type project_id: str
   :param sep: Separator used to concatenate connections_prefix and conn_id. Default: "-"
   :type sep: str

   
   .. method:: client(self)

      Cached property returning secret client.

      :return: Secrets client



   
   .. method:: _is_valid_prefix_and_sep(self)



   
   .. method:: get_conn_uri(self, conn_id: str)

      Get secret value from the SecretManager.

      :param conn_id: connection id
      :type conn_id: str



   
   .. method:: get_variable(self, key: str)

      Get Airflow Variable from Environment Variable

      :param key: Variable Key
      :return: Variable Value



   
   .. method:: get_config(self, key: str)

      Get Airflow Configuration

      :param key: Configuration Option Key
      :return: Configuration Option Value



   
   .. method:: _get_secret(self, path_prefix: str, secret_id: str)

      Get secret value from the SecretManager based on prefix.

      :param path_prefix: Prefix for the Path to get Secret
      :type path_prefix: str
      :param secret_id: Secret Key
      :type secret_id: str




