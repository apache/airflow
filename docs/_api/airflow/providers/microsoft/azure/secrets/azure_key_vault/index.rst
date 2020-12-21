:mod:`airflow.providers.microsoft.azure.secrets.azure_key_vault`
================================================================

.. py:module:: airflow.providers.microsoft.azure.secrets.azure_key_vault


Module Contents
---------------

.. py:class:: AzureKeyVaultBackend(connections_prefix: str = 'airflow-connections', variables_prefix: str = 'airflow-variables', config_prefix: str = 'airflow-config', vault_url: str = '', sep: str = '-', **kwargs)

   Bases: :class:`airflow.secrets.BaseSecretsBackend`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Airflow Connections or Variables from Azure Key Vault secrets.

   The Azure Key Vault can be configured as a secrets backend in the ``airflow.cfg``:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend
       backend_kwargs = {"connections_prefix": "airflow-connections", "vault_url": "<azure_key_vault_uri>"}

   For example, if the secrets prefix is ``airflow-connections-smtp-default``, this would be accessible
   if you provide ``{"connections_prefix": "airflow-connections"}`` and request conn_id ``smtp-default``.
   And if variables prefix is ``airflow-variables-hello``, this would be accessible
   if you provide ``{"variables_prefix": "airflow-variables"}`` and request variable key ``hello``.

   :param connections_prefix: Specifies the prefix of the secret to read to get Connections
       If set to None (null), requests for connections will not be sent to Azure Key Vault
   :type connections_prefix: str
   :param variables_prefix: Specifies the prefix of the secret to read to get Variables
       If set to None (null), requests for variables will not be sent to Azure Key Vault
   :type variables_prefix: str
   :param config_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for configurations will not be sent to Azure Key Vault
   :type config_prefix: str
   :param vault_url: The URL of an Azure Key Vault to use
   :type vault_url: str
   :param sep: separator used to concatenate secret_prefix and secret_id. Default: "-"
   :type sep: str

   
   .. method:: client(self)

      Create a Azure Key Vault client.



   
   .. method:: get_conn_uri(self, conn_id: str)

      Get an Airflow Connection URI from an Azure Key Vault secret

      :param conn_id: The Airflow connection id to retrieve
      :type conn_id: str



   
   .. method:: get_variable(self, key: str)

      Get an Airflow Variable from an Azure Key Vault secret.

      :param key: Variable Key
      :type key: str
      :return: Variable Value



   
   .. method:: get_config(self, key: str)

      Get Airflow Configuration

      :param key: Configuration Option Key
      :return: Configuration Option Value



   
   .. staticmethod:: build_path(path_prefix: str, secret_id: str, sep: str = '-')

      Given a path_prefix and secret_id, build a valid secret name for the Azure Key Vault Backend.
      Also replaces underscore in the path with dashes to support easy switching between
      environment variables, so ``connection_default`` becomes ``connection-default``.

      :param path_prefix: The path prefix of the secret to retrieve
      :type path_prefix: str
      :param secret_id: Name of the secret
      :type secret_id: str
      :param sep: Separator used to concatenate path_prefix and secret_id
      :type sep: str



   
   .. method:: _get_secret(self, path_prefix: str, secret_id: str)

      Get an Azure Key Vault secret value

      :param path_prefix: Prefix for the Path to get Secret
      :type path_prefix: str
      :param secret_id: Secret Key
      :type secret_id: str




