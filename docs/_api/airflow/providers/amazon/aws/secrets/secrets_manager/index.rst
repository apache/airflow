:mod:`airflow.providers.amazon.aws.secrets.secrets_manager`
===========================================================

.. py:module:: airflow.providers.amazon.aws.secrets.secrets_manager

.. autoapi-nested-parse::

   Objects relating to sourcing secrets from AWS Secrets Manager



Module Contents
---------------

.. py:class:: SecretsManagerBackend(connections_prefix: str = 'airflow/connections', variables_prefix: str = 'airflow/variables', config_prefix: str = 'airflow/config', profile_name: Optional[str] = None, sep: str = '/', **kwargs)

   Bases: :class:`airflow.secrets.BaseSecretsBackend`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connection or Variables from AWS Secrets Manager

   Configurable via ``airflow.cfg`` like so:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
       backend_kwargs = {"connections_prefix": "airflow/connections"}

   For example, if secrets prefix is ``airflow/connections/smtp_default``, this would be accessible
   if you provide ``{"connections_prefix": "airflow/connections"}`` and request conn_id ``smtp_default``.
   If variables prefix is ``airflow/variables/hello``, this would be accessible
   if you provide ``{"variables_prefix": "airflow/variables"}`` and request variable key ``hello``.
   And if config_prefix is ``airflow/config/sql_alchemy_conn``, this would be accessible
   if you provide ``{"config_prefix": "airflow/config"}`` and request config
   key ``sql_alchemy_conn``.

   You can also pass additional keyword arguments like ``aws_secret_access_key``, ``aws_access_key_id``
   or ``region_name`` to this class and they would be passed on to Boto3 client.

   :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
       If set to None (null), requests for connections will not be sent to AWS Secrets Manager
   :type connections_prefix: str
   :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for variables will not be sent to AWS Secrets Manager
   :type variables_prefix: str
   :param config_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for configurations will not be sent to AWS Secrets Manager
   :type config_prefix: str
   :param profile_name: The name of a profile to use. If not given, then the default profile is used.
   :type profile_name: str
   :param sep: separator used to concatenate secret_prefix and secret_id. Default: "/"
   :type sep: str

   
   .. method:: client(self)

      Create a Secrets Manager client



   
   .. method:: get_conn_uri(self, conn_id: str)

      Get Connection Value

      :param conn_id: connection id
      :type conn_id: str



   
   .. method:: get_variable(self, key: str)

      Get Airflow Variable

      :param key: Variable Key
      :return: Variable Value



   
   .. method:: get_config(self, key: str)

      Get Airflow Configuration

      :param key: Configuration Option Key
      :return: Configuration Option Value



   
   .. method:: _get_secret(self, path_prefix: str, secret_id: str)

      Get secret value from Secrets Manager

      :param path_prefix: Prefix for the Path to get Secret
      :type path_prefix: str
      :param secret_id: Secret Key
      :type secret_id: str




