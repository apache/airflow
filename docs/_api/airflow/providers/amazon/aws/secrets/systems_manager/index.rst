:mod:`airflow.providers.amazon.aws.secrets.systems_manager`
===========================================================

.. py:module:: airflow.providers.amazon.aws.secrets.systems_manager

.. autoapi-nested-parse::

   Objects relating to sourcing connections from AWS SSM Parameter Store



Module Contents
---------------

.. py:class:: SystemsManagerParameterStoreBackend(connections_prefix: str = '/airflow/connections', variables_prefix: str = '/airflow/variables', config_prefix: str = '/airflow/config', profile_name: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.secrets.BaseSecretsBackend`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Retrieves Connection or Variables from AWS SSM Parameter Store

   Configurable via ``airflow.cfg`` like so:

   .. code-block:: ini

       [secrets]
       backend = airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend
       backend_kwargs = {"connections_prefix": "/airflow/connections", "profile_name": null}

   For example, if ssm path is ``/airflow/connections/smtp_default``, this would be accessible
   if you provide ``{"connections_prefix": "/airflow/connections"}`` and request conn_id ``smtp_default``.
   And if ssm path is ``/airflow/variables/hello``, this would be accessible
   if you provide ``{"variables_prefix": "/airflow/variables"}`` and request conn_id ``hello``.

   :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
       If set to None (null), requests for connections will not be sent to AWS SSM Parameter Store.
   :type connections_prefix: str
   :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for variables will not be sent to AWS SSM Parameter Store.
   :type variables_prefix: str
   :param config_prefix: Specifies the prefix of the secret to read to get Variables.
       If set to None (null), requests for configurations will not be sent to AWS SSM Parameter Store.
   :type config_prefix: str
   :param profile_name: The name of a profile to use. If not given, then the default profile is used.
   :type profile_name: str

   
   .. method:: client(self)

      Create a SSM client



   
   .. method:: get_conn_uri(self, conn_id: str)

      Get param value

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

      Get secret value from Parameter Store.

      :param path_prefix: Prefix for the Path to get Secret
      :type path_prefix: str
      :param secret_id: Secret Key
      :type secret_id: str




