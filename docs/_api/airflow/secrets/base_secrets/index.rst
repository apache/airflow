:mod:`airflow.secrets.base_secrets`
===================================

.. py:module:: airflow.secrets.base_secrets


Module Contents
---------------

.. py:class:: BaseSecretsBackend(**kwargs)

   Bases: :class:`abc.ABC`

   Abstract base class to retrieve secrets given a conn_id and construct a Connection object

   
   .. staticmethod:: build_path(path_prefix: str, secret_id: str, sep: str = '/')

      Given conn_id, build path for Secrets Backend

      :param path_prefix: Prefix of the path to get secret
      :type path_prefix: str
      :param secret_id: Secret id
      :type secret_id: str
      :param sep: separator used to concatenate connections_prefix and conn_id. Default: "/"
      :type sep: str



   
   .. method:: get_conn_uri(self, conn_id: str)

      Get conn_uri from Secrets Backend

      :param conn_id: connection id
      :type conn_id: str



   
   .. method:: get_connections(self, conn_id: str)

      Return connection object with a given ``conn_id``.

      :param conn_id: connection id
      :type conn_id: str



   
   .. method:: get_variable(self, key: str)

      Return value for Airflow Connection

      :param key: Variable Key
      :return: Variable Value



   
   .. method:: get_config(self, key: str)

      Return value for Airflow Config Key

      :param key: Config Key
      :return: Config Value




