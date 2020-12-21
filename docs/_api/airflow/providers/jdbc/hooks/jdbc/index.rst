:mod:`airflow.providers.jdbc.hooks.jdbc`
========================================

.. py:module:: airflow.providers.jdbc.hooks.jdbc


Module Contents
---------------

.. py:class:: JdbcHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   General hook for jdbc db access.

   JDBC URL, username and password will be taken from the predefined connection.
   Note that the whole JDBC URL must be specified in the "host" field in the DB.
   Raises an airflow error if the given connection id doesn't exist.

   .. attribute:: conn_name_attr
      :annotation: = jdbc_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = jdbc_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: get_conn(self)



   
   .. method:: set_autocommit(self, conn: jaydebeapi.Connection, autocommit: bool)

      Enable or disable autocommit for the given connection.

      :param conn: The connection.
      :type conn: connection object
      :param autocommit: The connection's autocommit setting.
      :type autocommit: bool



   
   .. method:: get_autocommit(self, conn: jaydebeapi.Connection)

      Get autocommit setting for the provided connection.
      Return True if conn.autocommit is set to True.
      Return False if conn.autocommit is not set or set to False

      :param conn: The connection.
      :type conn: connection object
      :return: connection autocommit setting.
      :rtype: bool




