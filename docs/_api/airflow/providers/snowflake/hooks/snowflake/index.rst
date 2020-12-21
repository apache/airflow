:mod:`airflow.providers.snowflake.hooks.snowflake`
==================================================

.. py:module:: airflow.providers.snowflake.hooks.snowflake


Module Contents
---------------

.. py:class:: SnowflakeHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Snowflake.
   get_sqlalchemy_engine() depends on snowflake-sqlalchemy

   .. attribute:: conn_name_attr
      :annotation: = snowflake_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = snowflake_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: _get_conn_params(self)

      One method to fetch connection params as a dict
      used in get_uri() and get_connection()



   
   .. method:: get_uri(self)

      Override DbApiHook get_uri method for get_sqlalchemy_engine()



   
   .. method:: get_conn(self)

      Returns a snowflake.connection object



   
   .. method:: _get_aws_credentials(self)

      Returns aws_access_key_id, aws_secret_access_key
      from extra

      intended to be used by external import and export statements



   
   .. method:: set_autocommit(self, conn, autocommit: Any)



   
   .. method:: get_autocommit(self, conn)




