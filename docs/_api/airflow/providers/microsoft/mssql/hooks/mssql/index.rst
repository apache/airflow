:mod:`airflow.providers.microsoft.mssql.hooks.mssql`
====================================================

.. py:module:: airflow.providers.microsoft.mssql.hooks.mssql

.. autoapi-nested-parse::

   Microsoft SQLServer hook module



Module Contents
---------------

.. py:class:: MsSqlHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Microsoft SQL Server.

   .. attribute:: conn_name_attr
      :annotation: = mssql_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = mssql_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: get_conn(self)

      Returns a mssql connection object



   
   .. method:: set_autocommit(self, conn: pymssql.connect, autocommit: bool)



   
   .. method:: get_autocommit(self, conn: pymssql.connect)




