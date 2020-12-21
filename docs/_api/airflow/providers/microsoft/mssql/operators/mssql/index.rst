:mod:`airflow.providers.microsoft.mssql.operators.mssql`
========================================================

.. py:module:: airflow.providers.microsoft.mssql.operators.mssql


Module Contents
---------------

.. py:class:: MsSqlOperator(*, sql: str, mssql_conn_id: str = 'mssql_default', parameters: Optional[Union[Mapping, Iterable]] = None, autocommit: bool = False, database: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes sql code in a specific Microsoft SQL database

   This operator may use one of two hooks, depending on the ``conn_type`` of the connection.

   If conn_type is ``'odbc'``, then :py:class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`
   is used.  Otherwise, :py:class:`~airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook` is used.

   :param sql: the sql code to be executed
   :type sql: str or string pointing to a template file with .sql
       extension. (templated)
   :param mssql_conn_id: reference to a specific mssql database
   :type mssql_conn_id: str
   :param parameters: (optional) the parameters to render the SQL query with.
   :type parameters: dict or iterable
   :param autocommit: if True, each command is automatically committed.
       (default value: False)
   :type autocommit: bool
   :param database: name of database which overwrite defined one in connection
   :type database: str

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #ededed

      

   
   .. method:: get_hook(self)

      Will retrieve hook as determined by Connection.

      If conn_type is ``'odbc'``, will use
      :py:class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`.
      Otherwise, :py:class:`~airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook` will be used.



   
   .. method:: execute(self, context: dict)




