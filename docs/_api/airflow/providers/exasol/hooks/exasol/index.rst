:mod:`airflow.providers.exasol.hooks.exasol`
============================================

.. py:module:: airflow.providers.exasol.hooks.exasol


Module Contents
---------------

.. py:class:: ExasolHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Exasol.
   You can specify the pyexasol ``compression``, ``encryption``, ``json_lib``
   and ``client_name``  parameters in the extra field of your connection
   as ``{"compression": True, "json_lib": "rapidjson", etc}``.
   See `pyexasol reference
   <https://github.com/badoo/pyexasol/blob/master/docs/REFERENCE.md#connect>`_
   for more details.

   .. attribute:: conn_name_attr
      :annotation: = exasol_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = exasol_default

      

   .. attribute:: supports_autocommit
      :annotation: = True

      

   
   .. method:: get_conn(self)



   
   .. method:: get_pandas_df(self, sql: Union[str, list], parameters: Optional[dict] = None, **kwargs)

      Executes the sql and returns a pandas dataframe

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable
      :param kwargs: (optional) passed into pyexasol.ExaConnection.export_to_pandas method
      :type kwargs: dict



   
   .. method:: get_records(self, sql: Union[str, list], parameters: Optional[dict] = None)

      Executes the sql and returns a set of records.

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: get_first(self, sql: Union[str, list], parameters: Optional[dict] = None)

      Executes the sql and returns the first resulting row.

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: run(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None)

      Runs a command or a list of commands. Pass a list of sql
      statements to the sql parameter to get them to execute
      sequentially

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param autocommit: What to set the connection's autocommit setting to
          before executing the query.
      :type autocommit: bool
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: set_autocommit(self, conn, autocommit: bool)

      Sets the autocommit flag on the connection

      :param conn: Connection to set autocommit setting to.
      :type conn: connection object
      :param autocommit: The autocommit setting to set.
      :type autocommit: bool



   
   .. method:: get_autocommit(self, conn)

      Get autocommit setting for the provided connection.
      Return True if autocommit is set.
      Return False if autocommit is not set or set to False or conn
      does not support autocommit.

      :param conn: Connection to get autocommit setting from.
      :type conn: connection object
      :return: connection autocommit setting.
      :rtype: bool



   
   .. staticmethod:: _serialize_cell(cell, conn=None)

      Exasol will adapt all arguments to the execute() method internally,
      hence we return cell without any conversion.

      :param cell: The cell to insert into the table
      :type cell: object
      :param conn: The database connection
      :type conn: connection object
      :return: The cell
      :rtype: object




