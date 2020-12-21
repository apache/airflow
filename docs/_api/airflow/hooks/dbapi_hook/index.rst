:mod:`airflow.hooks.dbapi_hook`
===============================

.. py:module:: airflow.hooks.dbapi_hook


Module Contents
---------------

.. py:class:: ConnectorProtocol

   Bases: :class:`airflow.typing_compat.Protocol`

   A protocol where you can connect to a database.

   
   .. method:: connect(self, host: str, port: int, username: str, schema: str)

      Connect to a database.

      :param host: The database host to connect to.
      :param port: The database port to connect to.
      :param username: The database username used for the authentication.
      :param schema: The database schema to connect to.
      :return: the authorized connection object.




.. py:class:: DbApiHook(*args, **kwargs)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Abstract base class for sql hooks.

   .. attribute:: conn_name_attr
      :annotation: :str

      

   .. attribute:: default_conn_name
      :annotation: = default_conn_id

      

   .. attribute:: supports_autocommit
      :annotation: = False

      

   .. attribute:: connector
      :annotation: :Optional[ConnectorProtocol]

      

   
   .. method:: get_conn(self)

      Returns a connection object



   
   .. method:: get_uri(self)

      Extract the URI from the connection.

      :return: the extracted uri.



   
   .. method:: get_sqlalchemy_engine(self, engine_kwargs=None)

      Get an sqlalchemy_engine object.

      :param engine_kwargs: Kwargs used in :func:`~sqlalchemy.create_engine`.
      :return: the created engine.



   
   .. method:: get_pandas_df(self, sql, parameters=None, **kwargs)

      Executes the sql and returns a pandas dataframe

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable
      :param kwargs: (optional) passed into pandas.io.sql.read_sql method
      :type kwargs: dict



   
   .. method:: get_records(self, sql, parameters=None)

      Executes the sql and returns a set of records.

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: get_first(self, sql, parameters=None)

      Executes the sql and returns the first resulting row.

      :param sql: the sql statement to be executed (str) or a list of
          sql statements to execute
      :type sql: str or list
      :param parameters: The parameters to render the SQL query with.
      :type parameters: dict or iterable



   
   .. method:: run(self, sql, autocommit=False, parameters=None)

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



   
   .. method:: set_autocommit(self, conn, autocommit)

      Sets the autocommit flag on the connection



   
   .. method:: get_autocommit(self, conn)

      Get autocommit setting for the provided connection.
      Return True if conn.autocommit is set to True.
      Return False if conn.autocommit is not set or set to False or conn
      does not support autocommit.

      :param conn: Connection to get autocommit setting from.
      :type conn: connection object.
      :return: connection autocommit setting.
      :rtype: bool



   
   .. method:: get_cursor(self)

      Returns a cursor



   
   .. staticmethod:: _generate_insert_sql(table, values, target_fields, replace, **kwargs)

      Static helper method that generate the INSERT SQL statement.
      The REPLACE variant is specific to MySQL syntax.

      :param table: Name of the target table
      :type table: str
      :param values: The row to insert into the table
      :type values: tuple of cell values
      :param target_fields: The names of the columns to fill in the table
      :type target_fields: iterable of strings
      :param replace: Whether to replace instead of insert
      :type replace: bool
      :return: The generated INSERT or REPLACE SQL statement
      :rtype: str



   
   .. method:: insert_rows(self, table, rows, target_fields=None, commit_every=1000, replace=False, **kwargs)

      A generic way to insert a set of tuples into a table,
      a new transaction is created every commit_every rows

      :param table: Name of the target table
      :type table: str
      :param rows: The rows to insert into the table
      :type rows: iterable of tuples
      :param target_fields: The names of the columns to fill in the table
      :type target_fields: iterable of strings
      :param commit_every: The maximum number of rows to insert in one
          transaction. Set to 0 to insert all rows in one transaction.
      :type commit_every: int
      :param replace: Whether to replace instead of insert
      :type replace: bool



   
   .. staticmethod:: _serialize_cell(cell, conn=None)

      Returns the SQL literal of the cell as a string.

      :param cell: The cell to insert into the table
      :type cell: object
      :param conn: The database connection
      :type conn: connection object
      :return: The serialized cell
      :rtype: str



   
   .. method:: bulk_dump(self, table, tmp_file)

      Dumps a database table into a tab-delimited file

      :param table: The name of the source table
      :type table: str
      :param tmp_file: The path of the target file
      :type tmp_file: str



   
   .. method:: bulk_load(self, table, tmp_file)

      Loads a tab-delimited file into a database table

      :param table: The name of the target table
      :type table: str
      :param tmp_file: The path of the file to load into the table
      :type tmp_file: str




