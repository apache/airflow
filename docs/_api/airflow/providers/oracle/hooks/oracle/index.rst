:mod:`airflow.providers.oracle.hooks.oracle`
============================================

.. py:module:: airflow.providers.oracle.hooks.oracle


Module Contents
---------------

.. py:class:: OracleHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Oracle SQL.

   .. attribute:: conn_name_attr
      :annotation: = oracle_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = oracle_default

      

   .. attribute:: supports_autocommit
      :annotation: = False

      

   
   .. method:: get_conn(self)

      Returns a oracle connection object
      Optional parameters for using a custom DSN connection
      (instead of using a server alias from tnsnames.ora)
      The dsn (data source name) is the TNS entry
      (from the Oracle names server or tnsnames.ora file)
      or is a string like the one returned from makedsn().

      :param dsn: the host address for the Oracle server
      :param service_name: the db_unique_name of the database
            that you are connecting to (CONNECT_DATA part of TNS)

      You can set these parameters in the extra fields of your connection
      as in ``{ "dsn":"some.host.address" , "service_name":"some.service.name" }``
      see more param detail in
      `cx_Oracle.connect <https://cx-oracle.readthedocs.io/en/latest/module.html#cx_Oracle.connect>`_



   
   .. method:: insert_rows(self, table: str, rows: List[tuple], target_fields=None, commit_every: int = 1000, replace: Optional[bool] = False, **kwargs)

      A generic way to insert a set of tuples into a table,
      the whole set of inserts is treated as one transaction
      Changes from standard DbApiHook implementation:

      - Oracle SQL queries in cx_Oracle can not be terminated with a semicolon (`;`)
      - Replace NaN values with NULL using `numpy.nan_to_num` (not using
        `is_nan()` because of input types error for strings)
      - Coerce datetime cells to Oracle DATETIME format during insert

      :param table: target Oracle table, use dot notation to target a
          specific database
      :type table: str
      :param rows: the rows to insert into the table
      :type rows: iterable of tuples
      :param target_fields: the names of the columns to fill in the table
      :type target_fields: iterable of str
      :param commit_every: the maximum number of rows to insert in one transaction
          Default 1000, Set greater than 0.
          Set 1 to insert each row in each single transaction
      :type commit_every: int
      :param replace: Whether to replace instead of insert
      :type replace: bool



   
   .. method:: bulk_insert_rows(self, table: str, rows: List[tuple], target_fields: Optional[List[str]] = None, commit_every: int = 5000)

      A performant bulk insert for cx_Oracle
      that uses prepared statements via `executemany()`.
      For best performance, pass in `rows` as an iterator.

      :param table: target Oracle table, use dot notation to target a
          specific database
      :type table: str
      :param rows: the rows to insert into the table
      :type rows: iterable of tuples
      :param target_fields: the names of the columns to fill in the table, default None.
          If None, each rows should have some order as table columns name
      :type target_fields: iterable of str Or None
      :param commit_every: the maximum number of rows to insert in one transaction
          Default 5000. Set greater than 0. Set 1 to insert each row in each transaction
      :type commit_every: int




