:mod:`airflow.providers.presto.hooks.presto`
============================================

.. py:module:: airflow.providers.presto.hooks.presto


Module Contents
---------------

.. py:exception:: PrestoException

   Bases: :class:`Exception`

   Presto exception


.. function:: _boolify(value)

.. py:class:: PrestoHook

   Bases: :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with Presto through prestodb.

   >>> ph = PrestoHook()
   >>> sql = "SELECT count(1) AS num FROM airflow.static_babynames"
   >>> ph.get_records(sql)
   [[340698]]

   .. attribute:: conn_name_attr
      :annotation: = presto_conn_id

      

   .. attribute:: default_conn_name
      :annotation: = presto_default

      

   
   .. method:: get_conn(self)

      Returns a connection object



   
   .. method:: get_isolation_level(self)

      Returns an isolation level



   
   .. staticmethod:: _strip_sql(sql: str)



   
   .. method:: get_records(self, hql, parameters: Optional[dict] = None)

      Get a set of records from Presto



   
   .. method:: get_first(self, hql: str, parameters: Optional[dict] = None)

      Returns only the first row, regardless of how many rows the query returns.



   
   .. method:: get_pandas_df(self, hql, parameters=None, **kwargs)

      Get a pandas dataframe from a sql query.



   
   .. method:: run(self, hql, autocommit: bool = False, parameters: Optional[dict] = None)

      Execute the statement against Presto. Can be used to create views.



   
   .. method:: insert_rows(self, table: str, rows: Iterable[tuple], target_fields: Optional[Iterable[str]] = None, commit_every: int = 0, replace: bool = False, **kwargs)

      A generic way to insert a set of tuples into a table.

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




