:mod:`airflow.providers.apache.hive.transfers.mssql_to_hive`
============================================================

.. py:module:: airflow.providers.apache.hive.transfers.mssql_to_hive

.. autoapi-nested-parse::

   This module contains operator to move data from MSSQL to Hive.



Module Contents
---------------

.. py:class:: MsSqlToHiveOperator(*, sql: str, hive_table: str, create: bool = True, recreate: bool = False, partition: Optional[Dict] = None, delimiter: str = chr(1), mssql_conn_id: str = 'mssql_default', hive_cli_conn_id: str = 'hive_cli_default', tblproperties: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Microsoft SQL Server to Hive. The operator runs
   your query against Microsoft SQL Server, stores the file locally
   before loading it into a Hive table. If the ``create`` or
   ``recreate`` arguments are set to ``True``,
   a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
   Hive data types are inferred from the cursor's metadata.
   Note that the table generated in Hive uses ``STORED AS textfile``
   which isn't the most efficient serialization format. If a
   large amount of data is loaded and/or if the table gets
   queried considerably, you may want to use this operator only to
   stage the data into a temporary table before loading it into its
   final destination using a ``HiveOperator``.

   :param sql: SQL query to execute against the Microsoft SQL Server
       database. (templated)
   :type sql: str
   :param hive_table: target Hive table, use dot notation to target a specific
       database. (templated)
   :type hive_table: str
   :param create: whether to create the table if it doesn't exist
   :type create: bool
   :param recreate: whether to drop and recreate the table at every execution
   :type recreate: bool
   :param partition: target partition as a dict of partition columns and
       values. (templated)
   :type partition: dict
   :param delimiter: field delimiter in the file
   :type delimiter: str
   :param mssql_conn_id: source Microsoft SQL Server connection
   :type mssql_conn_id: str
   :param hive_conn_id: destination hive connection
   :type hive_conn_id: str
   :param tblproperties: TBLPROPERTIES of the hive table being created
   :type tblproperties: dict

   .. attribute:: template_fields
      :annotation: = ['sql', 'partition', 'hive_table']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. classmethod:: type_map(cls, mssql_type: int)

      Maps MsSQL type to Hive type.



   
   .. method:: execute(self, context: Dict[str, str])




