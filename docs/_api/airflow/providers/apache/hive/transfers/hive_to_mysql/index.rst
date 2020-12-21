:mod:`airflow.providers.apache.hive.transfers.hive_to_mysql`
============================================================

.. py:module:: airflow.providers.apache.hive.transfers.hive_to_mysql

.. autoapi-nested-parse::

   This module contains operator to move data from Hive to MySQL.



Module Contents
---------------

.. py:class:: HiveToMySqlOperator(*, sql: str, mysql_table: str, hiveserver2_conn_id: str = 'hiveserver2_default', mysql_conn_id: str = 'mysql_default', mysql_preoperator: Optional[str] = None, mysql_postoperator: Optional[str] = None, bulk_load: bool = False, hive_conf: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Hive to MySQL, note that for now the data is loaded
   into memory before being pushed to MySQL, so this operator should
   be used for smallish amount of data.

   :param sql: SQL query to execute against Hive server. (templated)
   :type sql: str
   :param mysql_table: target MySQL table, use dot notation to target a
       specific database. (templated)
   :type mysql_table: str
   :param mysql_conn_id: source mysql connection
   :type mysql_conn_id: str
   :param hiveserver2_conn_id: destination hive connection
   :type hiveserver2_conn_id: str
   :param mysql_preoperator: sql statement to run against mysql prior to
       import, typically use to truncate of delete in place
       of the data coming in, allowing the task to be idempotent (running
       the task twice won't double load data). (templated)
   :type mysql_preoperator: str
   :param mysql_postoperator: sql statement to run against mysql after the
       import, typically used to move data from staging to
       production and issue cleanup commands. (templated)
   :type mysql_postoperator: str
   :param bulk_load: flag to use bulk_load option.  This loads mysql directly
       from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
       This option requires an extra connection parameter for the
       destination MySQL connection: {'local_infile': true}.
   :type bulk_load: bool
   :param hive_conf:
   :type hive_conf: dict

   .. attribute:: template_fields
      :annotation: = ['sql', 'mysql_table', 'mysql_preoperator', 'mysql_postoperator']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. method:: execute(self, context)




