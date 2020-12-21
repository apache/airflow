:mod:`airflow.providers.mysql.transfers.vertica_to_mysql`
=========================================================

.. py:module:: airflow.providers.mysql.transfers.vertica_to_mysql


Module Contents
---------------

.. py:class:: VerticaToMySqlOperator(sql: str, mysql_table: str, vertica_conn_id: str = 'vertica_default', mysql_conn_id: str = 'mysql_default', mysql_preoperator: Optional[str] = None, mysql_postoperator: Optional[str] = None, bulk_load: bool = False, *args, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Vertica to MySQL.

   :param sql: SQL query to execute against the Vertica database. (templated)
   :type sql: str
   :param vertica_conn_id: source Vertica connection
   :type vertica_conn_id: str
   :param mysql_table: target MySQL table, use dot notation to target a
       specific database. (templated)
   :type mysql_table: str
   :param mysql_conn_id: source mysql connection
   :type mysql_conn_id: str
   :param mysql_preoperator: sql statement to run against MySQL prior to
       import, typically use to truncate of delete in place of the data
       coming in, allowing the task to be idempotent (running the task
       twice won't double load data). (templated)
   :type mysql_preoperator: str
   :param mysql_postoperator: sql statement to run against MySQL after the
       import, typically used to move data from staging to production
       and issue cleanup commands. (templated)
   :type mysql_postoperator: str
   :param bulk_load: flag to use bulk_load option.  This loads MySQL directly
       from a tab-delimited text file using the LOAD DATA LOCAL INFILE command.
       This option requires an extra connection parameter for the
       destination MySQL connection: {'local_infile': true}.
   :type bulk_load: bool

   .. attribute:: template_fields
      :annotation: = ['sql', 'mysql_table', 'mysql_preoperator', 'mysql_postoperator']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. method:: execute(self, context)




