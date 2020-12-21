:mod:`airflow.providers.mysql.transfers.presto_to_mysql`
========================================================

.. py:module:: airflow.providers.mysql.transfers.presto_to_mysql


Module Contents
---------------

.. py:class:: PrestoToMySqlOperator(*, sql: str, mysql_table: str, presto_conn_id: str = 'presto_default', mysql_conn_id: str = 'mysql_default', mysql_preoperator: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Presto to MySQL, note that for now the data is loaded
   into memory before being pushed to MySQL, so this operator should
   be used for smallish amount of data.

   :param sql: SQL query to execute against Presto. (templated)
   :type sql: str
   :param mysql_table: target MySQL table, use dot notation to target a
       specific database. (templated)
   :type mysql_table: str
   :param mysql_conn_id: source mysql connection
   :type mysql_conn_id: str
   :param presto_conn_id: source presto connection
   :type presto_conn_id: str
   :param mysql_preoperator: sql statement to run against mysql prior to
       import, typically use to truncate of delete in place
       of the data coming in, allowing the task to be idempotent (running
       the task twice won't double load data). (templated)
   :type mysql_preoperator: str

   .. attribute:: template_fields
      :annotation: = ['sql', 'mysql_table', 'mysql_preoperator']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. method:: execute(self, context: Dict)




