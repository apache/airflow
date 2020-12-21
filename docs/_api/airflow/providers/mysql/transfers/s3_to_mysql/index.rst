:mod:`airflow.providers.mysql.transfers.s3_to_mysql`
====================================================

.. py:module:: airflow.providers.mysql.transfers.s3_to_mysql


Module Contents
---------------

.. py:class:: S3ToMySqlOperator(*, s3_source_key: str, mysql_table: str, mysql_duplicate_key_handling: str = 'IGNORE', mysql_extra_options: Optional[str] = None, aws_conn_id: str = 'aws_default', mysql_conn_id: str = 'mysql_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Loads a file from S3 into a MySQL table.

   :param s3_source_key: The path to the file (S3 key) that will be loaded into MySQL.
   :type s3_source_key: str
   :param mysql_table: The MySQL table into where the data will be sent.
   :type mysql_table: str
   :param mysql_duplicate_key_handling: Specify what should happen to duplicate data.
       You can choose either `IGNORE` or `REPLACE`.

       .. seealso::
           https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-duplicate-key-handling
   :type mysql_duplicate_key_handling: str
   :param mysql_extra_options: MySQL options to specify exactly how to load the data.
   :type mysql_extra_options: Optional[str]
   :param aws_conn_id: The S3 connection that contains the credentials to the S3 Bucket.
   :type aws_conn_id: str
   :param mysql_conn_id: The MySQL connection that contains the credentials to the MySQL data base.
   :type mysql_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['s3_source_key', 'mysql_table']

      

   .. attribute:: template_ext
      :annotation: = []

      

   .. attribute:: ui_color
      :annotation: = #f4a460

      

   
   .. method:: execute(self, context: Dict)

      Executes the transfer operation from S3 to MySQL.

      :param context: The context that is being provided when executing.
      :type context: dict




