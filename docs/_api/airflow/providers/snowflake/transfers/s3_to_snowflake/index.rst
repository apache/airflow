:mod:`airflow.providers.snowflake.transfers.s3_to_snowflake`
============================================================

.. py:module:: airflow.providers.snowflake.transfers.s3_to_snowflake

.. autoapi-nested-parse::

   This module contains AWS S3 to Snowflake operator.



Module Contents
---------------

.. py:class:: S3ToSnowflakeOperator(*, s3_keys: list, table: str, stage: Any, file_format: str, schema: str, columns_array: Optional[list] = None, autocommit: bool = True, snowflake_conn_id: str = 'snowflake_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes an COPY command to load files from s3 to Snowflake

   :param s3_keys: reference to a list of S3 keys
   :type s3_keys: list
   :param table: reference to a specific table in snowflake database
   :type table: str
   :param s3_bucket: reference to a specific S3 bucket
   :type s3_bucket: str
   :param file_format: reference to a specific file format
   :type file_format: str
   :param schema: reference to a specific schema in snowflake database
   :type schema: str
   :param columns_array: reference to a specific columns array in snowflake database
   :type columns_array: list
   :param snowflake_conn_id: reference to a specific snowflake database
   :type snowflake_conn_id: str

   
   .. method:: execute(self, context: Any)




