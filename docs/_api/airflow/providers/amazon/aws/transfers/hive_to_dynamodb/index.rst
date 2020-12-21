:mod:`airflow.providers.amazon.aws.transfers.hive_to_dynamodb`
==============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.hive_to_dynamodb

.. autoapi-nested-parse::

   This module contains operator to move data from Hive to DynamoDB.



Module Contents
---------------

.. py:class:: HiveToDynamoDBOperator(*, sql: str, table_name: str, table_keys: list, pre_process: Optional[Callable] = None, pre_process_args: Optional[list] = None, pre_process_kwargs: Optional[list] = None, region_name: Optional[str] = None, schema: str = 'default', hiveserver2_conn_id: str = 'hiveserver2_default', aws_conn_id: str = 'aws_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Hive to DynamoDB, note that for now the data is loaded
   into memory before being pushed to DynamoDB, so this operator should
   be used for smallish amount of data.

   :param sql: SQL query to execute against the hive database. (templated)
   :type sql: str
   :param table_name: target DynamoDB table
   :type table_name: str
   :param table_keys: partition key and sort key
   :type table_keys: list
   :param pre_process: implement pre-processing of source data
   :type pre_process: function
   :param pre_process_args: list of pre_process function arguments
   :type pre_process_args: list
   :param pre_process_kwargs: dict of pre_process function arguments
   :type pre_process_kwargs: dict
   :param region_name: aws region name (example: us-east-1)
   :type region_name: str
   :param schema: hive database schema
   :type schema: str
   :param hiveserver2_conn_id: source hive connection
   :type hiveserver2_conn_id: str
   :param aws_conn_id: aws connection
   :type aws_conn_id: str

   .. attribute:: template_fields
      :annotation: = ['sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. method:: execute(self, context)




