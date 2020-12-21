:mod:`airflow.providers.oracle.transfers.oracle_to_oracle`
==========================================================

.. py:module:: airflow.providers.oracle.transfers.oracle_to_oracle


Module Contents
---------------

.. py:class:: OracleToOracleOperator(*, oracle_destination_conn_id: str, destination_table: str, oracle_source_conn_id: str, source_sql: str, source_sql_params: Optional[dict] = None, rows_chunk: int = 5000, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Oracle to Oracle.


   :param oracle_destination_conn_id: destination Oracle connection.
   :type oracle_destination_conn_id: str
   :param destination_table: destination table to insert rows.
   :type destination_table: str
   :param oracle_source_conn_id: source Oracle connection.
   :type oracle_source_conn_id: str
   :param source_sql: SQL query to execute against the source Oracle
       database. (templated)
   :type source_sql: str
   :param source_sql_params: Parameters to use in sql query. (templated)
   :type source_sql_params: dict
   :param rows_chunk: number of rows per chunk to commit.
   :type rows_chunk: int

   .. attribute:: template_fields
      :annotation: = ['source_sql', 'source_sql_params']

      

   .. attribute:: ui_color
      :annotation: = #e08c8c

      

   
   .. method:: _execute(self, src_hook, dest_hook, context)



   
   .. method:: execute(self, context)




