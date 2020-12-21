:mod:`airflow.providers.microsoft.azure.transfers.oracle_to_azure_data_lake`
============================================================================

.. py:module:: airflow.providers.microsoft.azure.transfers.oracle_to_azure_data_lake


Module Contents
---------------

.. py:class:: OracleToAzureDataLakeOperator(*, filename: str, azure_data_lake_conn_id: str, azure_data_lake_path: str, oracle_conn_id: str, sql: str, sql_params: Optional[dict] = None, delimiter: str = ',', encoding: str = 'utf-8', quotechar: str = '"', quoting: str = csv.QUOTE_MINIMAL, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Moves data from Oracle to Azure Data Lake. The operator runs the query against
   Oracle and stores the file locally before loading it into Azure Data Lake.


   :param filename: file name to be used by the csv file.
   :type filename: str
   :param azure_data_lake_conn_id: destination azure data lake connection.
   :type azure_data_lake_conn_id: str
   :param azure_data_lake_path: destination path in azure data lake to put the file.
   :type azure_data_lake_path: str
   :param oracle_conn_id: source Oracle connection.
   :type oracle_conn_id: str
   :param sql: SQL query to execute against the Oracle database. (templated)
   :type sql: str
   :param sql_params: Parameters to use in sql query. (templated)
   :type sql_params: Optional[dict]
   :param delimiter: field delimiter in the file.
   :type delimiter: str
   :param encoding: encoding type for the file.
   :type encoding: str
   :param quotechar: Character to use in quoting.
   :type quotechar: str
   :param quoting: Quoting strategy. See unicodecsv quoting for more information.
   :type quoting: str

   .. attribute:: template_fields
      :annotation: = ['filename', 'sql', 'sql_params']

      

   .. attribute:: ui_color
      :annotation: = #e08c8c

      

   
   .. method:: _write_temp_file(self, cursor: Any, path_to_save: Union[str, bytes, int])



   
   .. method:: execute(self, context: dict)




