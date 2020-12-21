:mod:`airflow.providers.microsoft.azure.operators.adls_list`
============================================================

.. py:module:: airflow.providers.microsoft.azure.operators.adls_list


Module Contents
---------------

.. py:class:: AzureDataLakeStorageListOperator(*, path: str, azure_data_lake_conn_id: str = 'azure_data_lake_default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   List all files from the specified path

   This operator returns a python list with the names of files which can be used by
    `xcom` in the downstream tasks.

   :param path: The Azure Data Lake path to find the objects. Supports glob
       strings (templated)
   :type path: str
   :param azure_data_lake_conn_id: The connection ID to use when
       connecting to Azure Data Lake Storage.
   :type azure_data_lake_conn_id: str

   **Example**:
       The following Operator would list all the Parquet files from ``folder/output/``
       folder in the specified ADLS account ::

           adls_files = AzureDataLakeStorageListOperator(
               task_id='adls_files',
               path='folder/output/*.parquet',
               azure_data_lake_conn_id='azure_data_lake_default'
           )

   .. attribute:: template_fields
      :annotation: :Sequence[str] = ['path']

      

   .. attribute:: ui_color
      :annotation: = #901dd2

      

   
   .. method:: execute(self, context: dict)




