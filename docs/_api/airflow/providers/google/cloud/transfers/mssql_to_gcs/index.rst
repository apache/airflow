:mod:`airflow.providers.google.cloud.transfers.mssql_to_gcs`
============================================================

.. py:module:: airflow.providers.google.cloud.transfers.mssql_to_gcs

.. autoapi-nested-parse::

   MsSQL to GCS operator.



Module Contents
---------------

.. py:class:: MSSQLToGCSOperator(*, mssql_conn_id='mssql_default', **kwargs)

   Bases: :class:`airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator`

   Copy data from Microsoft SQL Server to Google Cloud Storage
   in JSON or CSV format.

   :param mssql_conn_id: Reference to a specific MSSQL hook.
   :type mssql_conn_id: str

   **Example**:
       The following operator will export data from the Customers table
       within the given MSSQL Database and then upload it to the
       'mssql-export' GCS bucket (along with a schema file). ::

           export_customers = MsSqlToGoogleCloudStorageOperator(
               task_id='export_customers',
               sql='SELECT * FROM dbo.Customers;',
               bucket='mssql-export',
               filename='data/customers/export.json',
               schema_filename='schemas/export.json',
               mssql_conn_id='mssql_default',
               google_cloud_storage_conn_id='google_cloud_default',
               dag=dag
           )

   .. attribute:: ui_color
      :annotation: = #e0a98c

      

   .. attribute:: type_map
      

      

   
   .. method:: query(self)

      Queries MSSQL and returns a cursor of results.

      :return: mssql cursor



   
   .. method:: field_to_bigquery(self, field)



   
   .. classmethod:: convert_type(cls, value, schema_type)

      Takes a value from MSSQL, and converts it to a value that's safe for
      JSON/Google Cloud Storage/BigQuery.




