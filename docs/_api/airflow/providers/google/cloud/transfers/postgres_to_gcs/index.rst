:mod:`airflow.providers.google.cloud.transfers.postgres_to_gcs`
===============================================================

.. py:module:: airflow.providers.google.cloud.transfers.postgres_to_gcs

.. autoapi-nested-parse::

   PostgreSQL to GCS operator.



Module Contents
---------------

.. py:class:: _PostgresServerSideCursorDecorator(cursor)

   Inspired by `_PrestoToGCSPrestoCursorAdapter` to keep this consistent.

   Decorator for allowing description to be available for postgres cursor in case server side
   cursor is used. It doesn't provide other methods except those needed in BaseSQLToGCSOperator,
   which is more of a safety feature.

   .. attribute:: description
      

      Fetch first row to initialize cursor description when using server side cursor.


   
   .. method:: __iter__(self)



   
   .. method:: __next__(self)




.. py:class:: PostgresToGCSOperator(*, postgres_conn_id='postgres_default', use_server_side_cursor=False, cursor_itersize=2000, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator`

   Copy data from Postgres to Google Cloud Storage in JSON or CSV format.

   :param postgres_conn_id: Reference to a specific Postgres hook.
   :type postgres_conn_id: str
   :param use_server_side_cursor: If server-side cursor should be used for querying postgres.
       For detailed info, check https://www.psycopg.org/docs/usage.html#server-side-cursors
   :type use_server_side_cursor: bool
   :param cursor_itersize: How many records are fetched at a time in case of server-side cursor.
   :type cursor_itersize: int

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   .. attribute:: type_map
      

      

   
   .. method:: _unique_name(self)



   
   .. method:: query(self)

      Queries Postgres and returns a cursor to the results.



   
   .. method:: field_to_bigquery(self, field)



   
   .. method:: convert_type(self, value, schema_type)

      Takes a value from Postgres, and converts it to a value that's safe for
      JSON/Google Cloud Storage/BigQuery. Dates are converted to UTC seconds.
      Decimals are converted to floats. Times are converted to seconds.




