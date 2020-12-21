:mod:`airflow.providers.google.cloud.transfers.mysql_to_gcs`
============================================================

.. py:module:: airflow.providers.google.cloud.transfers.mysql_to_gcs

.. autoapi-nested-parse::

   MySQL to GCS operator.



Module Contents
---------------

.. py:class:: MySQLToGCSOperator(*, mysql_conn_id='mysql_default', ensure_utc=False, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator`

   Copy data from MySQL to Google Cloud Storage in JSON or CSV format.

   :param mysql_conn_id: Reference to a specific MySQL hook.
   :type mysql_conn_id: str
   :param ensure_utc: Ensure TIMESTAMP columns exported as UTC. If set to
       `False`, TIMESTAMP columns will be exported using the MySQL server's
       default timezone.
   :type ensure_utc: bool

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   .. attribute:: type_map
      

      

   
   .. method:: query(self)

      Queries mysql and returns a cursor to the results.



   
   .. method:: field_to_bigquery(self, field)



   
   .. method:: convert_type(self, value, schema_type: str)

      Takes a value from MySQLdb, and converts it to a value that's safe for
      JSON/Google Cloud Storage/BigQuery.

      * Datetimes are converted to UTC seconds.
      * Decimals are converted to floats.
      * Dates are converted to ISO formatted string if given schema_type is
        DATE, or UTC seconds otherwise.
      * Binary type fields are converted to integer if given schema_type is
        INTEGER, or encoded with base64 otherwise. Imported BYTES data must
        be base64-encoded according to BigQuery documentation:
        https://cloud.google.com/bigquery/data-types

      :param value: MySQLdb column value
      :type value: Any
      :param schema_type: BigQuery data type
      :type schema_type: str




