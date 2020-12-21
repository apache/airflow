:mod:`airflow.providers.google.cloud.transfers.sql_to_gcs`
==========================================================

.. py:module:: airflow.providers.google.cloud.transfers.sql_to_gcs

.. autoapi-nested-parse::

   Base operator for SQL to GCS operators.



Module Contents
---------------

.. py:class:: BaseSQLToGCSOperator(*, sql: str, bucket: str, filename: str, schema_filename: Optional[str] = None, approx_max_file_size_bytes: int = 1900000000, export_format: str = 'json', field_delimiter: str = ',', null_marker: Optional[str] = None, gzip: bool = False, schema: Optional[Union[str, list]] = None, parameters: Optional[dict] = None, gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   :param sql: The SQL to execute.
   :type sql: str
   :param bucket: The bucket to upload to.
   :type bucket: str
   :param filename: The filename to use as the object name when uploading
       to Google Cloud Storage. A ``{}`` should be specified in the filename
       to allow the operator to inject file numbers in cases where the
       file is split due to size.
   :type filename: str
   :param schema_filename: If set, the filename to use as the object name
       when uploading a .json file containing the BigQuery schema fields
       for the table that was dumped from the database.
   :type schema_filename: str
   :param approx_max_file_size_bytes: This operator supports the ability
       to split large table dumps into multiple files (see notes in the
       filename param docs above). This param allows developers to specify the
       file size of the splits. Check https://cloud.google.com/storage/quotas
       to see the maximum allowed file size for a single object.
   :type approx_max_file_size_bytes: long
   :param export_format: Desired format of files to be exported.
   :type export_format: str
   :param field_delimiter: The delimiter to be used for CSV files.
   :type field_delimiter: str
   :param null_marker: The null marker to be used for CSV files.
   :type null_marker: str
   :param gzip: Option to compress file for upload (does not apply to schemas).
   :type gzip: bool
   :param schema: The schema to use, if any. Should be a list of dict or
       a str. Pass a string if using Jinja template, otherwise, pass a list of
       dict. Examples could be seen: https://cloud.google.com/bigquery/docs
       /schemas#specifying_a_json_schema_file
   :type schema: str or list
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param parameters: a parameters dict that is substituted at query runtime.
   :type parameters: dict
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['sql', 'bucket', 'filename', 'schema_filename', 'schema', 'parameters', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   
   .. method:: execute(self, context)



   
   .. method:: convert_types(self, schema, col_type_dict, row)

      Convert values from DBAPI to output-friendly formats.



   
   .. method:: _write_local_data_files(self, cursor)

      Takes a cursor, and writes results to a local file.

      :return: A dictionary where keys are filenames to be used as object
          names in GCS, and values are file handles to local files that
          contain the data for the GCS objects.



   
   .. method:: _configure_csv_file(self, file_handle, schema)

      Configure a csv writer with the file_handle and write schema
      as headers for the new file.



   
   .. method:: query(self)

      Execute DBAPI query.



   
   .. method:: field_to_bigquery(self, field)

      Convert a DBAPI field to BigQuery schema format.



   
   .. method:: convert_type(self, value, schema_type)

      Convert a value from DBAPI to output-friendly formats.



   
   .. method:: _get_col_type_dict(self)

      Return a dict of column name and column type based on self.schema if not None.



   
   .. method:: _write_local_schema_file(self, cursor)

      Takes a cursor, and writes the BigQuery schema for the results to a
      local file system. Schema for database will be read from cursor if
      not specified.

      :return: A dictionary where key is a filename to be used as an object
          name in GCS, and values are file handles to local files that
          contains the BigQuery schema fields in .json format.



   
   .. method:: _upload_to_gcs(self, files_to_upload)

      Upload all of the file splits (and optionally the schema .json file) to
      Google Cloud Storage.




