:mod:`airflow.providers.google.cloud.transfers.cassandra_to_gcs`
================================================================

.. py:module:: airflow.providers.google.cloud.transfers.cassandra_to_gcs

.. autoapi-nested-parse::

   This module contains operator for copying
   data from Cassandra to Google Cloud Storage in JSON format.



Module Contents
---------------

.. py:class:: CassandraToGCSOperator(*, cql: str, bucket: str, filename: str, schema_filename: Optional[str] = None, approx_max_file_size_bytes: int = 1900000000, gzip: bool = False, cassandra_conn_id: str = 'cassandra_default', gcp_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Copy data from Cassandra to Google Cloud Storage in JSON format

   Note: Arrays of arrays are not supported.

   :param cql: The CQL to execute on the Cassandra table.
   :type cql: str
   :param bucket: The bucket to upload to.
   :type bucket: str
   :param filename: The filename to use as the object name when uploading
       to Google Cloud Storage. A {} should be specified in the filename
       to allow the operator to inject file numbers in cases where the
       file is split due to size.
   :type filename: str
   :param schema_filename: If set, the filename to use as the object name
       when uploading a .json file containing the BigQuery schema fields
       for the table that was dumped from MySQL.
   :type schema_filename: str
   :param approx_max_file_size_bytes: This operator supports the ability
       to split large table dumps into multiple files (see notes in the
       filename param docs above). This param allows developers to specify the
       file size of the splits. Check https://cloud.google.com/storage/quotas
       to see the maximum allowed file size for a single object.
   :type approx_max_file_size_bytes: long
   :param cassandra_conn_id: Reference to a specific Cassandra hook.
   :type cassandra_conn_id: str
   :param gzip: Option to compress file for upload
   :type gzip: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['cql', 'bucket', 'filename', 'schema_filename', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.cql']

      

   .. attribute:: ui_color
      :annotation: = #a0e08c

      

   .. attribute:: CQL_TYPE_MAP
      

      

   
   .. method:: execute(self, context: Dict[str, str])



   
   .. method:: _write_local_data_files(self, cursor)

      Takes a cursor, and writes results to a local file.

      :return: A dictionary where keys are filenames to be used as object
          names in GCS, and values are file handles to local files that
          contain the data for the GCS objects.



   
   .. method:: _write_local_schema_file(self, cursor)

      Takes a cursor, and writes the BigQuery schema for the results to a
      local file system.

      :return: A dictionary where key is a filename to be used as an object
          name in GCS, and values are file handles to local files that
          contains the BigQuery schema fields in .json format.



   
   .. method:: _upload_to_gcs(self, files_to_upload: Dict[str, Any])



   
   .. classmethod:: generate_data_dict(cls, names: Iterable[str], values: Any)

      Generates data structure that will be stored as file in GCS.



   
   .. classmethod:: convert_value(cls, value: Optional[Any])

      Convert value to BQ type.



   
   .. classmethod:: convert_array_types(cls, value: Union[List[Any], SortedSet])

      Maps convert_value over array.



   
   .. classmethod:: convert_user_type(cls, value: Any)

      Converts a user type to RECORD that contains n fields, where n is the
      number of attributes. Each element in the user type class will be converted to its
      corresponding data type in BQ.



   
   .. classmethod:: convert_tuple_type(cls, values: Tuple[Any])

      Converts a tuple to RECORD that contains n fields, each will be converted
      to its corresponding data type in bq and will be named 'field_<index>', where
      index is determined by the order of the tuple elements defined in cassandra.



   
   .. classmethod:: convert_map_type(cls, value: OrderedMapSerializedKey)

      Converts a map to a repeated RECORD that contains two fields: 'key' and 'value',
      each will be converted to its corresponding data type in BQ.



   
   .. classmethod:: generate_schema_dict(cls, name: str, type_: Any)

      Generates BQ schema.



   
   .. classmethod:: get_bq_fields(cls, type_: Any)

      Converts non simple type value to BQ representation.



   
   .. staticmethod:: is_simple_type(type_: Any)

      Check if type is a simple type.



   
   .. staticmethod:: is_array_type(type_: Any)

      Check if type is an array type.



   
   .. staticmethod:: is_record_type(type_: Any)

      Checks the record type.



   
   .. classmethod:: get_bq_type(cls, type_: Any)

      Converts type to equivalent BQ type.



   
   .. classmethod:: get_bq_mode(cls, type_: Any)

      Converts type to equivalent BQ mode.




