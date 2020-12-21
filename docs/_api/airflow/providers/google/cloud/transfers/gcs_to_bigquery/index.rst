:mod:`airflow.providers.google.cloud.transfers.gcs_to_bigquery`
===============================================================

.. py:module:: airflow.providers.google.cloud.transfers.gcs_to_bigquery

.. autoapi-nested-parse::

   This module contains a Google Cloud Storage to BigQuery operator.



Module Contents
---------------

.. py:class:: GCSToBigQueryOperator(*, bucket, source_objects, destination_project_dataset_table, schema_fields=None, schema_object=None, source_format='CSV', compression='NONE', create_disposition='CREATE_IF_NEEDED', skip_leading_rows=0, write_disposition='WRITE_EMPTY', field_delimiter=',', max_bad_records=0, quote_character=None, ignore_unknown_values=False, allow_quoted_newlines=False, allow_jagged_rows=False, encoding='UTF-8', max_id_key=None, bigquery_conn_id='google_cloud_default', google_cloud_storage_conn_id='google_cloud_default', delegate_to=None, schema_update_options=(), src_fmt_configs=None, external_table=False, time_partitioning=None, cluster_fields=None, autodetect=True, encryption_configuration=None, location=None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Loads files from Google Cloud Storage into BigQuery.

   The schema to be used for the BigQuery table may be specified in one of
   two ways. You may either directly pass the schema fields in, or you may
   point the operator to a Google Cloud Storage object name. The object in
   Google Cloud Storage must be a JSON file with the schema fields in it.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToBigQueryOperator`

   :param bucket: The bucket to load from. (templated)
   :type bucket: str
   :param source_objects: List of Google Cloud Storage URIs to load from. (templated)
       If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
   :type source_objects: list[str]
   :param destination_project_dataset_table: The dotted
       ``(<project>.|<project>:)<dataset>.<table>`` BigQuery table to load data into.
       If ``<project>`` is not included, project will be the project defined in
       the connection json. (templated)
   :type destination_project_dataset_table: str
   :param schema_fields: If set, the schema field list as defined here:
       https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
       Should not be set when source_format is 'DATASTORE_BACKUP'.
       Parameter must be defined if 'schema_object' is null and autodetect is False.
   :type schema_fields: list
   :param schema_object: If set, a GCS object path pointing to a .json file that
       contains the schema for the table. (templated)
       Parameter must be defined if 'schema_fields' is null and autodetect is False.
   :type schema_object: str
   :param source_format: File format to export.
   :type source_format: str
   :param compression: [Optional] The compression type of the data source.
       Possible values include GZIP and NONE.
       The default value is NONE.
       This setting is ignored for Google Cloud Bigtable,
       Google Cloud Datastore backups and Avro formats.
   :type compression: str
   :param create_disposition: The create disposition if the table doesn't exist.
   :type create_disposition: str
   :param skip_leading_rows: Number of rows to skip when loading from a CSV.
   :type skip_leading_rows: int
   :param write_disposition: The write disposition if the table already exists.
   :type write_disposition: str
   :param field_delimiter: The delimiter to use when loading from a CSV.
   :type field_delimiter: str
   :param max_bad_records: The maximum number of bad records that BigQuery can
       ignore when running the job.
   :type max_bad_records: int
   :param quote_character: The value that is used to quote data sections in a CSV file.
   :type quote_character: str
   :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
       extra values that are not represented in the table schema.
       If true, the extra values are ignored. If false, records with extra columns
       are treated as bad records, and if there are too many bad records, an
       invalid error is returned in the job result.
   :type ignore_unknown_values: bool
   :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
   :type allow_quoted_newlines: bool
   :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
       The missing values are treated as nulls. If false, records with missing trailing
       columns are treated as bad records, and if there are too many bad records, an
       invalid error is returned in the job result. Only applicable to CSV, ignored
       for other formats.
   :type allow_jagged_rows: bool
   :param encoding: The character encoding of the data. See:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).csvOptions.encoding
       https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
   :param max_id_key: If set, the name of a column in the BigQuery table
       that's to be loaded. This will be used to select the MAX value from
       BigQuery after the load occurs. The results will be returned by the
       execute() command, which in turn gets stored in XCom for future
       operators to use. This can be helpful with incremental loads--during
       future executions, you can pick up from the max ID.
   :type max_id_key: str
   :param bigquery_conn_id: (Optional) The connection ID used to connect to Google Cloud and
       interact with the BigQuery service.
   :type bigquery_conn_id: str
   :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
       and interact with the Google Cloud Storage service.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param schema_update_options: Allows the schema of the destination
       table to be updated as a side effect of the load job.
   :type schema_update_options: list
   :param src_fmt_configs: configure optional fields specific to the source format
   :type src_fmt_configs: dict
   :param external_table: Flag to specify if the destination table should be
       a BigQuery external table. Default Value is False.
   :type external_table: bool
   :param time_partitioning: configure optional time partitioning fields i.e.
       partition by field, type and  expiration as per API specifications.
       Note that 'field' is not available in concurrency with
       dataset.table$partition.
   :type time_partitioning: dict
   :param cluster_fields: Request that the result of this load be stored sorted
       by one or more columns. BigQuery supports clustering for both partitioned and
       non-partitioned tables. The order of columns given determines the sort order.
       Not applicable for external tables.
   :type cluster_fields: list[str]
   :param autodetect: [Optional] Indicates if we should automatically infer the
       options and schema for CSV and JSON sources. (Default: ``True``).
       Parameter must be setted to True if 'schema_fields' and 'schema_object' are undefined.
       It is suggested to set to True if table are create outside of Airflow.
   :type autodetect: bool
   :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
       **Example**: ::

           encryption_configuration = {
               "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
           }
   :type encryption_configuration: dict
   :param location: [Optional] The geographic location of the job. Required except for US and EU.
       See details at https://cloud.google.com/bigquery/docs/locations#specifying_your_location
   :type location: str
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
      :annotation: = ['bucket', 'source_objects', 'schema_object', 'destination_project_dataset_table', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      :annotation: = #f0eee4

      

   
   .. method:: execute(self, context)




