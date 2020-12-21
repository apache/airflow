:mod:`airflow.providers.google.cloud.hooks.bigquery`
====================================================

.. py:module:: airflow.providers.google.cloud.hooks.bigquery

.. autoapi-nested-parse::

   This module contains a BigQuery Hook, as well as a very basic PEP 249
   implementation for BigQuery.



Module Contents
---------------

.. data:: log
   

   

.. data:: BigQueryJob
   

   

.. py:class:: BigQueryHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, use_legacy_sql: bool = True, location: Optional[str] = None, bigquery_conn_id: Optional[str] = None, api_resource_configs: Optional[Dict] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`, :class:`airflow.hooks.dbapi_hook.DbApiHook`

   Interact with BigQuery. This hook uses the Google Cloud connection.

   .. attribute:: conn_name_attr
      :annotation: :str = gcp_conn_id

      

   
   .. method:: get_conn(self)

      Returns a BigQuery PEP 249 connection object.



   
   .. method:: get_service(self)

      Returns a BigQuery service object.



   
   .. method:: get_client(self, project_id: Optional[str] = None, location: Optional[str] = None)

      Returns authenticated BigQuery Client.

      :param project_id: Project ID for the project which the client acts on behalf of.
      :type project_id: str
      :param location: Default location for jobs / datasets / tables.
      :type location: str
      :return:



   
   .. staticmethod:: _resolve_table_reference(table_resource: Dict[str, Any], project_id: Optional[str] = None, dataset_id: Optional[str] = None, table_id: Optional[str] = None)



   
   .. method:: insert_rows(self, table: Any, rows: Any, target_fields: Any = None, commit_every: Any = 1000, replace: Any = False, **kwargs)

      Insertion is currently unsupported. Theoretically, you could use
      BigQuery's streaming API to insert rows into a table, but this hasn't
      been implemented.



   
   .. method:: get_pandas_df(self, sql: str, parameters: Optional[Union[Iterable, Mapping]] = None, dialect: Optional[str] = None, **kwargs)

      Returns a Pandas DataFrame for the results produced by a BigQuery
      query. The DbApiHook method must be overridden because Pandas
      doesn't support PEP 249 connections, except for SQLite. See:

      https://github.com/pydata/pandas/blob/master/pandas/io/sql.py#L447
      https://github.com/pydata/pandas/issues/6900

      :param sql: The BigQuery SQL to execute.
      :type sql: str
      :param parameters: The parameters to render the SQL query with (not
          used, leave to override superclass method)
      :type parameters: mapping or iterable
      :param dialect: Dialect of BigQuery SQL – legacy SQL or standard SQL
          defaults to use `self.use_legacy_sql` if not specified
      :type dialect: str in {'legacy', 'standard'}
      :param kwargs: (optional) passed into pandas_gbq.read_gbq method
      :type kwargs: dict



   
   .. method:: table_exists(self, dataset_id: str, table_id: str, project_id: str)

      Checks for the existence of a table in Google BigQuery.

      :param project_id: The Google cloud project in which to look for the
          table. The connection supplied to the hook must provide access to
          the specified project.
      :type project_id: str
      :param dataset_id: The name of the dataset in which to look for the
          table.
      :type dataset_id: str
      :param table_id: The name of the table to check the existence of.
      :type table_id: str



   
   .. method:: table_partition_exists(self, dataset_id: str, table_id: str, partition_id: str, project_id: str)

      Checks for the existence of a partition in a table in Google BigQuery.

      :param project_id: The Google cloud project in which to look for the
          table. The connection supplied to the hook must provide access to
          the specified project.
      :type project_id: str
      :param dataset_id: The name of the dataset in which to look for the
          table.
      :type dataset_id: str
      :param table_id: The name of the table to check the existence of.
      :type table_id: str
      :param partition_id: The name of the partition to check the existence of.
      :type partition_id: str



   
   .. method:: create_empty_table(self, project_id: Optional[str] = None, dataset_id: Optional[str] = None, table_id: Optional[str] = None, table_resource: Optional[Dict[str, Any]] = None, schema_fields: Optional[List] = None, time_partitioning: Optional[Dict] = None, cluster_fields: Optional[List[str]] = None, labels: Optional[Dict] = None, view: Optional[Dict] = None, encryption_configuration: Optional[Dict] = None, retry: Optional[Retry] = DEFAULT_RETRY, num_retries: Optional[int] = None, location: Optional[str] = None, exists_ok: bool = True)

      Creates a new, empty table in the dataset.
      To create a view, which is defined by a SQL query, parse a dictionary to 'view' kwarg

      :param project_id: The project to create the table into.
      :type project_id: str
      :param dataset_id: The dataset to create the table into.
      :type dataset_id: str
      :param table_id: The Name of the table to be created.
      :type table_id: str
      :param table_resource: Table resource as described in documentation:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
          If provided all other parameters are ignored.
      :type table_resource: Dict[str, Any]
      :param schema_fields: If set, the schema field list as defined here:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema
      :type schema_fields: list
      :param labels: a dictionary containing labels for the table, passed to BigQuery
      :type labels: dict
      :param retry: Optional. How to retry the RPC.
      :type retry: google.api_core.retry.Retry

      **Example**: ::

          schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                         {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

      :param time_partitioning: configure optional time partitioning fields i.e.
          partition by field, type and expiration as per API specifications.

          .. seealso::
              https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
      :type time_partitioning: dict
      :param cluster_fields: [Optional] The fields used for clustering.
          BigQuery supports clustering for both partitioned and
          non-partitioned tables.
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#clustering.fields
      :type cluster_fields: list
      :param view: [Optional] A dictionary containing definition for the view.
          If set, it will create a view instead of a table:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
      :type view: dict

      **Example**: ::

          view = {
              "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 1000",
              "useLegacySql": False
          }

      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::

              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict
      :param num_retries: Maximum number of retries in case of connection problems.
      :type num_retries: int
      :param exists_ok: If ``True``, ignore "already exists" errors when creating the table.
      :type exists_ok: bool
      :return: Created table



   
   .. method:: create_empty_dataset(self, dataset_id: Optional[str] = None, project_id: Optional[str] = None, location: Optional[str] = None, dataset_reference: Optional[Dict[str, Any]] = None, exists_ok: bool = True)

      Create a new empty dataset:
      https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert

      :param project_id: The name of the project where we want to create
          an empty a dataset. Don't need to provide, if projectId in dataset_reference.
      :type project_id: str
      :param dataset_id: The id of dataset. Don't need to provide, if datasetId in dataset_reference.
      :type dataset_id: str
      :param location: (Optional) The geographic location where the dataset should reside.
          There is no default value but the dataset will be created in US if nothing is provided.
      :type location: str
      :param dataset_reference: Dataset reference that could be provided with request body. More info:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
      :type dataset_reference: dict
      :param exists_ok: If ``True``, ignore "already exists" errors when creating the DATASET.
      :type exists_ok: bool



   
   .. method:: get_dataset_tables(self, dataset_id: str, project_id: Optional[str] = None, max_results: Optional[int] = None, retry: Retry = DEFAULT_RETRY)

      Get the list of tables for a given dataset.

      For more information, see:
      https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

      :param dataset_id: the dataset ID of the requested dataset.
      :type dataset_id: str
      :param project_id: (Optional) the project of the requested dataset. If None,
          self.project_id will be used.
      :type project_id: str
      :param max_results: (Optional) the maximum number of tables to return.
      :type max_results: int
      :param retry: How to retry the RPC.
      :type retry: google.api_core.retry.Retry
      :return: List of tables associated with the dataset.



   
   .. method:: delete_dataset(self, dataset_id: str, project_id: Optional[str] = None, delete_contents: bool = False, retry: Retry = DEFAULT_RETRY)

      Delete a dataset of Big query in your project.

      :param project_id: The name of the project where we have the dataset.
      :type project_id: str
      :param dataset_id: The dataset to be delete.
      :type dataset_id: str
      :param delete_contents: If True, delete all the tables in the dataset.
          If False and the dataset contains tables, the request will fail.
      :type delete_contents: bool
      :param retry: How to retry the RPC.
      :type retry: google.api_core.retry.Retry



   
   .. method:: create_external_table(self, external_project_dataset_table: str, schema_fields: List, source_uris: List, source_format: str = 'CSV', autodetect: bool = False, compression: str = 'NONE', ignore_unknown_values: bool = False, max_bad_records: int = 0, skip_leading_rows: int = 0, field_delimiter: str = ',', quote_character: Optional[str] = None, allow_quoted_newlines: bool = False, allow_jagged_rows: bool = False, encoding: str = 'UTF-8', src_fmt_configs: Optional[Dict] = None, labels: Optional[Dict] = None, encryption_configuration: Optional[Dict] = None, location: Optional[str] = None, project_id: Optional[str] = None)

      Creates a new external table in the dataset with the data from Google
      Cloud Storage. See here:

      https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource

      for more details about these parameters.

      :param external_project_dataset_table:
          The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery
          table name to create external table.
          If ``<project>`` is not included, project will be the
          project defined in the connection json.
      :type external_project_dataset_table: str
      :param schema_fields: The schema field list as defined here:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource
      :type schema_fields: list
      :param source_uris: The source Google Cloud
          Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
          per-object name can be used.
      :type source_uris: list
      :param source_format: File format to export.
      :type source_format: str
      :param autodetect: Try to detect schema and format options automatically.
          Any option specified explicitly will be honored.
      :type autodetect: bool
      :param compression: [Optional] The compression type of the data source.
          Possible values include GZIP and NONE.
          The default value is NONE.
          This setting is ignored for Google Cloud Bigtable,
          Google Cloud Datastore backups and Avro formats.
      :type compression: str
      :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
          extra values that are not represented in the table schema.
          If true, the extra values are ignored. If false, records with extra columns
          are treated as bad records, and if there are too many bad records, an
          invalid error is returned in the job result.
      :type ignore_unknown_values: bool
      :param max_bad_records: The maximum number of bad records that BigQuery can
          ignore when running the job.
      :type max_bad_records: int
      :param skip_leading_rows: Number of rows to skip when loading from a CSV.
      :type skip_leading_rows: int
      :param field_delimiter: The delimiter to use when loading from a CSV.
      :type field_delimiter: str
      :param quote_character: The value that is used to quote data sections in a CSV
          file.
      :type quote_character: str
      :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not
          (false).
      :type allow_quoted_newlines: bool
      :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
          The missing values are treated as nulls. If false, records with missing
          trailing columns are treated as bad records, and if there are too many bad
          records, an invalid error is returned in the job result. Only applicable when
          source_format is CSV.
      :type allow_jagged_rows: bool
      :param encoding: The character encoding of the data. See:

          .. seealso::
              https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
      :type encoding: str
      :param src_fmt_configs: configure optional fields specific to the source format
      :type src_fmt_configs: dict
      :param labels: a dictionary containing labels for the table, passed to BigQuery
      :type labels: dict
      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::

              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict



   
   .. method:: update_table(self, table_resource: Dict[str, Any], fields: Optional[List[str]] = None, dataset_id: Optional[str] = None, table_id: Optional[str] = None, project_id: Optional[str] = None)

      Change some fields of a table.

      Use ``fields`` to specify which fields to update. At least one field
      must be provided. If a field is listed in ``fields`` and is ``None``
      in ``table``, the field value will be deleted.

      If ``table.etag`` is not ``None``, the update will only succeed if
      the table on the server has the same ETag. Thus reading a table with
      ``get_table``, changing its fields, and then passing it to
      ``update_table`` will ensure that the changes will only be saved if
      no modifications to the table occurred since the read.

      :param project_id: The project to create the table into.
      :type project_id: str
      :param dataset_id: The dataset to create the table into.
      :type dataset_id: str
      :param table_id: The Name of the table to be created.
      :type table_id: str
      :param table_resource: Table resource as described in documentation:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
          The table has to contain ``tableReference`` or ``project_id``, ``dataset_id`` and ``table_id``
          have to be provided.
      :type table_resource: Dict[str, Any]
      :param fields: The fields of ``table`` to change, spelled as the Table
          properties (e.g. "friendly_name").
      :type fields: List[str]



   
   .. method:: patch_table(self, dataset_id: str, table_id: str, project_id: Optional[str] = None, description: Optional[str] = None, expiration_time: Optional[int] = None, external_data_configuration: Optional[Dict] = None, friendly_name: Optional[str] = None, labels: Optional[Dict] = None, schema: Optional[List] = None, time_partitioning: Optional[Dict] = None, view: Optional[Dict] = None, require_partition_filter: Optional[bool] = None, encryption_configuration: Optional[Dict] = None)

      Patch information in an existing table.
      It only updates fields that are provided in the request object.

      Reference: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/patch

      :param dataset_id: The dataset containing the table to be patched.
      :type dataset_id: str
      :param table_id: The Name of the table to be patched.
      :type table_id: str
      :param project_id: The project containing the table to be patched.
      :type project_id: str
      :param description: [Optional] A user-friendly description of this table.
      :type description: str
      :param expiration_time: [Optional] The time when this table expires,
          in milliseconds since the epoch.
      :type expiration_time: int
      :param external_data_configuration: [Optional] A dictionary containing
          properties of a table stored outside of BigQuery.
      :type external_data_configuration: dict
      :param friendly_name: [Optional] A descriptive name for this table.
      :type friendly_name: str
      :param labels: [Optional] A dictionary containing labels associated with this table.
      :type labels: dict
      :param schema: [Optional] If set, the schema field list as defined here:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema
          The supported schema modifications and unsupported schema modification are listed here:
          https://cloud.google.com/bigquery/docs/managing-table-schemas
          **Example**: ::

              schema=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                             {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

      :type schema: list
      :param time_partitioning: [Optional] A dictionary containing time-based partitioning
           definition for the table.
      :type time_partitioning: dict
      :param view: [Optional] A dictionary containing definition for the view.
          If set, it will patch a view instead of a table:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
          **Example**: ::

              view = {
                  "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
                  "useLegacySql": False
              }

      :type view: dict
      :param require_partition_filter: [Optional] If true, queries over the this table require a
          partition filter. If false, queries over the table
      :type require_partition_filter: bool
      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::

              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict



   
   .. method:: insert_all(self, project_id: str, dataset_id: str, table_id: str, rows: List, ignore_unknown_values: bool = False, skip_invalid_rows: bool = False, fail_on_error: bool = False)

      Method to stream data into BigQuery one record at a time without needing
      to run a load job

      .. seealso::
          For more information, see:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll

      :param project_id: The name of the project where we have the table
      :type project_id: str
      :param dataset_id: The name of the dataset where we have the table
      :type dataset_id: str
      :param table_id: The name of the table
      :type table_id: str
      :param rows: the rows to insert
      :type rows: list

      **Example or rows**:
          rows=[{"json": {"a_key": "a_value_0"}}, {"json": {"a_key": "a_value_1"}}]

      :param ignore_unknown_values: [Optional] Accept rows that contain values
          that do not match the schema. The unknown values are ignored.
          The default value  is false, which treats unknown values as errors.
      :type ignore_unknown_values: bool
      :param skip_invalid_rows: [Optional] Insert all valid rows of a request,
          even if invalid rows exist. The default value is false, which causes
          the entire request to fail if any invalid rows exist.
      :type skip_invalid_rows: bool
      :param fail_on_error: [Optional] Force the task to fail if any errors occur.
          The default value is false, which indicates the task should not fail
          even if any insertion errors occur.
      :type fail_on_error: bool



   
   .. method:: update_dataset(self, fields: Sequence[str], dataset_resource: Dict[str, Any], dataset_id: Optional[str] = None, project_id: Optional[str] = None, retry: Retry = DEFAULT_RETRY)

      Change some fields of a dataset.

      Use ``fields`` to specify which fields to update. At least one field
      must be provided. If a field is listed in ``fields`` and is ``None`` in
      ``dataset``, it will be deleted.

      If ``dataset.etag`` is not ``None``, the update will only
      succeed if the dataset on the server has the same ETag. Thus
      reading a dataset with ``get_dataset``, changing its fields,
      and then passing it to ``update_dataset`` will ensure that the changes
      will only be saved if no modifications to the dataset occurred
      since the read.

      :param dataset_resource: Dataset resource that will be provided
          in request body.
          https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
      :type dataset_resource: dict
      :param dataset_id: The id of the dataset.
      :type dataset_id: str
      :param fields: The properties of ``dataset`` to change (e.g. "friendly_name").
      :type fields: Sequence[str]
      :param project_id: The Google Cloud Project ID
      :type project_id: str
      :param retry: How to retry the RPC.
      :type retry: google.api_core.retry.Retry



   
   .. method:: patch_dataset(self, dataset_id: str, dataset_resource: Dict, project_id: Optional[str] = None)

      Patches information in an existing dataset.
      It only replaces fields that are provided in the submitted dataset resource.
      More info:
      https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/patch

      :param dataset_id: The BigQuery Dataset ID
      :type dataset_id: str
      :param dataset_resource: Dataset resource that will be provided
          in request body.
          https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
      :type dataset_resource: dict
      :param project_id: The Google Cloud Project ID
      :type project_id: str
      :rtype: dataset
          https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource



   
   .. method:: get_dataset_tables_list(self, dataset_id: str, project_id: Optional[str] = None, table_prefix: Optional[str] = None, max_results: Optional[int] = None)

      Method returns tables list of a BigQuery tables. If table prefix is specified,
      only tables beginning by it are returned.

      For more information, see:
      https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

      :param dataset_id: The BigQuery Dataset ID
      :type dataset_id: str
      :param project_id: The Google Cloud Project ID
      :type project_id: str
      :param table_prefix: Tables must begin by this prefix to be returned (case sensitive)
      :type table_prefix: str
      :param max_results: The maximum number of results to return in a single response page.
          Leverage the page tokens to iterate through the entire collection.
      :type max_results: int
      :return: List of tables associated with the dataset



   
   .. method:: get_datasets_list(self, project_id: Optional[str] = None, include_all: bool = False, filter_: Optional[str] = None, max_results: Optional[int] = None, page_token: Optional[str] = None, retry: Retry = DEFAULT_RETRY)

      Method returns full list of BigQuery datasets in the current project

      For more information, see:
      https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list

      :param project_id: Google Cloud Project for which you try to get all datasets
      :type project_id: str
      :param include_all: True if results include hidden datasets. Defaults to False.
      :param filter_: An expression for filtering the results by label. For syntax, see
          https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list#filter.
      :param filter_: str
      :param max_results: Maximum number of datasets to return.
      :param max_results: int
      :param page_token: Token representing a cursor into the datasets. If not passed,
          the API will return the first page of datasets. The token marks the beginning of the
          iterator to be returned and the value of the ``page_token`` can be accessed at
          ``next_page_token`` of the :class:`~google.api_core.page_iterator.HTTPIterator`.
      :param page_token: str
      :param retry: How to retry the RPC.
      :type retry: google.api_core.retry.Retry



   
   .. method:: get_dataset(self, dataset_id: str, project_id: Optional[str] = None)

      Fetch the dataset referenced by dataset_id.

      :param dataset_id: The BigQuery Dataset ID
      :type dataset_id: str
      :param project_id: The Google Cloud Project ID
      :type project_id: str
      :return: dataset_resource

          .. seealso::
              For more information, see Dataset Resource content:
              https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource



   
   .. method:: run_grant_dataset_view_access(self, source_dataset: str, view_dataset: str, view_table: str, source_project: Optional[str] = None, view_project: Optional[str] = None, project_id: Optional[str] = None)

      Grant authorized view access of a dataset to a view table.
      If this view has already been granted access to the dataset, do nothing.
      This method is not atomic.  Running it may clobber a simultaneous update.

      :param source_dataset: the source dataset
      :type source_dataset: str
      :param view_dataset: the dataset that the view is in
      :type view_dataset: str
      :param view_table: the table of the view
      :type view_table: str
      :param project_id: the project of the source dataset. If None,
          self.project_id will be used.
      :type project_id: str
      :param view_project: the project that the view is in. If None,
          self.project_id will be used.
      :type view_project: str
      :return: the datasets resource of the source dataset.



   
   .. method:: run_table_upsert(self, dataset_id: str, table_resource: Dict[str, Any], project_id: Optional[str] = None)

      If the table already exists, update the existing table if not create new.
      Since BigQuery does not natively allow table upserts, this is not an
      atomic operation.

      :param dataset_id: the dataset to upsert the table into.
      :type dataset_id: str
      :param table_resource: a table resource. see
          https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
      :type table_resource: dict
      :param project_id: the project to upsert the table into.  If None,
          project will be self.project_id.
      :return:



   
   .. method:: run_table_delete(self, deletion_dataset_table: str, ignore_if_missing: bool = False)

      Delete an existing table from the dataset;
      If the table does not exist, return an error unless ignore_if_missing
      is set to True.

      :param deletion_dataset_table: A dotted
          ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
          will be deleted.
      :type deletion_dataset_table: str
      :param ignore_if_missing: if True, then return success even if the
          requested table does not exist.
      :type ignore_if_missing: bool
      :return:



   
   .. method:: delete_table(self, table_id: str, not_found_ok: bool = True, project_id: Optional[str] = None)

      Delete an existing table from the dataset. If the table does not exist, return an error
      unless not_found_ok is set to True.

      :param table_id: A dotted ``(<project>.|<project>:)<dataset>.<table>``
          that indicates which table will be deleted.
      :type table_id: str
      :param not_found_ok: if True, then return success even if the
          requested table does not exist.
      :type not_found_ok: bool
      :param project_id: the project used to perform the request
      :type project_id: str



   
   .. method:: get_tabledata(self, dataset_id: str, table_id: str, max_results: Optional[int] = None, selected_fields: Optional[str] = None, page_token: Optional[str] = None, start_index: Optional[int] = None)

      Get the data of a given dataset.table and optionally with selected columns.
      see https://cloud.google.com/bigquery/docs/reference/v2/tabledata/list

      :param dataset_id: the dataset ID of the requested table.
      :param table_id: the table ID of the requested table.
      :param max_results: the maximum results to return.
      :param selected_fields: List of fields to return (comma-separated). If
          unspecified, all fields are returned.
      :param page_token: page token, returned from a previous call,
          identifying the result set.
      :param start_index: zero based index of the starting row to read.
      :return: list of rows



   
   .. method:: list_rows(self, dataset_id: str, table_id: str, max_results: Optional[int] = None, selected_fields: Optional[Union[List[str], str]] = None, page_token: Optional[str] = None, start_index: Optional[int] = None, project_id: Optional[str] = None, location: Optional[str] = None)

      List the rows of the table.
      See https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list

      :param dataset_id: the dataset ID of the requested table.
      :param table_id: the table ID of the requested table.
      :param max_results: the maximum results to return.
      :param selected_fields: List of fields to return (comma-separated). If
          unspecified, all fields are returned.
      :param page_token: page token, returned from a previous call,
          identifying the result set.
      :param start_index: zero based index of the starting row to read.
      :param project_id: Project ID for the project which the client acts on behalf of.
      :param location: Default location for job.
      :return: list of rows



   
   .. method:: get_schema(self, dataset_id: str, table_id: str, project_id: Optional[str] = None)

      Get the schema for a given dataset and table.
      see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource

      :param dataset_id: the dataset ID of the requested table
      :param table_id: the table ID of the requested table
      :param project_id: the optional project ID of the requested table.
              If not provided, the connector's configured project will be used.
      :return: a table schema



   
   .. method:: poll_job_complete(self, job_id: str, project_id: Optional[str] = None, location: Optional[str] = None, retry: Retry = DEFAULT_RETRY)

      Check if jobs completed.

      :param job_id: id of the job.
      :type job_id: str
      :param project_id: Google Cloud Project where the job is running
      :type project_id: str
      :param location: location the job is running
      :type location: str
      :param retry: How to retry the RPC.
      :type retry: google.api_core.retry.Retry
      :rtype: bool



   
   .. method:: cancel_query(self)

      Cancel all started queries that have not yet completed



   
   .. method:: cancel_job(self, job_id: str, project_id: Optional[str] = None, location: Optional[str] = None)

      Cancels a job an wait for cancellation to complete

      :param job_id: id of the job.
      :type job_id: str
      :param project_id: Google Cloud Project where the job is running
      :type project_id: str
      :param location: location the job is running
      :type location: str



   
   .. method:: get_job(self, job_id: Optional[str] = None, project_id: Optional[str] = None, location: Optional[str] = None)

      Retrieves a BigQuery job. For more information see:
      https://cloud.google.com/bigquery/docs/reference/v2/jobs

      :param job_id: The ID of the job. The ID must contain only letters (a-z, A-Z),
          numbers (0-9), underscores (_), or dashes (-). The maximum length is 1,024
          characters. If not provided then uuid will be generated.
      :type job_id: str
      :param project_id: Google Cloud Project where the job is running
      :type project_id: str
      :param location: location the job is running
      :type location: str



   
   .. staticmethod:: _custom_job_id(configuration: Dict[str, Any])



   
   .. method:: insert_job(self, configuration: Dict, job_id: Optional[str] = None, project_id: Optional[str] = None, location: Optional[str] = None)

      Executes a BigQuery job. Waits for the job to complete and returns job id.
      See here:

      https://cloud.google.com/bigquery/docs/reference/v2/jobs

      :param configuration: The configuration parameter maps directly to
          BigQuery's configuration field in the job object. See
          https://cloud.google.com/bigquery/docs/reference/v2/jobs for
          details.
      :type configuration: Dict[str, Any]
      :param job_id: The ID of the job. The ID must contain only letters (a-z, A-Z),
          numbers (0-9), underscores (_), or dashes (-). The maximum length is 1,024
          characters. If not provided then uuid will be generated.
      :type job_id: str
      :param project_id: Google Cloud Project where the job is running
      :type project_id: str
      :param location: location the job is running
      :type location: str



   
   .. method:: run_with_configuration(self, configuration: dict)

      Executes a BigQuery SQL query. See here:

      https://cloud.google.com/bigquery/docs/reference/v2/jobs

      For more details about the configuration parameter.

      :param configuration: The configuration parameter maps directly to
          BigQuery's configuration field in the job object. See
          https://cloud.google.com/bigquery/docs/reference/v2/jobs for
          details.



   
   .. method:: run_load(self, destination_project_dataset_table: str, source_uris: List, schema_fields: Optional[List] = None, source_format: str = 'CSV', create_disposition: str = 'CREATE_IF_NEEDED', skip_leading_rows: int = 0, write_disposition: str = 'WRITE_EMPTY', field_delimiter: str = ',', max_bad_records: int = 0, quote_character: Optional[str] = None, ignore_unknown_values: bool = False, allow_quoted_newlines: bool = False, allow_jagged_rows: bool = False, encoding: str = 'UTF-8', schema_update_options: Optional[Iterable] = None, src_fmt_configs: Optional[Dict] = None, time_partitioning: Optional[Dict] = None, cluster_fields: Optional[List] = None, autodetect: bool = False, encryption_configuration: Optional[Dict] = None)

      Executes a BigQuery load command to load data from Google Cloud Storage
      to BigQuery. See here:

      https://cloud.google.com/bigquery/docs/reference/v2/jobs

      For more details about these parameters.

      :param destination_project_dataset_table:
          The dotted ``(<project>.|<project>:)<dataset>.<table>($<partition>)`` BigQuery
          table to load data into. If ``<project>`` is not included, project will be the
          project defined in the connection json. If a partition is specified the
          operator will automatically append the data, create a new partition or create
          a new DAY partitioned table.
      :type destination_project_dataset_table: str
      :param schema_fields: The schema field list as defined here:
          https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
          Required if autodetect=False; optional if autodetect=True.
      :type schema_fields: list
      :param autodetect: Attempt to autodetect the schema for CSV and JSON
          source files.
      :type autodetect: bool
      :param source_uris: The source Google Cloud
          Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
          per-object name can be used.
      :type source_uris: list
      :param source_format: File format to export.
      :type source_format: str
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
      :param quote_character: The value that is used to quote data sections in a CSV
          file.
      :type quote_character: str
      :param ignore_unknown_values: [Optional] Indicates if BigQuery should allow
          extra values that are not represented in the table schema.
          If true, the extra values are ignored. If false, records with extra columns
          are treated as bad records, and if there are too many bad records, an
          invalid error is returned in the job result.
      :type ignore_unknown_values: bool
      :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not
          (false).
      :type allow_quoted_newlines: bool
      :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
          The missing values are treated as nulls. If false, records with missing
          trailing columns are treated as bad records, and if there are too many bad
          records, an invalid error is returned in the job result. Only applicable when
          source_format is CSV.
      :type allow_jagged_rows: bool
      :param encoding: The character encoding of the data.

          .. seealso::
              https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.csvOptions.encoding
      :type encoding: str
      :param schema_update_options: Allows the schema of the destination
          table to be updated as a side effect of the load job.
      :type schema_update_options: Union[list, tuple, set]
      :param src_fmt_configs: configure optional fields specific to the source format
      :type src_fmt_configs: dict
      :param time_partitioning: configure optional time partitioning fields i.e.
          partition by field, type and  expiration as per API specifications.
      :type time_partitioning: dict
      :param cluster_fields: Request that the result of this load be stored sorted
          by one or more columns. BigQuery supports clustering for both partitioned and
          non-partitioned tables. The order of columns given determines the sort order.
      :type cluster_fields: list[str]
      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::

              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict



   
   .. method:: run_copy(self, source_project_dataset_tables: Union[List, str], destination_project_dataset_table: str, write_disposition: str = 'WRITE_EMPTY', create_disposition: str = 'CREATE_IF_NEEDED', labels: Optional[Dict] = None, encryption_configuration: Optional[Dict] = None)

      Executes a BigQuery copy command to copy data from one BigQuery table
      to another. See here:

      https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

      For more details about these parameters.

      :param source_project_dataset_tables: One or more dotted
          ``(project:|project.)<dataset>.<table>``
          BigQuery tables to use as the source data. Use a list if there are
          multiple source tables.
          If ``<project>`` is not included, project will be the project defined
          in the connection json.
      :type source_project_dataset_tables: list|string
      :param destination_project_dataset_table: The destination BigQuery
          table. Format is: ``(project:|project.)<dataset>.<table>``
      :type destination_project_dataset_table: str
      :param write_disposition: The write disposition if the table already exists.
      :type write_disposition: str
      :param create_disposition: The create disposition if the table doesn't exist.
      :type create_disposition: str
      :param labels: a dictionary containing labels for the job/query,
          passed to BigQuery
      :type labels: dict
      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::

              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict



   
   .. method:: run_extract(self, source_project_dataset_table: str, destination_cloud_storage_uris: str, compression: str = 'NONE', export_format: str = 'CSV', field_delimiter: str = ',', print_header: bool = True, labels: Optional[Dict] = None)

      Executes a BigQuery extract command to copy data from BigQuery to
      Google Cloud Storage. See here:

      https://cloud.google.com/bigquery/docs/reference/v2/jobs

      For more details about these parameters.

      :param source_project_dataset_table: The dotted ``<dataset>.<table>``
          BigQuery table to use as the source data.
      :type source_project_dataset_table: str
      :param destination_cloud_storage_uris: The destination Google Cloud
          Storage URI (e.g. gs://some-bucket/some-file.txt). Follows
          convention defined here:
          https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
      :type destination_cloud_storage_uris: list
      :param compression: Type of compression to use.
      :type compression: str
      :param export_format: File format to export.
      :type export_format: str
      :param field_delimiter: The delimiter to use when extracting to a CSV.
      :type field_delimiter: str
      :param print_header: Whether to print a header for a CSV file extract.
      :type print_header: bool
      :param labels: a dictionary containing labels for the job/query,
          passed to BigQuery
      :type labels: dict



   
   .. method:: run_query(self, sql: str, destination_dataset_table: Optional[str] = None, write_disposition: str = 'WRITE_EMPTY', allow_large_results: bool = False, flatten_results: Optional[bool] = None, udf_config: Optional[List] = None, use_legacy_sql: Optional[bool] = None, maximum_billing_tier: Optional[int] = None, maximum_bytes_billed: Optional[float] = None, create_disposition: str = 'CREATE_IF_NEEDED', query_params: Optional[List] = None, labels: Optional[Dict] = None, schema_update_options: Optional[Iterable] = None, priority: str = 'INTERACTIVE', time_partitioning: Optional[Dict] = None, api_resource_configs: Optional[Dict] = None, cluster_fields: Optional[List[str]] = None, location: Optional[str] = None, encryption_configuration: Optional[Dict] = None)

      Executes a BigQuery SQL query. Optionally persists results in a BigQuery
      table. See here:

      https://cloud.google.com/bigquery/docs/reference/v2/jobs

      For more details about these parameters.

      :param sql: The BigQuery SQL to execute.
      :type sql: str
      :param destination_dataset_table: The dotted ``<dataset>.<table>``
          BigQuery table to save the query results.
      :type destination_dataset_table: str
      :param write_disposition: What to do if the table already exists in
          BigQuery.
      :type write_disposition: str
      :param allow_large_results: Whether to allow large results.
      :type allow_large_results: bool
      :param flatten_results: If true and query uses legacy SQL dialect, flattens
          all nested and repeated fields in the query results. ``allowLargeResults``
          must be true if this is set to false. For standard SQL queries, this
          flag is ignored and results are never flattened.
      :type flatten_results: bool
      :param udf_config: The User Defined Function configuration for the query.
          See https://cloud.google.com/bigquery/user-defined-functions for details.
      :type udf_config: list
      :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
          If `None`, defaults to `self.use_legacy_sql`.
      :type use_legacy_sql: bool
      :param api_resource_configs: a dictionary that contain params
          'configuration' applied for Google BigQuery Jobs API:
          https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
          for example, {'query': {'useQueryCache': False}}. You could use it
          if you need to provide some params that are not supported by the
          BigQueryHook like args.
      :type api_resource_configs: dict
      :param maximum_billing_tier: Positive integer that serves as a
          multiplier of the basic price.
      :type maximum_billing_tier: int
      :param maximum_bytes_billed: Limits the bytes billed for this job.
          Queries that will have bytes billed beyond this limit will fail
          (without incurring a charge). If unspecified, this will be
          set to your project default.
      :type maximum_bytes_billed: float
      :param create_disposition: Specifies whether the job is allowed to
          create new tables.
      :type create_disposition: str
      :param query_params: a list of dictionary containing query parameter types and
          values, passed to BigQuery
      :type query_params: list
      :param labels: a dictionary containing labels for the job/query,
          passed to BigQuery
      :type labels: dict
      :param schema_update_options: Allows the schema of the destination
          table to be updated as a side effect of the query job.
      :type schema_update_options: Union[list, tuple, set]
      :param priority: Specifies a priority for the query.
          Possible values include INTERACTIVE and BATCH.
          The default value is INTERACTIVE.
      :type priority: str
      :param time_partitioning: configure optional time partitioning fields i.e.
          partition by field, type and expiration as per API specifications.
      :type time_partitioning: dict
      :param cluster_fields: Request that the result of this query be stored sorted
          by one or more columns. BigQuery supports clustering for both partitioned and
          non-partitioned tables. The order of columns given determines the sort order.
      :type cluster_fields: list[str]
      :param location: The geographic location of the job. Required except for
          US and EU. See details at
          https://cloud.google.com/bigquery/docs/locations#specifying_your_location
      :type location: str
      :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
          **Example**: ::

              encryption_configuration = {
                  "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
              }
      :type encryption_configuration: dict




.. py:class:: BigQueryPandasConnector(project_id: str, service: str, reauth: bool = False, verbose: bool = False, dialect='legacy')

   Bases: :class:`pandas_gbq.gbq.GbqConnector`

   This connector behaves identically to GbqConnector (from Pandas), except
   that it allows the service to be injected, and disables a call to
   self.get_credentials(). This allows Airflow to use BigQuery with Pandas
   without forcing a three legged OAuth connection. Instead, we can inject
   service account credentials into the binding.


.. py:class:: BigQueryConnection(*args, **kwargs)

   BigQuery does not have a notion of a persistent connection. Thus, these
   objects are small stateless factories for cursors, which do all the real
   work.

   
   .. method:: close(self)

      BigQueryConnection does not have anything to close



   
   .. method:: commit(self)

      BigQueryConnection does not support transactions



   
   .. method:: cursor(self)

      Return a new :py:class:`Cursor` object using the connection



   
   .. method:: rollback(self)

      BigQueryConnection does not have transactions




.. py:class:: BigQueryBaseCursor(service: Any, project_id: str, hook: BigQueryHook, use_legacy_sql: bool = True, api_resource_configs: Optional[Dict] = None, location: Optional[str] = None, num_retries: int = 5)

   Bases: :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   The BigQuery base cursor contains helper methods to execute queries against
   BigQuery. The methods can be used directly by operators, in cases where a
   PEP 249 cursor isn't needed.

   
   .. method:: create_empty_table(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_table`



   
   .. method:: create_empty_dataset(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_empty_dataset`



   
   .. method:: get_dataset_tables(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables`



   
   .. method:: delete_dataset(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.delete_dataset`



   
   .. method:: create_external_table(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_external_table`



   
   .. method:: patch_table(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_table`



   
   .. method:: insert_all(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_all`



   
   .. method:: update_dataset(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset`



   
   .. method:: patch_dataset(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.patch_dataset`



   
   .. method:: get_dataset_tables_list(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables_list`



   
   .. method:: get_datasets_list(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_datasets_list`



   
   .. method:: get_dataset(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset`



   
   .. method:: run_grant_dataset_view_access(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_grant_dataset_view_access`



   
   .. method:: run_table_upsert(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_upsert`



   
   .. method:: run_table_delete(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_table_delete`



   
   .. method:: get_tabledata(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_tabledata`



   
   .. method:: get_schema(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema`



   
   .. method:: poll_job_complete(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete`



   
   .. method:: cancel_query(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.cancel_query`



   
   .. method:: run_with_configuration(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration`



   
   .. method:: run_load(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_load`



   
   .. method:: run_copy(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_copy`



   
   .. method:: run_extract(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_extract`



   
   .. method:: run_query(self, *args, **kwargs)

      This method is deprecated.
      Please use `airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_query`




.. py:class:: BigQueryCursor(service: Any, project_id: str, hook: BigQueryHook, use_legacy_sql: bool = True, location: Optional[str] = None, num_retries: int = 5)

   Bases: :class:`airflow.providers.google.cloud.hooks.bigquery.BigQueryBaseCursor`

   A very basic BigQuery PEP 249 cursor implementation. The PyHive PEP 249
   implementation was used as a reference:

   https://github.com/dropbox/PyHive/blob/master/pyhive/presto.py
   https://github.com/dropbox/PyHive/blob/master/pyhive/common.py

   .. attribute:: description
      

      The schema description method is not currently implemented


   .. attribute:: rowcount
      

      By default, return -1 to indicate that this is not supported


   .. attribute:: arraysize
      

      

   
   .. method:: close(self)

      By default, do nothing



   
   .. method:: execute(self, operation: str, parameters: Optional[dict] = None)

      Executes a BigQuery query, and returns the job ID.

      :param operation: The query to execute.
      :type operation: str
      :param parameters: Parameters to substitute into the query.
      :type parameters: dict



   
   .. method:: executemany(self, operation: str, seq_of_parameters: list)

      Execute a BigQuery query multiple times with different parameters.

      :param operation: The query to execute.
      :type operation: str
      :param seq_of_parameters: List of dictionary parameters to substitute into the
          query.
      :type seq_of_parameters: list



   
   .. method:: flush_results(self)

      Flush results related cursor attributes



   
   .. method:: fetchone(self)

      Fetch the next row of a query result set



   
   .. method:: next(self)

      Helper method for fetchone, which returns the next row from a buffer.
      If the buffer is empty, attempts to paginate through the result set for
      the next page, and load it into the buffer.



   
   .. method:: fetchmany(self, size: Optional[int] = None)

      Fetch the next set of rows of a query result, returning a sequence of sequences
      (e.g. a list of tuples). An empty sequence is returned when no more rows are
      available. The number of rows to fetch per call is specified by the parameter.
      If it is not given, the cursor's arraysize determines the number of rows to be
      fetched. The method should try to fetch as many rows as indicated by the size
      parameter. If this is not possible due to the specified number of rows not being
      available, fewer rows may be returned. An :py:class:`~pyhive.exc.Error`
      (or subclass) exception is raised if the previous call to
      :py:meth:`execute` did not produce any result set or no call was issued yet.



   
   .. method:: fetchall(self)

      Fetch all (remaining) rows of a query result, returning them as a sequence of
      sequences (e.g. a list of tuples).



   
   .. method:: get_arraysize(self)

      Specifies the number of rows to fetch at a time with .fetchmany()



   
   .. method:: set_arraysize(self, arraysize: int)

      Specifies the number of rows to fetch at a time with .fetchmany()



   
   .. method:: setinputsizes(self, sizes: Any)

      Does nothing by default



   
   .. method:: setoutputsize(self, size: Any, column: Any = None)

      Does nothing by default




.. function:: _bind_parameters(operation: str, parameters: dict) -> str
   Helper method that binds parameters to a SQL query


.. function:: _escape(s: str) -> str
   Helper method that escapes parameters to a SQL query


.. function:: _bq_cast(string_field: str, bq_type: str) -> Union[None, int, float, bool, str]
   Helper method that casts a BigQuery row to the appropriate data types.
   This is useful because BigQuery returns all fields as strings.


.. function:: _split_tablename(table_input: str, default_project_id: str, var_name: Optional[str] = None) -> Tuple[str, str, str]

.. function:: _cleanse_time_partitioning(destination_dataset_table: Optional[str], time_partitioning_in: Optional[Dict]) -> Dict

.. function:: _validate_value(key: Any, value: Any, expected_type: Type) -> None
   Function to check expected type and raise error if type is not correct


.. function:: _api_resource_configs_duplication_check(key: Any, value: Any, config_dict: dict, config_dict_name='api_resource_configs') -> None

.. function:: _validate_src_fmt_configs(source_format: str, src_fmt_configs: dict, valid_configs: List[str], backward_compatibility_configs: Optional[Dict] = None) -> Dict
   Validates the given src_fmt_configs against a valid configuration for the source format.
   Adds the backward compatibility config to the src_fmt_configs.

   :param source_format: File format to export.
   :type source_format: str
   :param src_fmt_configs: Configure optional fields specific to the source format.
   :type src_fmt_configs: dict
   :param valid_configs: Valid configuration specific to the source format
   :type valid_configs: List[str]
   :param backward_compatibility_configs: The top-level params for backward-compatibility
   :type backward_compatibility_configs: dict


