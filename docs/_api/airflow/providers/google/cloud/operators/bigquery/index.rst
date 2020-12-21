:mod:`airflow.providers.google.cloud.operators.bigquery`
========================================================

.. py:module:: airflow.providers.google.cloud.operators.bigquery

.. autoapi-nested-parse::

   This module contains Google BigQuery operators.



Module Contents
---------------

.. data:: BIGQUERY_JOB_DETAILS_LINK_FMT
   :annotation: = https://console.cloud.google.com/bigquery?j={job_id}

   

.. data:: _DEPRECATION_MSG
   :annotation: = The bigquery_conn_id parameter has been deprecated. You should pass the gcp_conn_id parameter.

   

.. py:class:: BigQueryUIColors

   Bases: :class:`enum.Enum`

   Hex colors for BigQuery operators

   .. attribute:: CHECK
      :annotation: = #C0D7FF

      

   .. attribute:: QUERY
      :annotation: = #A1BBFF

      

   .. attribute:: TABLE
      :annotation: = #81A0FF

      

   .. attribute:: DATASET
      :annotation: = #5F86FF

      


.. py:class:: BigQueryConsoleLink

   Bases: :class:`airflow.models.BaseOperatorLink`

   Helper class for constructing BigQuery link.

   .. attribute:: name
      :annotation: = BigQuery Console

      

   
   .. method:: get_link(self, operator, dttm)




.. py:class:: BigQueryConsoleIndexableLink

   Bases: :class:`airflow.models.BaseOperatorLink`

   Helper class for constructing BigQuery link.

   .. attribute:: index
      :annotation: :int

      

   .. attribute:: name
      

      

   
   .. method:: get_link(self, operator: BaseOperator, dttm: datetime)




.. py:class:: BigQueryCheckOperator(*, sql: str, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, use_legacy_sql: bool = True, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.operators.check_operator.CheckOperator`

   Performs checks against BigQuery. The ``BigQueryCheckOperator`` expects
   a sql query that will return a single row. Each value on that
   first row is evaluated using python ``bool`` casting. If any of the
   values return ``False`` the check is failed and errors out.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryCheckOperator`

   Note that Python bool casting evals the following as ``False``:

   * ``False``
   * ``0``
   * Empty string (``""``)
   * Empty list (``[]``)
   * Empty dictionary or set (``{}``)

   Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
   the count ``== 0``. You can craft much more complex query that could,
   for instance, check that the table has the same number of rows as
   the source table upstream, or that the count of today's partition is
   greater than yesterday's partition, or that a set of metrics are less
   than 3 standard deviation for the 7 day average.

   This operator can be used as a data quality check in your pipeline, and
   depending on where you put it in your DAG, you have the choice to
   stop the critical path, preventing from
   publishing dubious data, or on the side and receive email alerts
   without stopping the progress of the DAG.

   :param sql: the sql to be executed
   :type sql: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param use_legacy_sql: Whether to use legacy SQL (true)
       or standard SQL (false).
   :type use_legacy_sql: bool
   :param location: The geographic location of the job. See details at:
       https://cloud.google.com/bigquery/docs/locations#specifying_your_location
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
      :annotation: = ['sql', 'gcp_conn_id', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      

      

   
   .. method:: get_db_hook(self)




.. py:class:: BigQueryValueCheckOperator(*, sql: str, pass_value: Any, tolerance: Any = None, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, use_legacy_sql: bool = True, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.operators.check_operator.ValueCheckOperator`

   Performs a simple value check using sql code.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryValueCheckOperator`

   :param sql: the sql to be executed
   :type sql: str
   :param use_legacy_sql: Whether to use legacy SQL (true)
       or standard SQL (false).
   :type use_legacy_sql: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param location: The geographic location of the job. See details at:
       https://cloud.google.com/bigquery/docs/locations#specifying_your_location
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
      :annotation: = ['sql', 'gcp_conn_id', 'pass_value', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      

      

   
   .. method:: get_db_hook(self)




.. py:class:: BigQueryIntervalCheckOperator(*, table: str, metrics_thresholds: dict, date_filter_column: str = 'ds', days_back: SupportsAbs[int] = -7, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, use_legacy_sql: bool = True, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.operators.check_operator.IntervalCheckOperator`

   Checks that the values of metrics given as SQL expressions are within
   a certain tolerance of the ones from days_back before.

   This method constructs a query like so ::

       SELECT {metrics_threshold_dict_key} FROM {table}
       WHERE {date_filter_column}=<date>

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryIntervalCheckOperator`

   :param table: the table name
   :type table: str
   :param days_back: number of days between ds and the ds we want to check
       against. Defaults to 7 days
   :type days_back: int
   :param metrics_threshold: a dictionary of ratios indexed by metrics, for
       example 'COUNT(*)': 1.5 would require a 50 percent or less difference
       between the current day, and the prior days_back.
   :type metrics_threshold: dict
   :param use_legacy_sql: Whether to use legacy SQL (true)
       or standard SQL (false).
   :type use_legacy_sql: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param location: The geographic location of the job. See details at:
       https://cloud.google.com/bigquery/docs/locations#specifying_your_location
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
      :annotation: = ['table', 'gcp_conn_id', 'sql1', 'sql2', 'impersonation_chain']

      

   .. attribute:: ui_color
      

      

   
   .. method:: get_db_hook(self)




.. py:class:: BigQueryGetDataOperator(*, dataset_id: str, table_id: str, max_results: int = 100, selected_fields: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
   and returns data in a python list. The number of elements in the returned list will
   be equal to the number of rows fetched. Each element in the list will again be a list
   where element would represent the columns values for that row.

   **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryGetDataOperator`

   .. note::
       If you pass fields to ``selected_fields`` which are in different order than the
       order of columns already in
       BQ table, the data will still be in the order of BQ table.
       For example if the BQ table has 3 columns as
       ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
       the data would still be of the form ``'A,B'``.

   **Example**: ::

       get_data = BigQueryGetDataOperator(
           task_id='get_data_from_bq',
           dataset_id='test_dataset',
           table_id='Transaction_partitions',
           max_results=100,
           selected_fields='DATE',
           gcp_conn_id='airflow-conn-id'
       )

   :param dataset_id: The dataset ID of the requested table. (templated)
   :type dataset_id: str
   :param table_id: The table ID of the requested table. (templated)
   :type table_id: str
   :param max_results: The maximum number of records (rows) to be fetched
       from the table. (templated)
   :type max_results: int
   :param selected_fields: List of fields to return (comma-separated). If
       unspecified, all fields are returned.
   :type selected_fields: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param location: The location used for the operation.
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
      :annotation: = ['dataset_id', 'table_id', 'max_results', 'selected_fields', 'impersonation_chain']

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryExecuteQueryOperator(*, sql: Union[str, Iterable], destination_dataset_table: Optional[str] = None, write_disposition: str = 'WRITE_EMPTY', allow_large_results: Optional[bool] = False, flatten_results: Optional[bool] = None, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, udf_config: Optional[list] = None, use_legacy_sql: bool = True, maximum_billing_tier: Optional[int] = None, maximum_bytes_billed: Optional[float] = None, create_disposition: str = 'CREATE_IF_NEEDED', schema_update_options: Optional[Union[list, tuple, set]] = None, query_params: Optional[list] = None, labels: Optional[dict] = None, priority: str = 'INTERACTIVE', time_partitioning: Optional[dict] = None, api_resource_configs: Optional[dict] = None, cluster_fields: Optional[List[str]] = None, location: Optional[str] = None, encryption_configuration: Optional[dict] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes BigQuery SQL queries in a specific BigQuery database.
   This operator does not assert idempotency.

   :param sql: the sql code to be executed (templated)
   :type sql: Can receive a str representing a sql statement,
       a list of str (sql statements), or reference to a template file.
       Template reference are recognized by str ending in '.sql'.
   :param destination_dataset_table: A dotted
       ``(<project>.|<project>:)<dataset>.<table>`` that, if set, will store the results
       of the query. (templated)
   :type destination_dataset_table: str
   :param write_disposition: Specifies the action that occurs if the destination table
       already exists. (default: 'WRITE_EMPTY')
   :type write_disposition: str
   :param create_disposition: Specifies whether the job is allowed to create new tables.
       (default: 'CREATE_IF_NEEDED')
   :type create_disposition: str
   :param allow_large_results: Whether to allow large results.
   :type allow_large_results: bool
   :param flatten_results: If true and query uses legacy SQL dialect, flattens
       all nested and repeated fields in the query results. ``allow_large_results``
       must be ``true`` if this is set to ``false``. For standard SQL queries, this
       flag is ignored and results are never flattened.
   :type flatten_results: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param udf_config: The User Defined Function configuration for the query.
       See https://cloud.google.com/bigquery/user-defined-functions for details.
   :type udf_config: list
   :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
   :type use_legacy_sql: bool
   :param maximum_billing_tier: Positive integer that serves as a multiplier
       of the basic price.
       Defaults to None, in which case it uses the value set in the project.
   :type maximum_billing_tier: int
   :param maximum_bytes_billed: Limits the bytes billed for this job.
       Queries that will have bytes billed beyond this limit will fail
       (without incurring a charge). If unspecified, this will be
       set to your project default.
   :type maximum_bytes_billed: float
   :param api_resource_configs: a dictionary that contain params
       'configuration' applied for Google BigQuery Jobs API:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
       for example, {'query': {'useQueryCache': False}}. You could use it
       if you need to provide some params that are not supported by BigQueryOperator
       like args.
   :type api_resource_configs: dict
   :param schema_update_options: Allows the schema of the destination
       table to be updated as a side effect of the load job.
   :type schema_update_options: Optional[Union[list, tuple, set]]
   :param query_params: a list of dictionary containing query parameter types and
       values, passed to BigQuery. The structure of dictionary should look like
       'queryParameters' in Google BigQuery Jobs API:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs.
       For example, [{ 'name': 'corpus', 'parameterType': { 'type': 'STRING' },
       'parameterValue': { 'value': 'romeoandjuliet' } }]. (templated)
   :type query_params: list
   :param labels: a dictionary containing labels for the job/query,
       passed to BigQuery
   :type labels: dict
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
      :annotation: = ['sql', 'destination_dataset_table', 'labels', 'query_params', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   .. attribute:: ui_color
      

      

   .. attribute:: operator_extra_links
      

      Return operator extra links


   
   .. method:: execute(self, context)



   
   .. method:: on_kill(self)




.. py:class:: BigQueryCreateEmptyTableOperator(*, dataset_id: str, table_id: str, table_resource: Optional[Dict[str, Any]] = None, project_id: Optional[str] = None, schema_fields: Optional[List] = None, gcs_schema_object: Optional[str] = None, time_partitioning: Optional[Dict] = None, bigquery_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, labels: Optional[Dict] = None, view: Optional[Dict] = None, encryption_configuration: Optional[Dict] = None, location: Optional[str] = None, cluster_fields: Optional[List[str]] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new, empty table in the specified BigQuery dataset,
   optionally with schema.

   The schema to be used for the BigQuery table may be specified in one of
   two ways. You may either directly pass the schema fields in, or you may
   point the operator to a Google Cloud Storage object name. The object in
   Google Cloud Storage must be a JSON file with the schema fields in it.
   You can also create a table without schema.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryCreateEmptyTableOperator`

   :param project_id: The project to create the table into. (templated)
   :type project_id: str
   :param dataset_id: The dataset to create the table into. (templated)
   :type dataset_id: str
   :param table_id: The Name of the table to be created. (templated)
   :type table_id: str
   :param table_resource: Table resource as described in documentation:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
       If provided all other parameters are ignored.
   :type table_resource: Dict[str, Any]
   :param schema_fields: If set, the schema field list as defined here:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

       **Example**: ::

           schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                          {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

   :type schema_fields: list
   :param gcs_schema_object: Full path to the JSON file containing
       schema (templated). For
       example: ``gs://test-bucket/dir1/dir2/employee_schema.json``
   :type gcs_schema_object: str
   :param time_partitioning: configure optional time partitioning fields i.e.
       partition by field, type and  expiration as per API specifications.

       .. seealso::
           https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
   :type time_partitioning: dict
   :param bigquery_conn_id: [Optional] The connection ID used to connect to Google Cloud and
       interact with the Bigquery service.
   :type bigquery_conn_id: str
   :param google_cloud_storage_conn_id: [Optional] The connection ID used to connect to Google Cloud.
       and interact with the Google Cloud Storage service.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param labels: a dictionary containing labels for the table, passed to BigQuery

       **Example (with schema JSON in GCS)**: ::

           CreateTable = BigQueryCreateEmptyTableOperator(
               task_id='BigQueryCreateEmptyTableOperator_task',
               dataset_id='ODS',
               table_id='Employees',
               project_id='internal-gcp-project',
               gcs_schema_object='gs://schema-bucket/employee_schema.json',
               bigquery_conn_id='airflow-conn-id',
               google_cloud_storage_conn_id='airflow-conn-id'
           )

       **Corresponding Schema file** (``employee_schema.json``): ::

           [
             {
               "mode": "NULLABLE",
               "name": "emp_name",
               "type": "STRING"
             },
             {
               "mode": "REQUIRED",
               "name": "salary",
               "type": "INTEGER"
             }
           ]

       **Example (with schema in the DAG)**: ::

           CreateTable = BigQueryCreateEmptyTableOperator(
               task_id='BigQueryCreateEmptyTableOperator_task',
               dataset_id='ODS',
               table_id='Employees',
               project_id='internal-gcp-project',
               schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                              {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}],
               bigquery_conn_id='airflow-conn-id-account',
               google_cloud_storage_conn_id='airflow-conn-id'
           )
   :type labels: dict
   :param view: [Optional] A dictionary containing definition for the view.
       If set, it will create a view instead of a table:

       .. seealso::
           https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#ViewDefinition
   :type view: dict
   :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
       **Example**: ::

           encryption_configuration = {
               "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
           }
   :type encryption_configuration: dict
   :param location: The location used for the operation.
   :type location: str
   :param cluster_fields: [Optional] The fields used for clustering.
           BigQuery supports clustering for both partitioned and
           non-partitioned tables.

           .. seealso::
               https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#clustering.fields
   :type cluster_fields: list
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
      :annotation: = ['dataset_id', 'table_id', 'project_id', 'gcs_schema_object', 'labels', 'view', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryCreateExternalTableOperator(*, bucket: str, source_objects: List, destination_project_dataset_table: str, table_resource: Optional[Dict[str, Any]] = None, schema_fields: Optional[List] = None, schema_object: Optional[str] = None, source_format: str = 'CSV', compression: str = 'NONE', skip_leading_rows: int = 0, field_delimiter: str = ',', max_bad_records: int = 0, quote_character: Optional[str] = None, allow_quoted_newlines: bool = False, allow_jagged_rows: bool = False, bigquery_conn_id: str = 'google_cloud_default', google_cloud_storage_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, src_fmt_configs: Optional[dict] = None, labels: Optional[Dict] = None, encryption_configuration: Optional[Dict] = None, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new external table in the dataset with the data from Google Cloud
   Storage.

   The schema to be used for the BigQuery table may be specified in one of
   two ways. You may either directly pass the schema fields in, or you may
   point the operator to a Google Cloud Storage object name. The object in
   Google Cloud Storage must be a JSON file with the schema fields in it.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryCreateExternalTableOperator`

   :param bucket: The bucket to point the external table to. (templated)
   :type bucket: str
   :param source_objects: List of Google Cloud Storage URIs to point
       table to. If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
   :type source_objects: list
   :param destination_project_dataset_table: The dotted ``(<project>.)<dataset>.<table>``
       BigQuery table to load data into (templated). If ``<project>`` is not included,
       project will be the project defined in the connection json.
   :type destination_project_dataset_table: str
   :param schema_fields: If set, the schema field list as defined here:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema

       **Example**: ::

           schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                          {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

       Should not be set when source_format is 'DATASTORE_BACKUP'.
   :param table_resource: Table resource as described in documentation:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
       If provided all other parameters are ignored. External schema from object will be resolved.
   :type table_resource: Dict[str, Any]
   :type schema_fields: list
   :param schema_object: If set, a GCS object path pointing to a .json file that
       contains the schema for the table. (templated)
   :type schema_object: str
   :param source_format: File format of the data.
   :type source_format: str
   :param compression: [Optional] The compression type of the data source.
       Possible values include GZIP and NONE.
       The default value is NONE.
       This setting is ignored for Google Cloud Bigtable,
       Google Cloud Datastore backups and Avro formats.
   :type compression: str
   :param skip_leading_rows: Number of rows to skip when loading from a CSV.
   :type skip_leading_rows: int
   :param field_delimiter: The delimiter to use for the CSV.
   :type field_delimiter: str
   :param max_bad_records: The maximum number of bad records that BigQuery can
       ignore when running the job.
   :type max_bad_records: int
   :param quote_character: The value that is used to quote data sections in a CSV file.
   :type quote_character: str
   :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
   :type allow_quoted_newlines: bool
   :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
       The missing values are treated as nulls. If false, records with missing trailing
       columns are treated as bad records, and if there are too many bad records, an
       invalid error is returned in the job result. Only applicable to CSV, ignored
       for other formats.
   :type allow_jagged_rows: bool
   :param bigquery_conn_id: (Optional) The connection ID used to connect to Google Cloud and
       interact with the Bigquery service.
   :type bigquery_conn_id: str
   :param google_cloud_storage_conn_id: (Optional) The connection ID used to connect to Google Cloud
       and interact with the Google Cloud Storage service.
   :type google_cloud_storage_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
   :param location: The location used for the operation.
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
      :annotation: = ['bucket', 'source_objects', 'schema_object', 'destination_project_dataset_table', 'labels', 'table_resource', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryDeleteDatasetOperator(*, dataset_id: str, project_id: Optional[str] = None, delete_contents: bool = False, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator deletes an existing dataset from your Project in Big query.
   https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/delete

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryDeleteDatasetOperator`

   :param project_id: The project id of the dataset.
   :type project_id: str
   :param dataset_id: The dataset to be deleted.
   :type dataset_id: str
   :param delete_contents: (Optional) Whether to force the deletion even if the dataset is not empty.
       Will delete all tables (if any) in the dataset if set to True.
       Will raise HttpError 400: "{dataset_id} is still in use" if set to False and dataset is not empty.
       The default value is False.
   :type delete_contents: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
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

   **Example**: ::

       delete_temp_data = BigQueryDeleteDatasetOperator(
           dataset_id='temp-dataset',
           project_id='temp-project',
           delete_contents=True, # Force the deletion of the dataset as well as its tables (if any).
           gcp_conn_id='_my_gcp_conn_',
           task_id='Deletetemp',
           dag=dag)

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'project_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryCreateEmptyDatasetOperator(*, dataset_id: Optional[str] = None, project_id: Optional[str] = None, dataset_reference: Optional[Dict] = None, location: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator is used to create new dataset for your Project in BigQuery.
   https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryCreateEmptyDatasetOperator`

   :param project_id: The name of the project where we want to create the dataset.
   :type project_id: str
   :param dataset_id: The id of dataset. Don't need to provide, if datasetId in dataset_reference.
   :type dataset_id: str
   :param location: The geographic location where the dataset should reside.
   :type location: str
   :param dataset_reference: Dataset reference that could be provided with request body.
       More info:
       https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
   :type dataset_reference: dict
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
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
       **Example**: ::

           create_new_dataset = BigQueryCreateEmptyDatasetOperator(
               dataset_id='new-dataset',
               project_id='my-project',
               dataset_reference={"friendlyName": "New Dataset"}
               gcp_conn_id='_my_gcp_conn_',
               task_id='newDatasetCreator',
               dag=dag)

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'project_id', 'dataset_reference', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryGetDatasetOperator(*, dataset_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator is used to return the dataset specified by dataset_id.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryGetDatasetOperator`

   :param dataset_id: The id of dataset. Don't need to provide,
       if datasetId in dataset_reference.
   :type dataset_id: str
   :param project_id: The name of the project where we want to create the dataset.
       Don't need to provide, if projectId in dataset_reference.
   :type project_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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

   :rtype: dataset
       https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'project_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryGetDatasetTablesOperator(*, dataset_id: str, project_id: Optional[str] = None, max_results: Optional[int] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator retrieves the list of tables in the specified dataset.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryGetDatasetTablesOperator`

   :param dataset_id: the dataset ID of the requested dataset.
   :type dataset_id: str
   :param project_id: (Optional) the project of the requested dataset. If None,
       self.project_id will be used.
   :type project_id: str
   :param max_results: (Optional) the maximum number of tables to return.
   :type max_results: int
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
      :annotation: = ['dataset_id', 'project_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryPatchDatasetOperator(*, dataset_id: str, dataset_resource: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator is used to patch dataset for your Project in BigQuery.
   It only replaces fields that are provided in the submitted dataset resource.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryPatchDatasetOperator`

   :param dataset_id: The id of dataset. Don't need to provide,
       if datasetId in dataset_reference.
   :type dataset_id: str
   :param dataset_resource: Dataset resource that will be provided with request body.
       https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
   :type dataset_resource: dict
   :param project_id: The name of the project where we want to create the dataset.
       Don't need to provide, if projectId in dataset_reference.
   :type project_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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

   :rtype: dataset
       https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'project_id', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryUpdateDatasetOperator(*, dataset_resource: dict, fields: Optional[List[str]] = None, dataset_id: Optional[str] = None, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator is used to update dataset for your Project in BigQuery.
   Use ``fields`` to specify which fields of dataset to update. If a field
   is listed in ``fields`` and is ``None`` in dataset, it will be deleted.
   If no ``fields`` are provided then all fields of provided ``dataset_resource``
   will be used.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryUpdateDatasetOperator`

   :param dataset_id: The id of dataset. Don't need to provide,
       if datasetId in dataset_reference.
   :type dataset_id: str
   :param dataset_resource: Dataset resource that will be provided with request body.
       https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
   :type dataset_resource: dict
   :param fields: The properties of dataset to change (e.g. "friendly_name").
   :type fields: Sequence[str]
   :param project_id: The name of the project where we want to create the dataset.
       Don't need to provide, if projectId in dataset_reference.
   :type project_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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

   :rtype: dataset
       https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource

   .. attribute:: template_fields
      :annotation: = ['dataset_id', 'project_id', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryDeleteTableOperator(*, deletion_dataset_table: str, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, ignore_if_missing: bool = False, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes BigQuery tables

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryDeleteTableOperator`

   :param deletion_dataset_table: A dotted
       ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
       will be deleted. (templated)
   :type deletion_dataset_table: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param ignore_if_missing: if True, then return success even if the
       requested table does not exist.
   :type ignore_if_missing: bool
   :param location: The location used for the operation.
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
      :annotation: = ['deletion_dataset_table', 'impersonation_chain']

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryUpsertTableOperator(*, dataset_id: str, table_resource: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', bigquery_conn_id: Optional[str] = None, delegate_to: Optional[str] = None, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Upsert BigQuery table

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryUpsertTableOperator`

   :param dataset_id: A dotted
       ``(<project>.|<project>:)<dataset>`` that indicates which dataset
       will be updated. (templated)
   :type dataset_id: str
   :param table_resource: a table resource. see
       https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
   :type table_resource: dict
   :param project_id: The name of the project where we want to update the dataset.
       Don't need to provide, if projectId in dataset_reference.
   :type project_id: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param delegate_to: The account to impersonate, if any.
       For this to work, the service account making the request must have domain-wide
       delegation enabled.
   :type delegate_to: str
   :param location: The location used for the operation.
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
      :annotation: = ['dataset_id', 'table_resource', 'impersonation_chain']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context)




.. py:class:: BigQueryInsertJobOperator(configuration: Dict[str, Any], project_id: Optional[str] = None, location: Optional[str] = None, job_id: Optional[str] = None, force_rerun: bool = True, reattach_states: Optional[Set[str]] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, cancel_on_kill: bool = True, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Executes a BigQuery job. Waits for the job to complete and returns job id.
   This operator work in the following way:

   - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
   - creates ``job_id`` in form of
       ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
   - submits a BigQuery job using the ``job_id``
   - if job with given id already exists then it tries to reattach to the job if its not done and its
       state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

   Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

   For job definition see here:

       https://cloud.google.com/bigquery/docs/reference/v2/jobs

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryInsertJobOperator`


   :param configuration: The configuration parameter maps directly to BigQuery's
       configuration field in the job  object. For more details see
       https://cloud.google.com/bigquery/docs/reference/v2/jobs
   :type configuration: Dict[str, Any]
   :param job_id: The ID of the job. It will be suffixed with hash of job configuration
       unless ``force_rerun`` is True.
       The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
       dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
       be generated.
   :type job_id: str
   :param force_rerun: If True then operator will use hash of uuid as job id suffix
   :type force_rerun: bool
   :param reattach_states: Set of BigQuery job's states in case of which we should reattach
       to the job. Should be other than final states.
   :param project_id: Google Cloud Project where the job is running
   :type project_id: str
   :param location: location the job is running
   :type location: str
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
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
   :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
   :type cancel_on_kill: bool

   .. attribute:: template_fields
      :annotation: = ['configuration', 'job_id', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   .. attribute:: template_fields_renderers
      

      

   .. attribute:: ui_color
      

      

   
   .. method:: prepare_template(self)



   
   .. method:: _submit_job(self, hook: BigQueryHook, job_id: str)



   
   .. staticmethod:: _handle_job_error(job: BigQueryJob)



   
   .. method:: _job_id(self, context)



   
   .. method:: execute(self, context: Any)



   
   .. method:: on_kill(self)




