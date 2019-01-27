# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains a BigQuery Hook, as well as a very basic PEP 249
implementation for BigQuery.
"""

import six

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.utils.log.logging_mixin import LoggingMixin

from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.api_core.exceptions import NotFound
from pandas_gbq.gbq import \
    _check_google_client_version as gbq_check_google_client_version
from pandas_gbq import read_gbq
from pandas_gbq.gbq import \
    _test_google_api_imports as gbq_test_google_api_imports
from pandas_gbq.gbq import GbqConnector


class BigQueryHook(GoogleCloudBaseHook, DbApiHook, LoggingMixin):
    """
    Interact with BigQuery. This hook uses the Google Cloud Platform
    connection.
    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 use_legacy_sql=False,
                 location=None):
        super(BigQueryHook, self).__init__(
            gcp_conn_id=bigquery_conn_id, delegate_to=delegate_to)
        self.use_legacy_sql = use_legacy_sql
        self.location = location

    def get_client(self, project_id=None):
        project_id = project_id if project_id is not None else self.project_id
        return bigquery.Client(
            project=project_id,
            credentials=self._get_credentials(),
            location=self.location,
        )

    def get_conn(self, project_id=None):
        """
        Returns a BigQuery PEP 249 connection object.
        """
        project_id = project_id if project_id is not None else self.project_id
        return dbapi.Connection(self.get_client(project_id))

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        Insertion is currently unsupported. Theoretically, you could use
        BigQuery's streaming API to insert rows into a table, but this hasn't
        been implemented.
        """
        raise NotImplementedError()

    def get_pandas_df(self, sql, parameters=None, dialect=None):
        """
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
        """
        private_key = self._get_field('key_path', None) or self._get_field('keyfile_dict', None)

        if dialect is None:
            dialect = 'legacy' if self.use_legacy_sql else 'standard'

        return read_gbq(sql,
                        project_id=self._get_field('project'),
                        dialect=dialect,
                        verbose=False,
                        private_key=private_key)

    def table_exists(self, dataset_id, table_id, project_id=None):
        """
        Checks for the existence of a table in Google BigQuery.

        :param dataset_id: The name of the dataset in which to look for the
            table.
        :type dataset_id: str
        :param table_id: The name of the table to check the existence of.
        :type table_id: str
        :param project_id: The Google cloud project in which to look for the
            table. The connection supplied to the hook must provide access to
            the specified project.
        :type project_id: str
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)
        try:
            client.get_table(
                bigquery.TableReference(
                    bigquery.DatasetReference(project_id, dataset_id),
                    table_id,
                )
            )
            return True
        except NotFound:
            return False

    def create_empty_table(self,
                           dataset_id,
                           table_id,
                           project_id=None,
                           schema_fields=None,
                           time_partitioning=None,
                           labels=None,
                           view=None):
        """
        Creates a new, empty table in the dataset.
        To create a view, which is defined by a SQL query, parse a dictionary to 'view' kwarg

        :param dataset_id: The dataset to create the table into.
        :type dataset_id: str
        :param table_id: The Name of the table to be created.
        :type table_id: str
        :param project_id: The project to create the table into.
        :type project_id: str
        :param schema_fields: If set, the schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schema
        :type schema_fields: list
        :param labels: a dictionary containing labels for the table, passed to BigQuery
        :type labels: dict

        **Example**: ::

            schema_fields=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and expiration as per API specifications.

            .. seealso::
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#timePartitioning
        :type time_partitioning: dict
        :param view: [Optional] A dictionary containing definition for the view.
            If set, it will create a view instead of a table:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#view
        :type view: dict

        **Example**: ::

            view = {
                "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 1000",
                "useLegacySql": False
            }

        :return:
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id),
            table_id,
        )
        table = bigquery.Table(table_ref)

        if schema_fields:
            table.fields = schema_fields

        if time_partitioning:
            table.time_partitioning = time_partitioning

        if labels:
            table.labels = labels

        if view:
            table.view_query = view

        self.log.info('Creating Table %s:%s.%s',
                      project_id, dataset_id, table_id)

        client.create_table(table)
        self.log.info('Table created successfully: %s:%s.%s',
                      project_id, dataset_id, table_id)

    def create_external_table(self,
                              external_project_dataset_table,
                              schema_fields,
                              source_uris,
                              source_format='CSV',
                              autodetect=False,
                              compression='NONE',
                              ignore_unknown_values=False,
                              max_bad_records=0,
                              skip_leading_rows=0,
                              field_delimiter=',',
                              quote_character=None,
                              allow_quoted_newlines=False,
                              allow_jagged_rows=False,
                              external_config_options=None,
                              labels=None):
        """
        Creates a new external table in the dataset with the data in Google
        Cloud Storage. See here:

        https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#resource

        for more details about these parameters.

        :param external_project_dataset_table:
            The dotted (<project>.|<project>:)<dataset>.<table>($<partition>) BigQuery
            table name to create external table.
            If <project> is not included, project will be the
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
            soure_format is CSV.
        :type allow_jagged_rows: bool
        :param labels: a dictionary containing labels for the table, passed to BigQuery
        :type labels: dict
        """
        table_ref = _split_tablename(table_input=external_project_dataset_table,
                                     default_project_id=self.project_id,
                                     var_name='external_project_dataset_table')
        table = bigquery.Table(table_ref)

        client = self.get_client(table_ref.project)

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.sourceFormat

        source_format = source_format.upper()
        allowed_formats = [
            "CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "GOOGLE_SHEETS",
            "DATASTORE_BACKUP", "PARQUET"
        ]
        if source_format not in allowed_formats:
            raise ValueError("{0} is not a valid source format. "
                             "Please use one of the following types: {1}"
                             .format(source_format, allowed_formats))

        compression = compression.upper()
        allowed_compressions = ['NONE', 'GZIP']
        if compression not in allowed_compressions:
            raise ValueError("{0} is not a valid compression format. "
                             "Please use one of the following types: {1}"
                             .format(compression, allowed_compressions))

        external_config = bigquery.ExternalConfig(source_format)
        external_config.autodetect = autodetect
        external_config.compression = compression
        external_config.source_uris = source_uris
        external_config.ignore_unknown_values = ignore_unknown_values

        if external_config_options is not None:
            if not isinstance(external_config_options, type(external_config.options)):
                raise AirflowException(
                    'external_config_options must have type {}'.format(
                        type(external_config.options)))
            external_config.options = external_config_options

        if schema_fields:
            external_config.schema = schema_fields

        if max_bad_records:
            external_config.max_bad_records = max_bad_records

        self.log.info('Creating external table: %s', external_project_dataset_table)

        # if following fields are not specified in external_config_options
        # honor the top-level params for backward-compatibility
        if external_config.skip_leading_rows is None:
            external_config.options.skip_leading_rows = skip_leading_rows
        if external_config.field_delimiter is None:
            external_config.field_delimiter = field_delimiter
        if external_config.options.quote_character is None:
            external_config.options.quote_character = quote_character
        if external_config.options.allow_quoted_newlines is None:
            external_config.options.allow_quoted_newlines = allow_quoted_newlines
        if external_config.options.allow_jagged_rows is None:
            external_config.options.allow_jagged_rows = allow_jagged_rows

        if labels:
            table.labels = labels

        client.create_table(table)
        self.log.info('External table created successfully: %s',
                      external_project_dataset_table)

    def patch_table(self,
                    dataset_id,
                    table_id,
                    project_id=None,
                    description=None,
                    expiration_time=None,
                    external_data_configuration=None,
                    friendly_name=None,
                    labels=None,
                    schema=None,
                    time_partitioning=None,
                    view=None,
                    require_partition_filter=None):
        """
        Patch information in an existing table.
        It only updates fileds that are provided in the request object.

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
        :type schema: list

        **Example**: ::

            schema=[{"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"}]

        :param time_partitioning: [Optional] A dictionary containing time-based partitioning
             definition for the table.
        :type time_partitioning: dict
        :param view: [Optional] A dictionary containing definition for the view.
            If set, it will patch a view instead of a table:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#view
        :type view: dict

        **Example**: ::

            view = {
                "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
                "useLegacySql": False
            }

        :param require_partition_filter: [Optional] If true, queries over the this table require a
            partition filter. If false, queries over the table
        :type require_partition_filter: bool

        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id),
            table_id,
        )
        table = bigquery.Table(table_ref)
        fields = []

        if description is not None:
            table.description = description
            fields.append('description')
        if expiration_time is not None:
            table.expires = expiration_time
            fields.append('expires')
        if external_data_configuration:
            table.external_data_configuration = external_data_configuration
            fields.append('external_data_configuration')
        if friendly_name is not None:
            table.friendly_name = friendly_name
            fields.append('friendly_name')
        if labels:
            table.labels = labels
            fields.append('labels')
        if schema:
            table.schema = schema
            fields.append('schema')
        if time_partitioning:
            table.time_partitioning = time_partitioning
            fields.append('time_partitioning')
        if view:
            table.view_query = view
            fields.append('view_query')

        self.log.info('Patching Table %s:%s.%s',
                      project_id, dataset_id, table_id)

        client.update_table(
            table,
            fields=fields,
        )

        self.log.info('Table patched successfully: %s:%s.%s',
                      project_id, dataset_id, table_id)

    def run_query(self,
                  sql,
                  project_id=None,
                  destination_dataset_table=None,
                  write_disposition='WRITE_EMPTY',
                  allow_large_results=False,
                  flatten_results=None,
                  udf_config=None,
                  use_legacy_sql=None,
                  maximum_billing_tier=None,
                  maximum_bytes_billed=None,
                  create_disposition='CREATE_IF_NEEDED',
                  query_params=None,
                  labels=None,
                  schema_update_options=(),
                  priority='INTERACTIVE',
                  time_partitioning=None,
                  cluster_fields=None,
                  job_config=None,
                  location=None):
        """
        Executes a BigQuery SQL query. Optionally persists results in a BigQuery
        table. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param sql: The BigQuery SQL to execute.
        :type sql: str
        :param destination_dataset_table: The dotted <dataset>.<table>
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
        :param query_params a dictionary containing query parameter types and
            values, passed to BigQuery
        :type query_params: dict
        :param labels a dictionary containing labels for the job/query,
            passed to BigQuery
        :type labels: dict
        :param schema_update_options: Allows the schema of the destination
            table to be updated as a side effect of the query job.
        :type schema_update_options: tuple
        :param priority: Specifies a priority for the query.
            Possible values include INTERACTIVE and BATCH.
            The default value is INTERACTIVE.
        :type priority: str
        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and expiration as per API specifications.
        :type time_partitioning: dict
        :param cluster_fields: Request that the result of this query be stored sorted
            by one or more columns. This is only available in combination with
            time_partitioning. The order of columns given determines the sort order.
        :type cluster_fields: list of str
        :param location: The geographic location of the job. Required except for
            US and EU. See details at
            https://cloud.google.com/bigquery/docs/locations#specifying_your_location
        :type location: str
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        job_config = job_config or bigquery.QueryJobConfig()

        # BigQuery also allows you to define how you want a table's schema to change
        # as a side effect of a query job
        # for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.schemaUpdateOptions

        allowed_schema_update_options = [
            'ALLOW_FIELD_ADDITION', "ALLOW_FIELD_RELAXATION"
        ]

        if not set(allowed_schema_update_options
                   ).issuperset(set(schema_update_options)):
            raise ValueError("{0} contains invalid schema update options. "
                             "Please only use one or more of the following "
                             "options: {1}"
                             .format(schema_update_options,
                                     allowed_schema_update_options))

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError("schema_update_options is only "
                                 "allowed if write_disposition is "
                                 "'WRITE_APPEND' or 'WRITE_TRUNCATE'.")

        if destination_dataset_table:
            job_config.destination = _split_tablename(table_input=destination_dataset_table,
                                                      default_project_id=self.project_id)

        query_param_list = [
            (priority, 'priority', 'INTERACTIVE', six.string_types),
            (use_legacy_sql, 'use_legacy_sql', self.use_legacy_sql, bool),
            (query_params, 'query_parameters', None, list),
            (udf_config, 'udf_resources', None, list),
            (maximum_billing_tier, 'maximum_billing_tier', None, int),
            (maximum_bytes_billed, 'maximum_bytes_billed', None, int),
            (time_partitioning, 'time_partitioning', {}, bigquery.TimePartitioning),
            (schema_update_options, 'schema_update_options', None, list),
            (cluster_fields, 'clustering_fields', None, list),
        ]

        for param_tuple in query_param_list:

            param, param_name, param_default, param_type = param_tuple

            if param in [None, {}, ()]:
                param = param_default

            if param not in [None, {}, ()]:
                setattr(job_config, param_name, param)

                # check valid type of provided param,
                # it last step because we can get param from 2 sources,
                # and first of all need to find it

                # _validate_value(param_name, configuration['query'][param_name],
                #                 param_type)

                if param_name == 'destination_table':
                    job_config.allow_large_results = allow_large_results
                    job_config.flatten_results = flatten_results
                    job_config.write_disposition = write_disposition
                    job_config.create_disposition = create_disposition

        if job_config.use_legacy_sql and job_config.query_parameters:
            raise ValueError("Query parameters are not allowed "
                             "when using legacy SQL")

        if labels:
            job_config.labels = labels

        return client.query(
            sql,
            project=project_id,
            location=location,
            job_config=job_config,
        ).result()

    def run_extract(  # noqa
            self,
            source_project_dataset_table,
            destination_cloud_storage_uris,
            compression='NONE',
            export_format='CSV',
            field_delimiter=',',
            print_header=True,
            labels=None,
            location=None):
        """
        Executes a BigQuery extract command to copy data from BigQuery to
        Google Cloud Storage. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param source_project_dataset_table: The dotted <dataset>.<table>
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
        """
        table_ref = _split_tablename(table_input=source_project_dataset_table,
                                     default_project_id=self.project_id,
                                     var_name='source_project_dataset_table')

        client = self.get_client(table_ref.project)

        job_config = bigquery.ExtractJobConfig(
            compression=compression,
            destination_format=export_format,
        )

        if labels:
            job_config.labels = labels

        if export_format == 'CSV':
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            job_config.field_delimiter = field_delimiter
            job_config.print_header = print_header

        return client.extract_table(
            table_ref,
            destination_cloud_storage_uris,
            location=location,
            job_config=job_config,
        ).result()

    def run_copy(self,
                 source_project_dataset_tables,
                 destination_project_dataset_table,
                 project_id=None,
                 location=None,
                 write_disposition='WRITE_EMPTY',
                 create_disposition='CREATE_IF_NEEDED',
                 labels=None):
        """
        Executes a BigQuery copy command to copy data from one BigQuery table
        to another. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

        For more details about these parameters.

        :param source_project_dataset_tables: One or more dotted
            (project:|project.)<dataset>.<table>
            BigQuery tables to use as the source data. Use a list if there are
            multiple source tables.
            If <project> is not included, project will be the project defined
            in the connection json.
        :type source_project_dataset_tables: list|string
        :param destination_project_dataset_table: The destination BigQuery
            table. Format is: (project:|project.)<dataset>.<table>
        :type destination_project_dataset_table: str
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: str
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: str
        :param labels a dictionary containing labels for the job/query,
            passed to BigQuery
        :type labels: dict
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        source_project_dataset_tables = ([
            source_project_dataset_tables
        ] if not isinstance(source_project_dataset_tables, list) else
            source_project_dataset_tables)

        source_table_refs = []
        for source_project_dataset_table in source_project_dataset_tables:
            source_table_ref = _split_tablename(table_input=source_project_dataset_table,
                                                default_project_id=project_id,
                                                var_name='source_project_dataset_table')
            source_table_refs.append(source_table_ref)

        dest_table_ref = _split_tablename(table_input=destination_project_dataset_table,
                                          default_project_id=project_id)

        job_config = bigquery.CopyJobConfig(
            write_disposition=write_disposition,
            create_disposition=create_disposition,
        )

        if labels:
            job_config.labels = labels

        return client.copy_table(
            source_table_refs,
            dest_table_ref,
            project=project_id,
            location=location,
            job_config=job_config,
        ).result()

    def run_load(self,
                 destination_project_dataset_table,
                 source_uris,
                 project_id=None,
                 location=None,
                 schema_fields=None,
                 source_format='CSV',
                 create_disposition='CREATE_IF_NEEDED',
                 skip_leading_rows=0,
                 write_disposition='WRITE_EMPTY',
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 ignore_unknown_values=False,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 schema_update_options=(),
                 job_config=None,
                 time_partitioning=None,
                 cluster_fields=None,
                 autodetect=False):
        """
        Executes a BigQuery load command to load data from Google Cloud Storage
        to BigQuery. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param destination_project_dataset_table:
            The dotted (<project>.|<project>:)<dataset>.<table>($<partition>) BigQuery
            table to load data into. If <project> is not included, project will be the
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
            soure_format is CSV.
        :type allow_jagged_rows: bool
        :param schema_update_options: Allows the schema of the destination
            table to be updated as a side effect of the load job.
        :type schema_update_options: tuple
        :param time_partitioning: configure optional time partitioning fields i.e.
            partition by field, type and  expiration as per API specifications.
        :type time_partitioning: dict
        :param cluster_fields: Request that the result of this load be stored sorted
            by one or more columns. This is only available in combination with
            time_partitioning. The order of columns given determines the sort order.
        :type cluster_fields: list of str
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).sourceFormat

        if schema_fields is None and not autodetect:
            raise ValueError(
                'You must either pass a schema or autodetect=True.')

        source_format = source_format.upper()
        allowed_formats = [
            "CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "GOOGLE_SHEETS",
            "DATASTORE_BACKUP", "PARQUET"
        ]
        if source_format not in allowed_formats:
            raise ValueError("{0} is not a valid source format. "
                             "Please use one of the following types: {1}"
                             .format(source_format, allowed_formats))

        # bigquery also allows you to define how you want a table's schema to change
        # as a side effect of a load
        # for more details:
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schemaUpdateOptions
        allowed_schema_update_options = [
            'ALLOW_FIELD_ADDITION', "ALLOW_FIELD_RELAXATION"
        ]
        if not set(allowed_schema_update_options).issuperset(
                set(schema_update_options)):
            raise ValueError(
                "{0} contains invalid schema update options."
                "Please only use one or more of the following options: {1}"
                .format(schema_update_options, allowed_schema_update_options))

        table_ref = _split_tablename(table_input=destination_project_dataset_table,
                                     default_project_id=project_id,
                                     var_name='destination_project_dataset_table')

        job_config = job_config or bigquery.LoadJobConfig()
        job_config.create_disposition = create_disposition
        job_config.write_disposition = write_disposition
        job_config.autodetect = autodetect
        job_config.source_format = source_format
        job_config.ignore_unknown_values = ignore_unknown_values

        if time_partitioning:
            job_config.time_partitioning = time_partitioning

        if cluster_fields:
            job_config.clustering_fields = cluster_fields

        if schema_fields:
            job_config.schema = schema_fields

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError("schema_update_options is only "
                                 "allowed if write_disposition is "
                                 "'WRITE_APPEND' or 'WRITE_TRUNCATE'.")
            else:
                job_config.schema_update_options = schema_update_options

        if max_bad_records:
            job_config.max_bad_records = max_bad_records

        # if following fields are not specified in job_config,
        # honor the top-level params for backward-compatibility
        if job_config.skip_leading_rows is None:
            job_config.skip_leading_rows = skip_leading_rows
        if job_config.field_delimiter is None:
            job_config.field_delimiter = field_delimiter
        if job_config.ignore_unknown_values is None:
            job_config.ignore_unknown_values = ignore_unknown_values
        if quote_character is not None:
            job_config.quote_character = quote_character
        if allow_quoted_newlines:
            job_config.allow_quoted_newlines = allow_quoted_newlines

        if allow_jagged_rows:
            job_config.allow_jagged_rows = allow_jagged_rows

        return client.load_table_from_uri(
            source_uris,
            table_ref,
            project=project_id,
            location=location,
            job_config=job_config,
        ).result()

    def get_schema(self, dataset_id, table_id, project_id=None):
        """
        Get the schema for a given datset.table.
        see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource

        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :return: a table schema
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id),
            table_id,
        )
        return client.get_table(table_ref).schema

    def get_tabledata(self, dataset_id, table_id, project_id=None,
                      max_results=None, selected_fields=None, page_token=None,
                      start_index=None, page_size=None):
        """
        Get the data of a given dataset.table and optionally with selected columns.
        see https://cloud.google.com/bigquery/docs/reference/v2/tabledata/list

        :param dataset_id: the dataset ID of the requested table.
        :param table_id: the table ID of the requested table.
        :param max_results: the maximum results to return.
        :param selected_fields: List of fields to return (comma-separated). If
            unspecified, all fields are returned.
        :param page_token: page token, returned from a previous call,
            identifying the result set.
        :param page_token: The maximum number of rows in each page of results
            from this request
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id),
            table_id,
        )
        table = bigquery.Table(table_ref)

        return client.list_rows(
            table,
            selected_fields=selected_fields,
            max_results=max_results,
            page_token=page_token,
            start_index=start_index,
            page_size=page_size,
        )

    def run_table_delete(self, deletion_dataset_table,
                         ignore_if_missing=False):
        """
        Delete an existing table from the dataset;
        If the table does not exist, return an error unless ignore_if_missing
        is set to True.

        :param deletion_dataset_table: A dotted
        (<project>.|<project>:)<dataset>.<table> that indicates which table
        will be deleted.
        :type deletion_dataset_table: str
        :param ignore_if_missing: if True, then return success even if the
        requested table does not exist.
        :type ignore_if_missing: bool
        :return:
        """
        table_ref = _split_tablename(table_input=deletion_dataset_table,
                                     default_project_id=self.project_id)

        client = self.get_client(table_ref.project)

        try:
            client.delete_table(table_ref)
            self.log.info('Deleted table %s', table_ref)
        except NotFound:
            if not ignore_if_missing:
                raise
            else:
                self.log.info('Table does not exist. Skipping.')

    def run_table_upsert(self, table, fields, project_id=None):
        """
        creates a new, empty table in the dataset;
        If the table already exists, update the existing table.
        Since BigQuery does not natively allow table upserts, this is not an
        atomic operation.

        :param dataset_id: the dataset to upsert the table into.
        :type dataset_id: str
        :param table_resource: a table resource. see
            https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
        :type table_resource: dict
        :param project_id: the project to upsert the table into. If None,
        project will be self.project_id.
        :return:
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        # Update or create table
        try:
            client.update_table(table, fields)
        except NotFound:
            client.create_table(table)

    def create_empty_dataset(self, dataset_id=None, project_id=None,
                             dataset_ref=None):
        """
        Create a new empty dataset:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert

        :param project_id: The name of the project where we want to create
            an empty a dataset. Don't need to provide, if projectId in dataset_reference.
        :type project_id: str
        :param dataset_id: The id of dataset. Don't need to provide,
            if datasetId in dataset_reference.
        :type dataset_id: str
        :param dataset_reference: Dataset reference that could be provided
            with request body. More info:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        :type dataset_reference: DatasetReference
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        if dataset_ref is None:
            dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.log.info('Creating Dataset: %s in project: %s ', dataset_ref)

        client.create_dataset(dataset_ref)
        self.log.info('Dataset created successfully: %s', dataset_ref)

    def delete_dataset(self, dataset_id, project_id=None):
        """
        Delete a dataset of Big query in your project.
        :param dataset_id: The dataset to be delete.
        :type dataset_id: str
        :param project_id: The name of the project where we have the dataset .
        :type project_id: str
        :return:
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.log.info('Deleting from project: %s  Dataset:%s',
                      project_id, dataset_id)

        client.delete_dataset(dataset_ref)
        self.log.info('Dataset deleted successfully: In project %s '
                      'Dataset %s', project_id, dataset_id)

    def get_dataset(self, dataset_id, project_id=None):
        """
        Method returns dataset_resource if dataset exist
        and raised 404 error if dataset does not exist

        :param dataset_id: The BigQuery Dataset ID
        :type dataset_id: str
        :param project_id: The GCP Project ID
        :type project_id: str
        :return: dataset_resource

            .. seealso::
                For more information, see Dataset Resource content:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

        dataset = client.get_dataset(dataset_ref)
        self.log.info("Dataset Resource: {}".format(dataset))
        return dataset

    def list_datasets(self, project_id=None):
        """
        Method returns full list of BigQuery datasets in the current project

        .. seealso::
            For more information, see:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list

        :param project_id: Google Cloud Project for which you
            try to get all datasets
        :type project_id: str
        :return: datasets_list

            Example of returned datasets_list: ::

                   {
                      "kind":"bigquery#dataset",
                      "location":"US",
                      "id":"your-project:dataset_2_test",
                      "datasetReference":{
                         "projectId":"your-project",
                         "datasetId":"dataset_2_test"
                      }
                   },
                   {
                      "kind":"bigquery#dataset",
                      "location":"US",
                      "id":"your-project:dataset_1_test",
                      "datasetReference":{
                         "projectId":"your-project",
                         "datasetId":"dataset_1_test"
                      }
                   }
                ]
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        datasets = list(client.list_datasets(project_id))
        self.log.info("Datasets List: {}".format(datasets))
        return datasets

    def insert_all(self, dataset_id, table_id, rows,
                   project_id=None, ignore_unknown_values=False,
                   skip_invalid_rows=False, fail_on_error=False):
        """
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
        """
        project_id = project_id if project_id is not None else self.project_id
        client = self.get_client(project_id)

        table_ref = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_id),
            table_id,
        )

        self.log.info('Inserting {} row(s) into Table {}:{}.{}'.format(
            len(rows), project_id,
            dataset_id, table_id))

        table = client.get_table(table_ref)
        errors = client.insert_rows(
            table,
            rows,
            skip_invalid_rows=skip_invalid_rows,
            ignore_unknown_values=ignore_unknown_values,
        )

        if not errors:
            self.log.info('All row(s) inserted successfully: {}:{}.{}'.format(
                project_id, dataset_id, table_id))
        else:
            error_msg = '{} insert error(s) occurred: {}:{}.{}. Details: {}'.format(
                len(errors),
                project_id, dataset_id, table_id, errors)
            if fail_on_error:
                raise AirflowException(
                    'BigQuery job failed. Error was: {}'.format(error_msg)
                )
            self.log.info(error_msg)


class BigQueryPandasConnector(GbqConnector):
    """
    This connector behaves identically to GbqConnector (from Pandas), except
    that it allows the service to be injected, and disables a call to
    self.get_credentials(). This allows Airflow to use BigQuery with Pandas
    without forcing a three legged OAuth connection. Instead, we can inject
    service account credentials into the binding.
    """

    def __init__(self,
                 project_id,
                 service,
                 reauth=False,
                 verbose=False,
                 dialect='legacy'):
        super(BigQueryPandasConnector, self).__init__(project_id)
        gbq_check_google_client_version()
        gbq_test_google_api_imports()
        self.project_id = project_id
        self.reauth = reauth
        self.service = service
        self.verbose = verbose
        self.dialect = dialect


def _split_tablename(table_input, default_project_id, var_name=None):

    if '.' not in table_input:
        raise ValueError(
            'Expected target table name in the format of '
            '<dataset>.<table>. Got: {}'.format(table_input))

    def var_print(var_name):
        if var_name is None:
            return ""
        else:
            return "Format exception for {var}: ".format(var=var_name)

    if table_input.count('.') + table_input.count(':') > 3:
        raise Exception(('{var}Use either : or . to specify project '
                         'got {input}').format(
                             var=var_print(var_name), input=table_input))
    cmpt = table_input.rsplit(':', 1)
    project_id = None
    rest = table_input
    if len(cmpt) == 1:
        project_id = None
        rest = cmpt[0]
    elif len(cmpt) == 2 and cmpt[0].count(':') <= 1:
        if cmpt[-1].count('.') != 2:
            project_id = cmpt[0]
            rest = cmpt[1]
    else:
        raise Exception(('{var}Expect format of (<project:)<dataset>.<table>, '
                         'got {input}').format(
                             var=var_print(var_name), input=table_input))

    cmpt = rest.split('.')
    if len(cmpt) == 3:
        if project_id:
            raise ValueError(
                "{var}Use either : or . to specify project".format(
                    var=var_print(var_name)))
        project_id = cmpt[0]
        dataset_id = cmpt[1]
        table_id = cmpt[2]

    elif len(cmpt) == 2:
        dataset_id = cmpt[0]
        table_id = cmpt[1]
    else:
        raise Exception(
            ('{var}Expect format of (<project.|<project:)<dataset>.<table>, '
             'got {input}').format(var=var_print(var_name), input=table_input))

    if project_id is None:
        if not default_project_id:
            raise ValueError("INTERNAL: No default project is specified")
        if var_name is not None:
            log = LoggingMixin().log
            log.info('Project not included in {var}: {input}; '
                     'using project "{project}"'.format(
                         var=var_name,
                         input=table_input,
                         project=default_project_id))
        project_id = default_project_id

    return bigquery.TableReference(
        bigquery.DatasetReference(project_id, dataset_id),
        table_id,
    )


def _validate_value(key, value, expected_type):
    """ function to check expected type and raise
    error if type is not correct """
    if not isinstance(value, expected_type):
        raise TypeError("{} argument must have a type {} not {}".format(
            key, expected_type, type(value)))
