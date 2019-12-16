# -*- coding: utf-8 -*- # pylint: disable=too-many-lines
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

import time
import warnings
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Mapping, NoReturn, Optional, Tuple, Type, Union

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pandas import DataFrame
from pandas_gbq import read_gbq
from pandas_gbq.gbq import (
    GbqConnector, _check_google_client_version as gbq_check_google_client_version,
    _test_google_api_imports as gbq_test_google_api_imports,
)

from airflow import AirflowException
from airflow.gcp.hooks.base import CloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.utils.log.logging_mixin import LoggingMixin


class BigQueryHook(CloudBaseHook, DbApiHook):
    """
    Interact with BigQuery. This hook uses the Google Cloud Platform
    connection.
    """
    conn_name_attr = 'gcp_conn_id'  # type: str

    def __init__(self,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 use_legacy_sql: bool = True,
                 location: Optional[str] = None,
                 bigquery_conn_id: Optional[str] = None) -> None:
        # To preserve backward compatibility
        # TODO: remove one day
        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=2)
            gcp_conn_id = bigquery_conn_id
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)
        self.use_legacy_sql = use_legacy_sql
        self.location = location

    def get_conn(self) -> "BigQueryConnection":
        """
        Returns a BigQuery PEP 249 connection object.
        """
        service = self.get_service()
        return BigQueryConnection(
            service=service,
            project_id=self.project_id,
            use_legacy_sql=self.use_legacy_sql,
            location=self.location,
            num_retries=self.num_retries
        )

    def get_service(self) -> Any:
        """
        Returns a BigQuery service object.
        """
        http_authorized = self._authorize()
        return build(
            'bigquery', 'v2', http=http_authorized, cache_discovery=False)

    def insert_rows(
        self, table: Any, rows: Any, target_fields: Any = None, commit_every: Any = 1000, replace: Any = False
    ) -> NoReturn:
        """
        Insertion is currently unsupported. Theoretically, you could use
        BigQuery's streaming API to insert rows into a table, but this hasn't
        been implemented.
        """
        raise NotImplementedError()

    def get_pandas_df(
        self, sql: str, parameters: Optional[Union[Iterable, Mapping]] = None, dialect: Optional[str] = None
    ) -> DataFrame:
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
        if dialect is None:
            dialect = 'legacy' if self.use_legacy_sql else 'standard'

        credentials, project_id = self._get_credentials_and_project_id()

        return read_gbq(sql,
                        project_id=project_id,
                        dialect=dialect,
                        verbose=False,
                        credentials=credentials)

    def table_exists(self, project_id: str, dataset_id: str, table_id: str) -> bool:
        """
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
        """
        service = self.get_service()
        try:
            service.tables().get(  # pylint: disable=no-member
                projectId=project_id, datasetId=dataset_id,
                tableId=table_id).execute(num_retries=self.num_retries)
            return True
        except HttpError as e:
            if e.resp['status'] == '404':
                return False
            raise


class BigQueryPandasConnector(GbqConnector):
    """
    This connector behaves identically to GbqConnector (from Pandas), except
    that it allows the service to be injected, and disables a call to
    self.get_credentials(). This allows Airflow to use BigQuery with Pandas
    without forcing a three legged OAuth connection. Instead, we can inject
    service account credentials into the binding.
    """

    def __init__(
        self, project_id: str, service: str, reauth: bool = False, verbose: bool = False, dialect="legacy"
    ) -> None:
        super().__init__(project_id)
        gbq_check_google_client_version()
        gbq_test_google_api_imports()
        self.project_id = project_id
        self.reauth = reauth
        self.service = service
        self.verbose = verbose
        self.dialect = dialect


class BigQueryConnection:
    """
    BigQuery does not have a notion of a persistent connection. Thus, these
    objects are small stateless factories for cursors, which do all the real
    work.
    """

    def __init__(self, *args, **kwargs) -> None:
        self._args = args
        self._kwargs = kwargs

    def close(self) -> None:
        """ BigQueryConnection does not have anything to close. """

    def commit(self) -> None:
        """ BigQueryConnection does not support transactions. """

    def cursor(self) -> "BigQueryCursor":
        """ Return a new :py:class:`Cursor` object using the connection. """
        return BigQueryCursor(*self._args, **self._kwargs)

    def rollback(self) -> NoReturn:
        """ BigQueryConnection does not have transactions """
        raise NotImplementedError(
            "BigQueryConnection does not have transactions")


class BigQueryBaseCursor(LoggingMixin):
    """
    The BigQuery base cursor contains helper methods to execute queries against
    BigQuery. The methods can be used directly by operators, in cases where a
    PEP 249 cursor isn't needed.
    """

    def __init__(self,
                 service: Any,
                 project_id: str,
                 use_legacy_sql: bool = True,
                 api_resource_configs: Optional[Dict] = None,
                 location: Optional[str] = None,
                 num_retries: int = 5) -> None:

        self.service = service
        self.project_id = project_id
        self.use_legacy_sql = use_legacy_sql
        if api_resource_configs:
            _validate_value("api_resource_configs", api_resource_configs, dict)
        self.api_resource_configs = api_resource_configs \
            if api_resource_configs else {}  # type Dict
        self.running_job_id = None  # type: Optional[str]
        self.location = location
        self.num_retries = num_retries

    # pylint: disable=too-many-arguments
    def create_empty_table(self,
                           project_id: str,
                           dataset_id: str,
                           table_id: str,
                           schema_fields: Optional[List] = None,
                           time_partitioning: Optional[Dict] = None,
                           cluster_fields: Optional[List] = None,
                           labels: Optional[Dict] = None,
                           view: Optional[Dict] = None,
                           encryption_configuration: Optional[Dict] = None,
                           num_retries: int = 5) -> None:
        """
        Creates a new, empty table in the dataset.
        To create a view, which is defined by a SQL query, parse a dictionary to 'view' kwarg

        :param project_id: The project to create the table into.
        :type project_id: str
        :param dataset_id: The dataset to create the table into.
        :type dataset_id: str
        :param table_id: The Name of the table to be created.
        :type table_id: str
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
        :param cluster_fields: [Optional] The fields used for clustering.
            Must be specified with time_partitioning, data in the table will be first
            partitioned and subsequently clustered.
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
        :return: None
        """

        project_id = project_id if project_id is not None else self.project_id

        table_resource = {
            'tableReference': {
                'tableId': table_id
            }
        }  # type: Dict[str, Any]

        if self.location:
            table_resource['location'] = self.location

        if schema_fields:
            table_resource['schema'] = {'fields': schema_fields}

        if time_partitioning:
            table_resource['timePartitioning'] = time_partitioning

        if cluster_fields:
            table_resource['clustering'] = {
                'fields': cluster_fields
            }

        if labels:
            table_resource['labels'] = labels

        if view:
            table_resource['view'] = view

        if encryption_configuration:
            table_resource["encryptionConfiguration"] = encryption_configuration

        num_retries = num_retries if num_retries else self.num_retries

        self.service.tables().insert(
            projectId=project_id,
            datasetId=dataset_id,
            body=table_resource).execute(num_retries=num_retries)

    def create_external_table(self,  # pylint: disable=too-many-locals,too-many-arguments
                              external_project_dataset_table: str,
                              schema_fields: List,
                              source_uris: List,
                              source_format: str = 'CSV',
                              autodetect: bool = False,
                              compression: str = 'NONE',
                              ignore_unknown_values: bool = False,
                              max_bad_records: int = 0,
                              skip_leading_rows: int = 0,
                              field_delimiter: str = ',',
                              quote_character: Optional[str] = None,
                              allow_quoted_newlines: bool = False,
                              allow_jagged_rows: bool = False,
                              encoding: str = "UTF-8",
                              src_fmt_configs: Optional[Dict] = None,
                              labels: Optional[Dict] = None,
                              encryption_configuration: Optional[Dict] = None
                              ) -> None:
        """
        Creates a new external table in the dataset with the data in Google
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
            soure_format is CSV.
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
        """

        if src_fmt_configs is None:
            src_fmt_configs = {}
        project_id, dataset_id, external_table_id = \
            _split_tablename(table_input=external_project_dataset_table,
                             default_project_id=self.project_id,
                             var_name='external_project_dataset_table')

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externalDataConfiguration.sourceFormat # noqa # pylint: disable=line-too-long

        source_format = source_format.upper()
        allowed_formats = [
            "CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "GOOGLE_SHEETS",
            "DATASTORE_BACKUP", "PARQUET"
        ]  # type: List[str]
        if source_format not in allowed_formats:
            raise ValueError("{0} is not a valid source format. "
                             "Please use one of the following types: {1}"
                             .format(source_format, allowed_formats))

        compression = compression.upper()
        allowed_compressions = ['NONE', 'GZIP']  # type: List[str]
        if compression not in allowed_compressions:
            raise ValueError("{0} is not a valid compression format. "
                             "Please use one of the following types: {1}"
                             .format(compression, allowed_compressions))

        table_resource = {
            'externalDataConfiguration': {
                'autodetect': autodetect,
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'compression': compression,
                'ignoreUnknownValues': ignore_unknown_values
            },
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': external_table_id,
            }
        }  # type: Dict[str, Any]

        if self.location:
            table_resource['location'] = self.location

        if schema_fields:
            table_resource['externalDataConfiguration'].update({
                'schema': {
                    'fields': schema_fields
                }
            })

        self.log.info('Creating external table: %s', external_project_dataset_table)

        if max_bad_records:
            table_resource['externalDataConfiguration']['maxBadRecords'] = max_bad_records

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {'skipLeadingRows': skip_leading_rows,
                                          'fieldDelimiter': field_delimiter,
                                          'quote': quote_character,
                                          'allowQuotedNewlines': allow_quoted_newlines,
                                          'allowJaggedRows': allow_jagged_rows,
                                          'encoding': encoding}

        src_fmt_to_param_mapping = {
            'CSV': 'csvOptions',
            'GOOGLE_SHEETS': 'googleSheetsOptions'
        }

        src_fmt_to_configs_mapping = {
            'csvOptions': [
                'allowJaggedRows', 'allowQuotedNewlines',
                'fieldDelimiter', 'skipLeadingRows',
                'quote', 'encoding'
            ],
            'googleSheetsOptions': ['skipLeadingRows']
        }

        if source_format in src_fmt_to_param_mapping.keys():
            valid_configs = src_fmt_to_configs_mapping[
                src_fmt_to_param_mapping[source_format]
            ]

            src_fmt_configs = _validate_src_fmt_configs(source_format, src_fmt_configs, valid_configs,
                                                        backward_compatibility_configs)

            table_resource['externalDataConfiguration'][src_fmt_to_param_mapping[
                source_format]] = src_fmt_configs

        if labels:
            table_resource['labels'] = labels

        if encryption_configuration:
            table_resource["encryptionConfiguration"] = encryption_configuration

        self.service.tables().insert(
            projectId=project_id,
            datasetId=dataset_id,
            body=table_resource
        ).execute(num_retries=self.num_retries)

        self.log.info('External table created successfully: %s',
                      external_project_dataset_table)

    def patch_table(self,  # pylint: disable=too-many-arguments
                    dataset_id: str,
                    table_id: str,
                    project_id: Optional[str] = None,
                    description: Optional[str] = None,
                    expiration_time: Optional[int] = None,
                    external_data_configuration: Optional[Dict] = None,
                    friendly_name: Optional[str] = None,
                    labels: Optional[Dict] = None,
                    schema: Optional[List] = None,
                    time_partitioning: Optional[Dict] = None,
                    view: Optional[Dict] = None,
                    require_partition_filter: Optional[bool] = None,
                    encryption_configuration: Optional[Dict] = None) -> None:
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

        """

        project_id = project_id if project_id is not None else self.project_id

        table_resource = {}  # type: Dict[str, Any]

        if description is not None:
            table_resource['description'] = description
        if expiration_time is not None:
            table_resource['expirationTime'] = expiration_time
        if external_data_configuration:
            table_resource['externalDataConfiguration'] = external_data_configuration
        if friendly_name is not None:
            table_resource['friendlyName'] = friendly_name
        if labels:
            table_resource['labels'] = labels
        if schema:
            table_resource['schema'] = {'fields': schema}
        if time_partitioning:
            table_resource['timePartitioning'] = time_partitioning
        if view:
            table_resource['view'] = view
        if require_partition_filter is not None:
            table_resource['requirePartitionFilter'] = require_partition_filter
        if encryption_configuration:
            table_resource["encryptionConfiguration"] = encryption_configuration

        self.log.info('Patching Table %s:%s.%s',
                      project_id, dataset_id, table_id)

        try:
            self.service.tables().patch(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id,
                body=table_resource).execute(num_retries=self.num_retries)

            self.log.info('Table patched successfully: %s:%s.%s',
                          project_id, dataset_id, table_id)

        except HttpError as err:
            raise AirflowException(
                'BigQuery job failed. Error was: {}'.format(err.content)
            )

    # pylint: disable=too-many-locals,too-many-arguments, too-many-branches
    def run_query(self,
                  sql: str,
                  destination_dataset_table: Optional[str] = None,
                  write_disposition: str = 'WRITE_EMPTY',
                  allow_large_results: bool = False,
                  flatten_results: Optional[bool] = None,
                  udf_config: Optional[List] = None,
                  use_legacy_sql: Optional[bool] = None,
                  maximum_billing_tier: Optional[int] = None,
                  maximum_bytes_billed: Optional[float] = None,
                  create_disposition: str = 'CREATE_IF_NEEDED',
                  query_params: Optional[List] = None,
                  labels: Optional[Dict] = None,
                  schema_update_options: Optional[Iterable] = None,
                  priority: str = 'INTERACTIVE',
                  time_partitioning: Optional[Dict] = None,
                  api_resource_configs: Optional[Dict] = None,
                  cluster_fields: Optional[List[str]] = None,
                  location: Optional[str] = None,
                  encryption_configuration: Optional[Dict] = None) -> str:
        """
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
            by one or more columns. This is only available in combination with
            time_partitioning. The order of columns given determines the sort order.
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
        """
        schema_update_options = list(schema_update_options or [])

        if time_partitioning is None:
            time_partitioning = {}

        if location:
            self.location = location

        if not api_resource_configs:
            api_resource_configs = self.api_resource_configs
        else:
            _validate_value('api_resource_configs',
                            api_resource_configs, dict)
        configuration = deepcopy(api_resource_configs)
        if 'query' not in configuration:
            configuration['query'] = {}

        else:
            _validate_value("api_resource_configs['query']",
                            configuration['query'], dict)

        if sql is None and not configuration['query'].get('query', None):
            raise TypeError('`BigQueryBaseCursor.run_query` '
                            'missing 1 required positional argument: `sql`')

        # BigQuery also allows you to define how you want a table's schema to change
        # as a side effect of a query job
        # for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.schemaUpdateOptions  # noqa # pylint: disable=line-too-long

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
            destination_project, destination_dataset, destination_table = \
                _split_tablename(table_input=destination_dataset_table,
                                 default_project_id=self.project_id)

            destination_dataset_table = {  # type: ignore
                'projectId': destination_project,
                'datasetId': destination_dataset,
                'tableId': destination_table,
            }

        if cluster_fields:
            cluster_fields = {'fields': cluster_fields}  # type: ignore

        query_param_list = [
            (sql, 'query', None, (str,)),
            (priority, 'priority', 'INTERACTIVE', (str,)),
            (use_legacy_sql, 'useLegacySql', self.use_legacy_sql, bool),
            (query_params, 'queryParameters', None, list),
            (udf_config, 'userDefinedFunctionResources', None, list),
            (maximum_billing_tier, 'maximumBillingTier', None, int),
            (maximum_bytes_billed, 'maximumBytesBilled', None, float),
            (time_partitioning, 'timePartitioning', {}, dict),
            (schema_update_options, 'schemaUpdateOptions', None, list),
            (destination_dataset_table, 'destinationTable', None, dict),
            (cluster_fields, 'clustering', None, dict),
        ]  # type: List[Tuple]

        for param, param_name, param_default, param_type in query_param_list:
            if param_name not in configuration['query'] and param in [None, {}, ()]:
                if param_name == 'timePartitioning':
                    param_default = _cleanse_time_partitioning(
                        destination_dataset_table, time_partitioning)
                param = param_default

            if param in [None, {}, ()]:
                continue

            _api_resource_configs_duplication_check(
                param_name, param, configuration['query'])

            configuration['query'][param_name] = param

            # check valid type of provided param,
            # it last step because we can get param from 2 sources,
            # and first of all need to find it

            _validate_value(param_name, configuration['query'][param_name],
                            param_type)

            if param_name == 'schemaUpdateOptions' and param:
                self.log.info("Adding experimental 'schemaUpdateOptions': "
                              "%s", schema_update_options)

            if param_name != 'destinationTable':
                continue

            for key in ['projectId', 'datasetId', 'tableId']:
                if key not in configuration['query']['destinationTable']:
                    raise ValueError(
                        "Not correct 'destinationTable' in "
                        "api_resource_configs. 'destinationTable' "
                        "must be a dict with {'projectId':'', "
                        "'datasetId':'', 'tableId':''}")

            configuration['query'].update({
                'allowLargeResults': allow_large_results,
                'flattenResults': flatten_results,
                'writeDisposition': write_disposition,
                'createDisposition': create_disposition,
            })

        if 'useLegacySql' in configuration['query'] and configuration['query']['useLegacySql'] and\
                'queryParameters' in configuration['query']:
            raise ValueError("Query parameters are not allowed "
                             "when using legacy SQL")

        if labels:
            _api_resource_configs_duplication_check(
                'labels', labels, configuration)
            configuration['labels'] = labels

        if encryption_configuration:
            configuration["query"][
                "destinationEncryptionConfiguration"
            ] = encryption_configuration

        return self.run_with_configuration(configuration)

    def run_extract(  # noqa
            self,
            source_project_dataset_table: str,
            destination_cloud_storage_uris: str,
            compression: str = 'NONE',
            export_format: str = 'CSV',
            field_delimiter: str = ',',
            print_header: bool = True,
            labels: Optional[Dict] = None) -> str:
        """
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
        """

        source_project, source_dataset, source_table = \
            _split_tablename(table_input=source_project_dataset_table,
                             default_project_id=self.project_id,
                             var_name='source_project_dataset_table')

        configuration = {
            'extract': {
                'sourceTable': {
                    'projectId': source_project,
                    'datasetId': source_dataset,
                    'tableId': source_table,
                },
                'compression': compression,
                'destinationUris': destination_cloud_storage_uris,
                'destinationFormat': export_format,
            }
        }  # type: Dict[str, Any]

        if labels:
            configuration['labels'] = labels

        if export_format == 'CSV':
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            configuration['extract']['fieldDelimiter'] = field_delimiter
            configuration['extract']['printHeader'] = print_header

        return self.run_with_configuration(configuration)

    def run_copy(self,  # pylint: disable=invalid-name
                 source_project_dataset_tables: Union[List, str],
                 destination_project_dataset_table: str,
                 write_disposition: str = 'WRITE_EMPTY',
                 create_disposition: str = 'CREATE_IF_NEEDED',
                 labels: Optional[Dict] = None,
                 encryption_configuration: Optional[Dict] = None) -> str:
        """
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
        """
        source_project_dataset_tables = ([
            source_project_dataset_tables
        ] if not isinstance(source_project_dataset_tables, list) else
            source_project_dataset_tables)

        source_project_dataset_tables_fixup = []
        for source_project_dataset_table in source_project_dataset_tables:
            source_project, source_dataset, source_table = \
                _split_tablename(table_input=source_project_dataset_table,
                                 default_project_id=self.project_id,
                                 var_name='source_project_dataset_table')
            source_project_dataset_tables_fixup.append({
                'projectId':
                source_project,
                'datasetId':
                source_dataset,
                'tableId':
                source_table
            })

        destination_project, destination_dataset, destination_table = \
            _split_tablename(table_input=destination_project_dataset_table,
                             default_project_id=self.project_id)
        configuration = {
            'copy': {
                'createDisposition': create_disposition,
                'writeDisposition': write_disposition,
                'sourceTables': source_project_dataset_tables_fixup,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table
                }
            }
        }

        if labels:
            configuration['labels'] = labels

        if encryption_configuration:
            configuration["copy"][
                "destinationEncryptionConfiguration"
            ] = encryption_configuration

        return self.run_with_configuration(configuration)

    def run_load(self,  # pylint: disable=too-many-locals,too-many-arguments,invalid-name
                 destination_project_dataset_table: str,
                 source_uris: List,
                 schema_fields: Optional[List] = None,
                 source_format: str = 'CSV',
                 create_disposition: str = 'CREATE_IF_NEEDED',
                 skip_leading_rows: int = 0,
                 write_disposition: str = 'WRITE_EMPTY',
                 field_delimiter: str = ',',
                 max_bad_records: int = 0,
                 quote_character: Optional[str] = None,
                 ignore_unknown_values: bool = False,
                 allow_quoted_newlines: bool = False,
                 allow_jagged_rows: bool = False,
                 encoding: str = "UTF-8",
                 schema_update_options: Optional[Iterable] = None,
                 src_fmt_configs: Optional[Dict] = None,
                 time_partitioning: Optional[Dict] = None,
                 cluster_fields: Optional[List] = None,
                 autodetect: bool = False,
                 encryption_configuration: Optional[Dict] = None) -> str:
        """
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
            soure_format is CSV.
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
            by one or more columns. This is only available in combination with
            time_partitioning. The order of columns given determines the sort order.
        :type cluster_fields: list[str]
        :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
            **Example**: ::

                encryption_configuration = {
                    "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
                }
        :type encryption_configuration: dict
        """
        # To provide backward compatibility
        schema_update_options = list(schema_update_options or [])

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).sourceFormat # noqa # pylint: disable=line-too-long

        if schema_fields is None and not autodetect:
            raise ValueError(
                'You must either pass a schema or autodetect=True.')

        if src_fmt_configs is None:
            src_fmt_configs = {}

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

        destination_project, destination_dataset, destination_table = \
            _split_tablename(table_input=destination_project_dataset_table,
                             default_project_id=self.project_id,
                             var_name='destination_project_dataset_table')

        configuration = {
            'load': {
                'autodetect': autodetect,
                'createDisposition': create_disposition,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                },
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'writeDisposition': write_disposition,
                'ignoreUnknownValues': ignore_unknown_values
            }
        }

        time_partitioning = _cleanse_time_partitioning(
            destination_project_dataset_table,
            time_partitioning
        )
        if time_partitioning:
            configuration['load'].update({
                'timePartitioning': time_partitioning
            })

        if cluster_fields:
            configuration['load'].update({'clustering': {'fields': cluster_fields}})

        if schema_fields:
            configuration['load']['schema'] = {'fields': schema_fields}

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError("schema_update_options is only "
                                 "allowed if write_disposition is "
                                 "'WRITE_APPEND' or 'WRITE_TRUNCATE'.")
            else:
                self.log.info(
                    "Adding experimental 'schemaUpdateOptions': %s",
                    schema_update_options
                )
                configuration['load'][
                    'schemaUpdateOptions'] = schema_update_options

        if max_bad_records:
            configuration['load']['maxBadRecords'] = max_bad_records

        if encryption_configuration:
            configuration["load"][
                "destinationEncryptionConfiguration"
            ] = encryption_configuration

        src_fmt_to_configs_mapping = {
            'CSV': [
                'allowJaggedRows', 'allowQuotedNewlines', 'autodetect',
                'fieldDelimiter', 'skipLeadingRows', 'ignoreUnknownValues',
                'nullMarker', 'quote', 'encoding'
            ],
            'DATASTORE_BACKUP': ['projectionFields'],
            'NEWLINE_DELIMITED_JSON': ['autodetect', 'ignoreUnknownValues'],
            'PARQUET': ['autodetect', 'ignoreUnknownValues'],
            'AVRO': ['useAvroLogicalTypes'],
        }

        valid_configs = src_fmt_to_configs_mapping[source_format]

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        backward_compatibility_configs = {'skipLeadingRows': skip_leading_rows,
                                          'fieldDelimiter': field_delimiter,
                                          'ignoreUnknownValues': ignore_unknown_values,
                                          'quote': quote_character,
                                          'allowQuotedNewlines': allow_quoted_newlines,
                                          'encoding': encoding}

        src_fmt_configs = _validate_src_fmt_configs(source_format, src_fmt_configs, valid_configs,
                                                    backward_compatibility_configs)

        configuration['load'].update(src_fmt_configs)

        if allow_jagged_rows:
            configuration['load']['allowJaggedRows'] = allow_jagged_rows

        return self.run_with_configuration(configuration)

    def run_with_configuration(self, configuration: Dict) -> str:
        """
        Executes a BigQuery SQL query. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about the configuration parameter.

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        """
        jobs = self.service.jobs()  # type: Any
        job_data = {'configuration': configuration}  # type: Dict[str, Dict]

        # Send query and wait for reply.
        query_reply = jobs \
            .insert(projectId=self.project_id, body=job_data) \
            .execute(num_retries=self.num_retries)
        self.running_job_id = query_reply['jobReference']['jobId']
        if 'location' in query_reply['jobReference']:
            location = query_reply['jobReference']['location']
        else:
            location = self.location

        # Wait for query to finish.
        keep_polling_job = True  # type: bool
        while keep_polling_job:
            try:
                keep_polling_job = self._check_query_status(jobs, keep_polling_job, location)

            except HttpError as err:
                if err.resp.status in [500, 503]:
                    self.log.info(
                        '%s: Retryable error, waiting for job to complete: %s',
                        err.resp.status, self.running_job_id)
                    time.sleep(5)
                else:
                    raise Exception(
                        'BigQuery job status check failed. Final error was: {}'.
                        format(err.resp.status))

        return self.running_job_id  # type: ignore

    def _check_query_status(self, jobs: Any, keep_polling_job: bool, location: str) -> bool:
        if location:
            job = jobs.get(
                projectId=self.project_id,
                jobId=self.running_job_id,
                location=location).execute(num_retries=self.num_retries)
        else:
            job = jobs.get(
                projectId=self.project_id,
                jobId=self.running_job_id).execute(num_retries=self.num_retries)

        if job['status']['state'] == 'DONE':
            keep_polling_job = False
            # Check if job had errors.
            if 'errorResult' in job['status']:
                raise Exception(
                    'BigQuery job failed. Final error was: {}. The job was: {}'.format(
                        job['status']['errorResult'], job))
        else:
            self.log.info('Waiting for job to complete : %s, %s',
                          self.project_id, self.running_job_id)
            time.sleep(5)
        return keep_polling_job

    def poll_job_complete(self, job_id: str) -> bool:
        """
        Check if jobs completed.

        :param job_id: id of the job.
        :type job_id: str
        :rtype: bool
        """
        jobs = self.service.jobs()
        try:
            if self.location:
                job = jobs.get(projectId=self.project_id,
                               jobId=job_id,
                               location=self.location).execute(num_retries=self.num_retries)
            else:
                job = jobs.get(projectId=self.project_id,
                               jobId=job_id).execute(num_retries=self.num_retries)
            if job['status']['state'] == 'DONE':
                return True
        except HttpError as err:
            if err.resp.status in [500, 503]:
                self.log.info(
                    '%s: Retryable error while polling job with id %s',
                    err.resp.status, job_id)
            else:
                raise Exception(
                    'BigQuery job status check failed. Final error was: {}'.
                    format(err.resp.status))
        return False

    def cancel_query(self) -> None:
        """
        Cancel all started queries that have not yet completed
        """
        jobs = self.service.jobs()
        if (self.running_job_id and
                not self.poll_job_complete(self.running_job_id)):
            self.log.info('Attempting to cancel job : %s, %s', self.project_id,
                          self.running_job_id)
            if self.location:
                jobs.cancel(
                    projectId=self.project_id,
                    jobId=self.running_job_id,
                    location=self.location).execute(num_retries=self.num_retries)
            else:
                jobs.cancel(
                    projectId=self.project_id,
                    jobId=self.running_job_id).execute(num_retries=self.num_retries)
        else:
            self.log.info('No running BigQuery jobs to cancel.')
            return

        # Wait for all the calls to cancel to finish
        max_polling_attempts = 12
        polling_attempts = 0

        job_complete = False
        while polling_attempts < max_polling_attempts and not job_complete:
            polling_attempts = polling_attempts + 1
            job_complete = self.poll_job_complete(self.running_job_id)
            if job_complete:
                self.log.info('Job successfully canceled: %s, %s',
                              self.project_id, self.running_job_id)
            elif polling_attempts == max_polling_attempts:
                self.log.info(
                    "Stopping polling due to timeout. Job with id %s "
                    "has not completed cancel and may or may not finish.",
                    self.running_job_id)
            else:
                self.log.info('Waiting for canceled job with id %s to finish.',
                              self.running_job_id)
                time.sleep(5)

    def get_dataset_tables(self, dataset_id: str, project_id: Optional[str] = None,
                           max_results: Optional[int] = None,
                           page_token: Optional[str] = None) -> Dict[str, Union[str, int, List]]:
        """
        Get the list of tables for a given dataset.
        .. seealso:: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

        :param dataset_id: the dataset ID of the requested dataset.
        :type dataset_id: str
        :param project_id: (Optional) the project of the requested dataset. If None,
            self.project_id will be used.
        :type project_id: str
        :param max_results: (Optional) the maximum number of tables to return.
        :type max_results: int
        :param page_token: (Optional) page token, returned from a previous call,
            identifying the result set.
        :type page_token: str

        :return: map containing the list of tables + metadata.
        """
        optional_params = {}  # type: Dict[str, Union[str, int]]
        if max_results:
            optional_params['maxResults'] = max_results
        if page_token:
            optional_params['pageToken'] = page_token

        dataset_project_id = project_id or self.project_id

        return (self.service.tables().list(
            projectId=dataset_project_id,
            datasetId=dataset_id,
            **optional_params).execute(num_retries=self.num_retries))

    def get_schema(self, dataset_id: str, table_id: str) -> Dict:
        """
        Get the schema for a given datset.table.
        see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource

        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :return: a table schema
        """
        tables_resource = self.service.tables() \
            .get(projectId=self.project_id, datasetId=dataset_id, tableId=table_id) \
            .execute(num_retries=self.num_retries)
        return tables_resource['schema']

    def get_tabledata(self, dataset_id: str, table_id: str,
                      max_results: Optional[int] = None, selected_fields: Optional[str] = None,
                      page_token: Optional[str] = None, start_index: Optional[int] = None) -> Dict:
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
        :param start_index: zero based index of the starting row to read.
        :return: map containing the requested rows.
        """
        optional_params = {}  # type: Dict[str, Any]
        if self.location:
            optional_params['location'] = self.location
        if max_results:
            optional_params['maxResults'] = max_results
        if selected_fields:
            optional_params['selectedFields'] = selected_fields
        if page_token:
            optional_params['pageToken'] = page_token
        if start_index:
            optional_params['startIndex'] = start_index
        return (self.service.tabledata().list(
            projectId=self.project_id,
            datasetId=dataset_id,
            tableId=table_id,
            **optional_params).execute(num_retries=self.num_retries))

    def run_table_delete(self, deletion_dataset_table: str,
                         ignore_if_missing: bool = False) -> None:
        """
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
        """
        deletion_project, deletion_dataset, deletion_table = \
            _split_tablename(table_input=deletion_dataset_table,
                             default_project_id=self.project_id)

        try:
            self.service.tables() \
                .delete(projectId=deletion_project,
                        datasetId=deletion_dataset,
                        tableId=deletion_table) \
                .execute(num_retries=self.num_retries)
            self.log.info('Deleted table %s:%s.%s.', deletion_project,
                          deletion_dataset, deletion_table)
        except HttpError:
            if not ignore_if_missing:
                raise Exception('Table deletion failed. Table does not exist.')
            else:
                self.log.info('Table does not exist. Skipping.')

    def run_table_upsert(self, dataset_id: str, table_resource: Dict,
                         project_id: Optional[str] = None) -> Dict:
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
        :param project_id: the project to upsert the table into.  If None,
            project will be self.project_id.
        :return:
        """
        # check to see if the table exists
        table_id = table_resource['tableReference']['tableId']
        project_id = project_id if project_id is not None else self.project_id
        tables_list_resp = self.service.tables().list(
            projectId=project_id, datasetId=dataset_id).execute(num_retries=self.num_retries)
        while True:
            for table in tables_list_resp.get('tables', []):
                if table['tableReference']['tableId'] == table_id:
                    # found the table, do update
                    self.log.info('Table %s:%s.%s exists, updating.',
                                  project_id, dataset_id, table_id)
                    return self.service.tables().update(
                        projectId=project_id,
                        datasetId=dataset_id,
                        tableId=table_id,
                        body=table_resource).execute(num_retries=self.num_retries)
            # If there is a next page, we need to check the next page.
            if 'nextPageToken' in tables_list_resp:
                tables_list_resp = self.service.tables()\
                    .list(projectId=project_id,
                          datasetId=dataset_id,
                          pageToken=tables_list_resp['nextPageToken'])\
                    .execute(num_retries=self.num_retries)
            # If there is no next page, then the table doesn't exist.
            else:
                # do insert
                self.log.info('Table %s:%s.%s does not exist. creating.',
                              project_id, dataset_id, table_id)
                return self.service.tables().insert(
                    projectId=project_id,
                    datasetId=dataset_id,
                    body=table_resource).execute(num_retries=self.num_retries)

    def run_grant_dataset_view_access(self,
                                      source_dataset: str,
                                      view_dataset: str,
                                      view_table: str,
                                      source_project: Optional[str] = None,
                                      view_project: Optional[str] = None) -> Dict:
        """
        Grant authorized view access of a dataset to a view table.
        If this view has already been granted access to the dataset, do nothing.
        This method is not atomic.  Running it may clobber a simultaneous update.

        :param source_dataset: the source dataset
        :type source_dataset: str
        :param view_dataset: the dataset that the view is in
        :type view_dataset: str
        :param view_table: the table of the view
        :type view_table: str
        :param source_project: the project of the source dataset. If None,
            self.project_id will be used.
        :type source_project: str
        :param view_project: the project that the view is in. If None,
            self.project_id will be used.
        :type view_project: str
        :return: the datasets resource of the source dataset.
        """

        # Apply default values to projects
        source_project = source_project if source_project else self.project_id
        view_project = view_project if view_project else self.project_id

        # we don't want to clobber any existing accesses, so we have to get
        # info on the dataset before we can add view access
        source_dataset_resource = self.service.datasets().get(
            projectId=source_project, datasetId=source_dataset).execute(num_retries=self.num_retries)
        access = source_dataset_resource[
            'access'] if 'access' in source_dataset_resource else []
        view_access = {
            'view': {
                'projectId': view_project,
                'datasetId': view_dataset,
                'tableId': view_table
            }
        }
        # check to see if the view we want to add already exists.
        if view_access not in access:
            self.log.info(
                'Granting table %s:%s.%s authorized view access to %s:%s dataset.',
                view_project, view_dataset, view_table, source_project,
                source_dataset)
            access.append(view_access)
            return self.service.datasets().patch(
                projectId=source_project,
                datasetId=source_dataset,
                body={
                    'access': access
                }).execute(num_retries=self.num_retries)
        else:
            # if view is already in access, do nothing.
            self.log.info(
                'Table %s:%s.%s already has authorized view access to %s:%s dataset.',
                view_project, view_dataset, view_table, source_project, source_dataset)
            return source_dataset_resource

    def create_empty_dataset(self,
                             dataset_id: str = "",
                             project_id: str = "",
                             location: Optional[str] = None,
                             dataset_reference: Optional[Dict] = None) -> None:
        """
        Create a new empty dataset:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/insert

        :param project_id: The name of the project where we want to create
            an empty a dataset. Don't need to provide, if projectId in dataset_reference.
        :type project_id: str
        :param dataset_id: The id of dataset. Don't need to provide,
            if datasetId in dataset_reference.
        :type dataset_id: str
        :param location: (Optional) The geographic location where the dataset should reside.
            There is no default value but the dataset will be created in US if nothing is provided.
        :type location: str
        :param dataset_reference: Dataset reference that could be provided
            with request body. More info:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        :type dataset_reference: dict
        """

        if dataset_reference:
            _validate_value('dataset_reference', dataset_reference, dict)
        else:
            dataset_reference = {}

        if "datasetReference" not in dataset_reference:
            dataset_reference["datasetReference"] = {}

        if self.location:
            dataset_reference['location'] = dataset_reference.get('location') or self.location

        if not dataset_reference["datasetReference"].get("datasetId") and not dataset_id:
            raise ValueError(
                "{} not provided datasetId. Impossible to create dataset")

        dataset_required_params = [(dataset_id, "datasetId", ""),
                                   (project_id, "projectId", self.project_id)]
        for param_tuple in dataset_required_params:
            param, param_name, param_default = param_tuple
            if param_name not in dataset_reference['datasetReference']:
                if param_default and not param:
                    self.log.info(
                        "%s was not specified. Will be used default value %s.",
                        param_name, param_default
                    )
                    param = param_default
                dataset_reference['datasetReference'].update(
                    {param_name: param})
            elif param:
                _api_resource_configs_duplication_check(
                    param_name, param,
                    dataset_reference['datasetReference'], 'dataset_reference')

        if location:
            if 'location' not in dataset_reference:
                dataset_reference.update({'location': location})
            else:
                _api_resource_configs_duplication_check(
                    'location', location,
                    dataset_reference, 'dataset_reference')

        dataset_id = dataset_reference.get("datasetReference").get("datasetId")  # type: ignore
        dataset_project_id = dataset_reference.get("datasetReference").get("projectId")  # type: ignore

        self.service.datasets().insert(
            projectId=dataset_project_id,
            body=dataset_reference).execute(num_retries=self.num_retries)

    def delete_dataset(self, project_id: str, dataset_id: str, delete_contents: bool = False) -> None:
        """
        Delete a dataset of Big query in your project.

        :param project_id: The name of the project where we have the dataset .
        :type project_id: str
        :param dataset_id: The dataset to be delete.
        :type dataset_id: str
        :param delete_contents: [Optional] Whether to force the deletion even if the dataset is not empty.
            Will delete all tables (if any) in the dataset if set to True.
            Will raise HttpError 400: "{dataset_id} is still in use" if set to False and dataset is not empty.
            The default value is False.
        :type delete_contents: bool
        :return:
        """
        project_id = project_id if project_id is not None else self.project_id
        self.log.info('Deleting from project: %s  Dataset:%s',
                      project_id, dataset_id)

        try:
            self.service.datasets().delete(
                projectId=project_id,
                datasetId=dataset_id,
                deleteContents=delete_contents).execute(num_retries=self.num_retries)
            self.log.info('Dataset deleted successfully: In project %s '
                          'Dataset %s', project_id, dataset_id)

        except HttpError as err:
            raise AirflowException(
                'BigQuery job failed. Error was: {}'.format(err.content)
            )

    def get_dataset(self, dataset_id: str, project_id: Optional[str] = None) -> Dict:
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

        if not dataset_id or not isinstance(dataset_id, str):
            raise ValueError("dataset_id argument must be provided and has "
                             "a type 'str'. You provided: {}".format(dataset_id))

        dataset_project_id = project_id if project_id else self.project_id

        try:
            dataset_resource = self.service.datasets().get(
                datasetId=dataset_id, projectId=dataset_project_id).execute(num_retries=self.num_retries)
            self.log.info("Dataset Resource: %s", dataset_resource)
        except HttpError as err:
            raise AirflowException(
                'BigQuery job failed. Error was: {}'.format(err.content))

        return dataset_resource

    def get_datasets_list(self, project_id: Optional[str] = None) -> List:
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
        dataset_project_id = project_id if project_id else self.project_id

        try:
            datasets_list = self.service.datasets().list(
                projectId=dataset_project_id).execute(num_retries=self.num_retries)['datasets']
            self.log.info("Datasets List: %s", datasets_list)

        except HttpError as err:
            raise AirflowException(
                'BigQuery job failed. Error was: {}'.format(err.content))

        return datasets_list

    @CloudBaseHook.catch_http_exception
    def get_dataset_tables_list(self, dataset_id, project_id=None, table_prefix=None, max_results=None):
        """
        Method returns tables list of a BigQuery dataset. If table prefix is specified,
        only tables beginning by it are returned.

        .. seealso::
            For more information, see:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/tables/list

        :param dataset_id: The BigQuery Dataset ID
        :type dataset_id: str
        :param project_id: The GCP Project ID
        :type project_id: str
        :param table_prefix: Tables must begin by this prefix to be returned (case sensitive)
        :type table_prefix: str
        :param max_results: The maximum number of results to return in a single response page.
            Leverage the page tokens to iterate through the entire collection.
        :type max_results: int
        :return: dataset_tables_list

            Example of returned dataset_tables_list: ::

                    [
                       {
                          "projectId": "your-project",
                          "datasetId": "dataset",
                          "tableId": "table1"
                        },
                        {
                          "projectId": "your-project",
                          "datasetId": "dataset",
                          "tableId": "table2"
                        }
                    ]
        """

        dataset_project_id = project_id if project_id else self.project_id

        optional_params = {}
        if max_results:
            optional_params['maxResults'] = max_results

        request = self.service.tables().list(projectId=dataset_project_id,
                                             datasetId=dataset_id,
                                             **optional_params)
        dataset_tables_list = []
        while request is not None:
            response = request.execute(num_retries=self.num_retries)

            for table in response.get('tables', []):
                table_ref = table.get('tableReference')
                table_id = table_ref.get('tableId')
                if table_id and (not table_prefix or table_id.startswith(table_prefix)):
                    dataset_tables_list.append(table_ref)

            request = self.service.tables().list_next(previous_request=request,
                                                      previous_response=response)

        self.log.info("%s tables found", len(dataset_tables_list))

        return dataset_tables_list

    def patch_dataset(self, dataset_id: str, dataset_resource: str, project_id: Optional[str] = None) -> Dict:
        """
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
        :param project_id: The GCP Project ID
        :type project_id: str
        :rtype: dataset
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        """

        if not dataset_id or not isinstance(dataset_id, str):
            raise ValueError(
                "dataset_id argument must be provided and has "
                "a type 'str'. You provided: {}".format(dataset_id)
            )

        dataset_project_id = project_id if project_id else self.project_id

        try:
            dataset = (
                self.service.datasets()
                .patch(
                    datasetId=dataset_id,
                    projectId=dataset_project_id,
                    body=dataset_resource,
                )
                .execute(num_retries=self.num_retries)
            )
            self.log.info("Dataset successfully patched: %s", dataset)
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

        return dataset

    def update_dataset(self, dataset_id: str,
                       dataset_resource: Dict, project_id: Optional[str] = None) -> Dict:
        """
        Updates information in an existing dataset. The update method replaces the entire
        dataset resource, whereas the patch method only replaces fields that are provided
        in the submitted dataset resource.
        More info:
        https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets/update

        :param dataset_id: The BigQuery Dataset ID
        :type dataset_id: str
        :param dataset_resource: Dataset resource that will be provided
            in request body.
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        :type dataset_resource: dict
        :param project_id: The GCP Project ID
        :type project_id: str
        :rtype: dataset
            https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource
        """

        if not dataset_id or not isinstance(dataset_id, str):
            raise ValueError(
                "dataset_id argument must be provided and has "
                "a type 'str'. You provided: {}".format(dataset_id)
            )

        dataset_project_id = project_id if project_id else self.project_id

        try:
            dataset = (
                self.service.datasets()
                .update(
                    datasetId=dataset_id,
                    projectId=dataset_project_id,
                    body=dataset_resource,
                )
                .execute(num_retries=self.num_retries)
            )
            self.log.info("Dataset successfully updated: %s", dataset)
        except HttpError as err:
            raise AirflowException(
                "BigQuery job failed. Error was: {}".format(err.content)
            )

        return dataset

    def insert_all(self, project_id: str, dataset_id: str, table_id: str,
                   rows: List, ignore_unknown_values: bool = False,
                   skip_invalid_rows: bool = False, fail_on_error: bool = False) -> None:
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

        dataset_project_id = project_id if project_id else self.project_id

        body = {
            "rows": rows,
            "ignoreUnknownValues": ignore_unknown_values,
            "kind": "bigquery#tableDataInsertAllRequest",
            "skipInvalidRows": skip_invalid_rows,
        }

        try:
            self.log.info(
                'Inserting %s row(s) into Table %s:%s.%s',
                len(rows), dataset_project_id, dataset_id, table_id
            )

            resp = self.service.tabledata().insertAll(
                projectId=dataset_project_id, datasetId=dataset_id,
                tableId=table_id, body=body
            ).execute(num_retries=self.num_retries)

            if 'insertErrors' not in resp:
                self.log.info(
                    'All row(s) inserted successfully: %s:%s.%s',
                    dataset_project_id, dataset_id, table_id
                )
            else:
                error_msg = '{} insert error(s) occurred: {}:{}.{}. Details: {}'.format(
                    len(resp['insertErrors']),
                    dataset_project_id, dataset_id, table_id, resp['insertErrors'])
                if fail_on_error:
                    raise AirflowException(
                        'BigQuery job failed. Error was: {}'.format(error_msg)
                    )
                self.log.info(error_msg)
        except HttpError as err:
            raise AirflowException(
                'BigQuery job failed. Error was: {}'.format(err.content)
            )


class BigQueryCursor(BigQueryBaseCursor):
    """
    A very basic BigQuery PEP 249 cursor implementation. The PyHive PEP 249
    implementation was used as a reference:

    https://github.com/dropbox/PyHive/blob/master/pyhive/presto.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py
    """

    def __init__(
        self,
        service: Any,
        project_id: str,
        use_legacy_sql: bool = True,
        location: Optional[str] = None,
        num_retries: int = 5,
    ) -> None:
        super().__init__(
            service=service,
            project_id=project_id,
            use_legacy_sql=use_legacy_sql,
            location=location,
            num_retries=num_retries
        )
        self.buffersize = None  # type: Optional[int]
        self.page_token = None  # type: Optional[str]
        self.job_id = None  # type: Optional[str]
        self.buffer = []  # type: list
        self.all_pages_loaded = False  # type: bool

    @property
    def description(self) -> NoReturn:
        """ The schema description method is not currently implemented. """
        raise NotImplementedError

    def close(self) -> None:
        """ By default, do nothing """

    @property
    def rowcount(self) -> int:
        """ By default, return -1 to indicate that this is not supported. """
        return -1

    def execute(self, operation: str, parameters: Optional[Dict] = None) -> None:
        """
        Executes a BigQuery query, and returns the job ID.

        :param operation: The query to execute.
        :type operation: str
        :param parameters: Parameters to substitute into the query.
        :type parameters: dict
        """
        sql = _bind_parameters(operation,
                               parameters) if parameters else operation
        self.flush_results()
        self.job_id = self.run_query(sql)

    def executemany(self, operation: str, seq_of_parameters: List) -> None:
        """
        Execute a BigQuery query multiple times with different parameters.

        :param operation: The query to execute.
        :type operation: str
        :param seq_of_parameters: List of dictionary parameters to substitute into the
            query.
        :type seq_of_parameters: list
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def flush_results(self) -> None:
        """ Flush results related cursor attributes. """
        self.page_token = None
        self.job_id = None
        self.all_pages_loaded = False
        self.buffer = []

    def fetchone(self) -> Union[List, None]:
        """ Fetch the next row of a query result set. """
        return self.next()

    def next(self) -> Union[List, None]:
        """
        Helper method for fetchone, which returns the next row from a buffer.
        If the buffer is empty, attempts to paginate through the result set for
        the next page, and load it into the buffer.
        """
        if not self.job_id:
            return None

        if not self.buffer:
            if self.all_pages_loaded:
                return None

            query_results = (self.service.jobs().getQueryResults(
                projectId=self.project_id,
                jobId=self.job_id,
                pageToken=self.page_token).execute(num_retries=self.num_retries))

            if 'rows' in query_results and query_results['rows']:
                self.page_token = query_results.get('pageToken')
                fields = query_results['schema']['fields']
                col_types = [field['type'] for field in fields]
                rows = query_results['rows']

                for dict_row in rows:
                    typed_row = ([
                        _bq_cast(vs['v'], col_types[idx])
                        for idx, vs in enumerate(dict_row['f'])
                    ])
                    self.buffer.append(typed_row)

                if not self.page_token:
                    self.all_pages_loaded = True

            else:
                # Reset all state since we've exhausted the results.
                self.flush_results()
                return None

        return self.buffer.pop(0)

    def fetchmany(self, size: Optional[int] = None) -> List:
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences
        (e.g. a list of tuples). An empty sequence is returned when no more rows are
        available. The number of rows to fetch per call is specified by the parameter.
        If it is not given, the cursor's arraysize determines the number of rows to be
        fetched. The method should try to fetch as many rows as indicated by the size
        parameter. If this is not possible due to the specified number of rows not being
        available, fewer rows may be returned. An :py:class:`~pyhive.exc.Error`
        (or subclass) exception is raised if the previous call to
        :py:meth:`execute` did not produce any result set or no call was issued yet.
        """
        if size is None:
            size = self.arraysize
        result = []
        for _ in range(size):
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    def fetchall(self) -> List[List]:
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of
        sequences (e.g. a list of tuples).
        """
        result = []
        while True:
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    def get_arraysize(self) -> int:
        """ Specifies the number of rows to fetch at a time with .fetchmany() """
        return self.buffersize or 1

    def set_arraysize(self, arraysize: int) -> None:
        """ Specifies the number of rows to fetch at a time with .fetchmany() """
        self.buffersize = arraysize

    arraysize = property(get_arraysize, set_arraysize)

    def setinputsizes(self, sizes: Any) -> None:
        """ Does nothing by default """

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        """ Does nothing by default """


def _bind_parameters(operation: str, parameters: Dict) -> str:
    """ Helper method that binds parameters to a SQL query. """
    # inspired by MySQL Python Connector (conversion.py)
    string_parameters = {}  # type Dict[str, str]
    for (name, value) in parameters.items():
        if value is None:
            string_parameters[name] = 'NULL'
        elif isinstance(value, str):
            string_parameters[name] = "'" + _escape(value) + "'"
        else:
            string_parameters[name] = str(value)
    return operation % string_parameters


def _escape(s: str) -> str:
    """ Helper method that escapes parameters to a SQL query. """
    e = s
    e = e.replace('\\', '\\\\')
    e = e.replace('\n', '\\n')
    e = e.replace('\r', '\\r')
    e = e.replace("'", "\\'")
    e = e.replace('"', '\\"')
    return e


def _bq_cast(string_field: str, bq_type: str) -> Union[None, int, float, bool, str]:
    """
    Helper method that casts a BigQuery row to the appropriate data types.
    This is useful because BigQuery returns all fields as strings.
    """
    if string_field is None:
        return None
    elif bq_type == 'INTEGER':
        return int(string_field)
    elif bq_type in ('FLOAT', 'TIMESTAMP'):
        return float(string_field)
    elif bq_type == 'BOOLEAN':
        if string_field not in ['true', 'false']:
            raise ValueError("{} must have value 'true' or 'false'".format(
                string_field))
        return string_field == 'true'
    else:
        return string_field


def _split_tablename(table_input: str, default_project_id: str,
                     var_name: Optional[str] = None) -> Tuple[str, str, str]:

    if '.' not in table_input:
        raise ValueError(
            'Expected target table name in the format of '
            '<dataset>.<table>. Got: {}'.format(table_input))

    if not default_project_id:
        raise ValueError("INTERNAL: No default project is specified")

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
        if var_name is not None:
            log = LoggingMixin().log
            log.info(
                'Project not included in %s: %s; using project "%s"',
                var_name, table_input, default_project_id
            )
        project_id = default_project_id

    return project_id, dataset_id, table_id


def _cleanse_time_partitioning(
    destination_dataset_table: Optional[str], time_partitioning_in: Optional[Dict]
) -> Dict:    # if it is a partitioned table ($ is in the table name) add partition load option

    if time_partitioning_in is None:
        time_partitioning_in = {}

    time_partitioning_out = {}
    if destination_dataset_table and '$' in destination_dataset_table:
        time_partitioning_out['type'] = 'DAY'
    time_partitioning_out.update(time_partitioning_in)
    return time_partitioning_out


def _validate_value(key: Any, value: Any, expected_type: Type) -> None:
    """ function to check expected type and raise
    error if type is not correct """
    if not isinstance(value, expected_type):
        raise TypeError("{} argument must have a type {} not {}".format(
            key, expected_type, type(value)))


def _api_resource_configs_duplication_check(key: Any, value: Any, config_dict: Dict,
                                            config_dict_name='api_resource_configs') -> None:
    if key in config_dict and value != config_dict[key]:
        raise ValueError("Values of {param_name} param are duplicated. "
                         "{dict_name} contained {param_name} param "
                         "in `query` config and {param_name} was also provided "
                         "with arg to run_query() method. Please remove duplicates."
                         .format(param_name=key, dict_name=config_dict_name))


def _validate_src_fmt_configs(source_format: str,
                              src_fmt_configs: Dict,
                              valid_configs: List[str],
                              backward_compatibility_configs: Optional[Dict] = None) -> Dict:
    """
    Validates the given src_fmt_configs against a valid configuration for the source format.
    Adds the backward compatiblity config to the src_fmt_configs.

    :param source_format: File format to export.
    :type source_format: str
    :param src_fmt_configs: Configure optional fields specific to the source format.
    :type src_fmt_configs: dict
    :param valid_configs: Valid configuration specific to the source format
    :type valid_configs: List[str]
    :param backward_compatibility_configs: The top-level params for backward-compatibility
    :type backward_compatibility_configs: dict
    """

    if backward_compatibility_configs is None:
        backward_compatibility_configs = {}

    for k, v in backward_compatibility_configs.items():
        if k not in src_fmt_configs and k in valid_configs:
            src_fmt_configs[k] = v

    for k, v in src_fmt_configs.items():
        if k not in valid_configs:
            raise ValueError("{0} is not a valid src_fmt_configs for type {1}."
                             .format(k, source_format))

    return src_fmt_configs
