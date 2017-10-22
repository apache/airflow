# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module contains a BigQuery Hook, as well as a very basic PEP 249
implementation for BigQuery.
"""

import time

from apiclient.discovery import build, HttpError
from googleapiclient import errors
from builtins import range
from pandas_gbq.gbq import GbqConnector, \
    _parse_data as gbq_parse_data, \
    _check_google_client_version as gbq_check_google_client_version, \
    _test_google_api_imports as gbq_test_google_api_imports
from pandas.tools.merge import concat
from past.builtins import basestring

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from airflow.utils.log.logging_mixin import LoggingMixin


class BigQueryHook(GoogleCloudBaseHook, DbApiHook, LoggingMixin):
    """
    Interact with BigQuery. This hook uses the Google Cloud Platform
    connection.
    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None):
        super(BigQueryHook, self).__init__(
            conn_id=bigquery_conn_id,
            delegate_to=delegate_to)

    def get_conn(self):
        """
        Returns a BigQuery PEP 249 connection object.
        """
        service = self.get_service()
        project = self._get_field('project')
        return BigQueryConnection(service=service, project_id=project)

    def get_service(self):
        """
        Returns a BigQuery service object.
        """
        http_authorized = self._authorize()
        return build('bigquery', 'v2', http=http_authorized)

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        """
        Insertion is currently unsupported. Theoretically, you could use
        BigQuery's streaming API to insert rows into a table, but this hasn't
        been implemented.
        """
        raise NotImplementedError()

    def get_pandas_df(self, bql, parameters=None, dialect='legacy'):
        """
        Returns a Pandas DataFrame for the results produced by a BigQuery
        query. The DbApiHook method must be overridden because Pandas
        doesn't support PEP 249 connections, except for SQLite. See:

        https://github.com/pydata/pandas/blob/master/pandas/io/sql.py#L447
        https://github.com/pydata/pandas/issues/6900

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        :param parameters: The parameters to render the SQL query with (not used, leave to override superclass method)
        :type parameters: mapping or iterable
        :param dialect: Dialect of BigQuery SQL – legacy SQL or standard SQL
        :type dialect: string in {'legacy', 'standard'}, default 'legacy'
        """
        service = self.get_service()
        project = self._get_field('project')
        connector = BigQueryPandasConnector(project, service, dialect=dialect)
        schema, pages = connector.run_query(bql)
        dataframe_list = []

        while len(pages) > 0:
            page = pages.pop()
            dataframe_list.append(gbq_parse_data(schema, page))

        if len(dataframe_list) > 0:
            return concat(dataframe_list, ignore_index=True)
        else:
            return gbq_parse_data(schema, [])

    def table_exists(self, project_id, dataset_id, table_id):
        """
        Checks for the existence of a table in Google BigQuery.

        :param project_id: The Google cloud project in which to look for the table. The connection supplied to the hook
        must provide access to the specified project.
        :type project_id: string
        :param dataset_id: The name of the dataset in which to look for the table.
            storage bucket.
        :type dataset_id: string
        :param table_id: The name of the table to check the existence of.
        :type table_id: string
        """
        service = self.get_service()
        try:
            service.tables().get(
                projectId=project_id,
                datasetId=dataset_id,
                tableId=table_id
            ).execute()
            return True
        except errors.HttpError as e:
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
    def __init__(self, project_id, service, reauth=False, verbose=False, dialect='legacy'):
        gbq_check_google_client_version()
        gbq_test_google_api_imports()
        self.project_id = project_id
        self.reauth = reauth
        self.service = service
        self.verbose = verbose
        self.dialect = dialect


class BigQueryConnection(object):
    """
    BigQuery does not have a notion of a persistent connection. Thus, these
    objects are small stateless factories for cursors, which do all the real
    work.
    """

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def close(self):
        """ BigQueryConnection does not have anything to close. """
        pass

    def commit(self):
        """ BigQueryConnection does not support transactions. """
        pass

    def cursor(self):
        """ Return a new :py:class:`Cursor` object using the connection. """
        return BigQueryCursor(*self._args, **self._kwargs)

    def rollback(self):
        raise NotImplementedError(
            "BigQueryConnection does not have transactions")


class BigQueryBaseCursor(LoggingMixin):
    """
    The BigQuery base cursor contains helper methods to execute queries against
    BigQuery. The methods can be used directly by operators, in cases where a
    PEP 249 cursor isn't needed.
    """
    def __init__(self, service, project_id):
        self.service = service
        self.project_id = project_id
        self.running_job_id = None

    def run_query(
            self, bql, destination_dataset_table = False,
            write_disposition = 'WRITE_EMPTY',
            allow_large_results=False,
            udf_config = False,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            create_disposition='CREATE_IF_NEEDED',
            query_params=None):
        """
        Executes a BigQuery SQL query. Optionally persists results in a BigQuery
        table. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        :param destination_dataset_table: The dotted <dataset>.<table>
            BigQuery table to save the query results.
        :param write_disposition: What to do if the table already exists in
            BigQuery.
        :type write_disposition: string
        :param create_disposition: Specifies whether the job is allowed to create new tables.
        :type create_disposition: string
        :param allow_large_results: Whether to allow large results.
        :type allow_large_results: boolean
        :param udf_config: The User Defined Function configuration for the query.
            See https://cloud.google.com/bigquery/user-defined-functions for details.
        :type udf_config: list
        :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
        :type use_legacy_sql: boolean
        :param maximum_billing_tier: Positive integer that serves as a multiplier of the basic price.
        :type maximum_billing_tier: integer
        """
        configuration = {
            'query': {
                'query': bql,
                'useLegacySql': use_legacy_sql,
                'maximumBillingTier': maximum_billing_tier
            }
        }

        if destination_dataset_table:
            assert '.' in destination_dataset_table, (
                'Expected destination_dataset_table in the format of '
                '<dataset>.<table>. Got: {}').format(destination_dataset_table)
            destination_project, destination_dataset, destination_table = \
                _split_tablename(table_input=destination_dataset_table,
                                 default_project_id=self.project_id)
            configuration['query'].update({
                'allowLargeResults': allow_large_results,
                'writeDisposition': write_disposition,
                'createDisposition': create_disposition,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                }
            })
        if udf_config:
            assert isinstance(udf_config, list)
            configuration['query'].update({
                'userDefinedFunctionResources': udf_config
            })

        if query_params:
            configuration['query']['queryParameters'] = query_params

        return self.run_with_configuration(configuration)

    def run_extract(  # noqa
            self, source_project_dataset_table, destination_cloud_storage_uris,
            compression='NONE', export_format='CSV', field_delimiter=',',
            print_header=True):
        """
        Executes a BigQuery extract command to copy data from BigQuery to
        Google Cloud Storage. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param source_project_dataset_table: The dotted <dataset>.<table>
            BigQuery table to use as the source data.
        :type source_project_dataset_table: string
        :param destination_cloud_storage_uris: The destination Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). Follows
            convention defined here:
            https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
        :type destination_cloud_storage_uris: list
        :param compression: Type of compression to use.
        :type compression: string
        :param export_format: File format to export.
        :type export_format: string
        :param field_delimiter: The delimiter to use when extracting to a CSV.
        :type field_delimiter: string
        :param print_header: Whether to print a header for a CSV file extract.
        :type print_header: boolean
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
        }

        if export_format == 'CSV':
            # Only set fieldDelimiter and printHeader fields if using CSV.
            # Google does not like it if you set these fields for other export
            # formats.
            configuration['extract']['fieldDelimiter'] = field_delimiter
            configuration['extract']['printHeader'] = print_header

        return self.run_with_configuration(configuration)

    def run_copy(self,
                 source_project_dataset_tables,
                 destination_project_dataset_table,
                 write_disposition='WRITE_EMPTY',
                 create_disposition='CREATE_IF_NEEDED'):
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
        :type destination_project_dataset_table: string
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: string
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: string
        """
        source_project_dataset_tables = (
            [source_project_dataset_tables]
            if not isinstance(source_project_dataset_tables, list)
            else source_project_dataset_tables)

        source_project_dataset_tables_fixup = []
        for source_project_dataset_table in source_project_dataset_tables:
            source_project, source_dataset, source_table = \
                _split_tablename(table_input=source_project_dataset_table,
                                 default_project_id=self.project_id,
                                 var_name='source_project_dataset_table')
            source_project_dataset_tables_fixup.append({
                'projectId': source_project,
                'datasetId': source_dataset,
                'tableId': source_table
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

        return self.run_with_configuration(configuration)

    def run_load(self,
                 destination_project_dataset_table,
                 schema_fields, source_uris,
                 source_format='CSV',
                 create_disposition='CREATE_IF_NEEDED',
                 skip_leading_rows=0,
                 write_disposition='WRITE_EMPTY',
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 schema_update_options=(),
                 src_fmt_configs={}):
        """
        Executes a BigQuery load command to load data from Google Cloud Storage
        to BigQuery. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param destination_project_dataset_table:
            The dotted (<project>.|<project>:)<dataset>.<table> BigQuery table to load
            data into. If <project> is not included, project will be the project defined
            in the connection json.
        :type destination_project_dataset_table: string
        :param schema_fields: The schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
        :type schema_fields: list
        :param source_uris: The source Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). A single wild
            per-object name can be used.
        :type source_uris: list
        :param source_format: File format to export.
        :type source_format: string
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: string
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :type skip_leading_rows: int
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: string
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :type field_delimiter: string
        :param max_bad_records: The maximum number of bad records that BigQuery can
            ignore when running the job.
        :type max_bad_records: int
        :param quote_character: The value that is used to quote data sections in a CSV file.
        :type quote_character: string
        :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
        :type allow_quoted_newlines: boolean
        :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
            The missing values are treated as nulls. If false, records with missing trailing columns
            are treated as bad records, and if there are too many bad records, an invalid error is
            returned in the job result. Only applicable when soure_format is CSV.
        :type allow_jagged_rows: bool
        :param schema_update_options: Allows the schema of the desitination
            table to be updated as a side effect of the load job.
        :type schema_update_options: list
        :param src_fmt_configs: configure optional fields specific to the source format
        :type src_fmt_configs: dict
        """

        # bigquery only allows certain source formats
        # we check to make sure the passed source format is valid
        # if it's not, we raise a ValueError
        # Refer to this link for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.query.tableDefinitions.(key).sourceFormat
        source_format = source_format.upper()
        allowed_formats = ["CSV", "NEWLINE_DELIMITED_JSON", "AVRO", "GOOGLE_SHEETS", "DATASTORE_BACKUP"]
        if source_format not in allowed_formats:
            raise ValueError("{0} is not a valid source format. "
                    "Please use one of the following types: {1}"
                    .format(source_format, allowed_formats))

        # bigquery also allows you to define how you want a table's schema to change
        # as a side effect of a load
        # for more details:
        #   https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.schemaUpdateOptions
        allowed_schema_update_options = [
            'ALLOW_FIELD_ADDITION',
            "ALLOW_FIELD_RELAXATION"
        ]
        if not set(allowed_schema_update_options).issuperset(set(schema_update_options)):
            raise ValueError(
                "{0} contains invalid schema update options. "
                "Please only use one or more of the following options: {1}"
                .format(schema_update_options, allowed_schema_update_options)
            )

        destination_project, destination_dataset, destination_table = \
            _split_tablename(table_input=destination_project_dataset_table,
                             default_project_id=self.project_id,
                             var_name='destination_project_dataset_table')

        configuration = {
            'load': {
                'createDisposition': create_disposition,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                },
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'writeDisposition': write_disposition,
            }
        }
        if schema_fields:
            configuration['load']['schema'] = {
                'fields': schema_fields
            }

        if schema_update_options:
            if write_disposition not in ["WRITE_APPEND", "WRITE_TRUNCATE"]:
                raise ValueError(
                    "schema_update_options is only "
                    "allowed if write_disposition is "
                    "'WRITE_APPEND' or 'WRITE_TRUNCATE'."
                )
            else:
                self.log.info(
                    "Adding experimental "
                    "'schemaUpdateOptions': {0}".format(schema_update_options)
                )
                configuration['load']['schemaUpdateOptions'] = schema_update_options

        if max_bad_records:
            configuration['load']['maxBadRecords'] = max_bad_records

        # if following fields are not specified in src_fmt_configs,
        # honor the top-level params for backward-compatibility
        if 'skipLeadingRows' not in src_fmt_configs:
            src_fmt_configs['skipLeadingRows'] = skip_leading_rows
        if 'fieldDelimiter' not in src_fmt_configs:
            src_fmt_configs['fieldDelimiter'] = field_delimiter
        if quote_character:
            src_fmt_configs['quote'] = quote_character
        if allow_quoted_newlines:
            src_fmt_configs['allowQuotedNewlines'] = allow_quoted_newlines

        src_fmt_to_configs_mapping = {
            'CSV': ['allowJaggedRows', 'allowQuotedNewlines', 'autodetect',
                    'fieldDelimiter', 'skipLeadingRows', 'ignoreUnknownValues',
                    'nullMarker', 'quote'],
            'DATASTORE_BACKUP': ['projectionFields'],
            'NEWLINE_DELIMITED_JSON': ['autodetect', 'ignoreUnknownValues'],
            'AVRO': [],
        }
        valid_configs = src_fmt_to_configs_mapping[source_format]
        src_fmt_configs = {k: v for k, v in src_fmt_configs.items()
                           if k in valid_configs}
        configuration['load'].update(src_fmt_configs)

        if allow_jagged_rows:
            configuration['load']['allowJaggedRows'] = allow_jagged_rows

        return self.run_with_configuration(configuration)

    def run_with_configuration(self, configuration):
        """
        Executes a BigQuery SQL query. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about the configuration parameter.

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        """
        jobs = self.service.jobs()
        job_data = {
            'configuration': configuration
        }

        # Send query and wait for reply.
        query_reply = jobs \
            .insert(projectId=self.project_id, body=job_data) \
            .execute()
        self.running_job_id = query_reply['jobReference']['jobId']

        # Wait for query to finish.
        keep_polling_job = True
        while (keep_polling_job):
            try:
                job = jobs.get(projectId=self.project_id, jobId=self.running_job_id).execute()
                if (job['status']['state'] == 'DONE'):
                    keep_polling_job = False
                    # Check if job had errors.
                    if 'errorResult' in job['status']:
                        raise Exception(
                            'BigQuery job failed. Final error was: {}. The job was: {}'.format(
                                job['status']['errorResult'], job
                            )
                        )
                else:
                    self.log.info('Waiting for job to complete : %s, %s', self.project_id, self.running_job_id)
                    time.sleep(5)

            except HttpError as err:
                if err.resp.status in [500, 503]:
                    self.log.info('%s: Retryable error, waiting for job to complete: %s', err.resp.status, self.running_job_id)
                    time.sleep(5)
                else:
                    raise Exception(
                        'BigQuery job status check failed. Final error was: %s', err.resp.status)

        return self.running_job_id
        
    def poll_job_complete(self, job_id):
        jobs = self.service.jobs()
        try:
            job = jobs.get(projectId=self.project_id, jobId=job_id).execute()
            if (job['status']['state'] == 'DONE'):
                return True
        except HttpError as err:
            if err.resp.status in [500, 503]:
                self.log.info('%s: Retryable error while polling job with id %s', err.resp.status, job_id)
            else:
                raise Exception(
                    'BigQuery job status check failed. Final error was: %s', err.resp.status)
        return False
      
        
    def cancel_query(self):
        """
        Cancel all started queries that have not yet completed
        """
        jobs = self.service.jobs()
        if (self.running_job_id and not self.poll_job_complete(self.running_job_id)):
            self.log.info('Attempting to cancel job : %s, %s', self.project_id, self.running_job_id)
            jobs.cancel(projectId=self.project_id, jobId=self.running_job_id).execute()
        else:
            self.log.info('No running BigQuery jobs to cancel.')
            return
        
        # Wait for all the calls to cancel to finish
        max_polling_attempts = 12
        polling_attempts = 0
        
        job_complete = False
        while (polling_attempts < max_polling_attempts and not job_complete):
            polling_attempts = polling_attempts+1
            job_complete = self.poll_job_complete(self.running_job_id)
            if (job_complete):
                self.log.info('Job successfully canceled: %s, %s', self.project_id, self.running_job_id)
            elif(polling_attempts == max_polling_attempts):
                self.log.info('Stopping polling due to timeout. Job with id %s has not completed cancel and may or may not finish.', self.running_job_id)
            else:
                self.log.info('Waiting for canceled job with id %s to finish.', self.running_job_id)
                time.sleep(5)

    def get_schema(self, dataset_id, table_id):
        """
        Get the schema for a given datset.table.
        see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource

        :param dataset_id: the dataset ID of the requested table
        :param table_id: the table ID of the requested table
        :return: a table schema
        """
        tables_resource = self.service.tables() \
            .get(projectId=self.project_id, datasetId=dataset_id, tableId=table_id) \
            .execute()
        return tables_resource['schema']

    def get_tabledata(self, dataset_id, table_id,
                      max_results=None, page_token=None, start_index=None):
        """
        Get the data of a given dataset.table.
        see https://cloud.google.com/bigquery/docs/reference/v2/tabledata/list

        :param dataset_id: the dataset ID of the requested table.
        :param table_id: the table ID of the requested table.
        :param max_results: the maximum results to return.
        :param page_token: page token, returned from a previous call,
            identifying the result set.
        :param start_index: zero based index of the starting row to read.
        :return: map containing the requested rows.
        """
        optional_params = {}
        if max_results:
            optional_params['maxResults'] = max_results
        if page_token:
            optional_params['pageToken'] = page_token
        if start_index:
            optional_params['startIndex'] = start_index
        return (
            self.service.tabledata()
            .list(
                projectId=self.project_id, datasetId=dataset_id,
                tableId=table_id, **optional_params)
            .execute()
        )

    def run_table_delete(self, deletion_dataset_table, ignore_if_missing=False):
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
        :type ignore_if_missing: boolean
        :return:
        """

        assert '.' in deletion_dataset_table, (
            'Expected deletion_dataset_table in the format of '
            '<dataset>.<table>. Got: {}').format(deletion_dataset_table)
        deletion_project, deletion_dataset, deletion_table = \
            _split_tablename(table_input=deletion_dataset_table,
                             default_project_id=self.project_id)

        try:
            tables_resource = self.service.tables() \
                .delete(projectId=deletion_project,
                        datasetId=deletion_dataset,
                        tableId=deletion_table) \
                .execute()
            self.log.info('Deleted table %s:%s.%s.',
                          deletion_project, deletion_dataset, deletion_table)
        except HttpError:
            if not ignore_if_missing:
                raise Exception(
                    'Table deletion failed. Table does not exist.')
            else:
                self.log.info('Table does not exist. Skipping.')


    def run_table_upsert(self, dataset_id, table_resource, project_id=None):
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
        tables_list_resp = self.service.tables().list(projectId=project_id,
                                                      datasetId=dataset_id).execute()
        while True:
            for table in tables_list_resp.get('tables', []):
                if table['tableReference']['tableId'] == table_id:
                    # found the table, do update
                    self.log.info(
                        'Table %s:%s.%s exists, updating.',
                        project_id, dataset_id, table_id
                    )
                    return self.service.tables().update(projectId=project_id,
                                                        datasetId=dataset_id,
                                                        tableId=table_id,
                                                        body=table_resource).execute()
            # If there is a next page, we need to check the next page.
            if 'nextPageToken' in tables_list_resp:
                tables_list_resp = self.service.tables()\
                    .list(projectId=project_id,
                          datasetId=dataset_id,
                          pageToken=tables_list_resp['nextPageToken'])\
                    .execute()
            # If there is no next page, then the table doesn't exist.
            else:
                # do insert
                self.log.info(
                    'Table %s:%s.%s does not exist. creating.',
                    project_id, dataset_id, table_id
                )
                return self.service.tables().insert(projectId=project_id,
                                                    datasetId=dataset_id,
                                                    body=table_resource).execute()

    def run_grant_dataset_view_access(self,
                                      source_dataset,
                                      view_dataset,
                                      view_table,
                                      source_project = None,
                                      view_project = None):
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
        source_dataset_resource = self.service.datasets().get(projectId=source_project,
                                                              datasetId=source_dataset).execute()
        access = source_dataset_resource['access'] if 'access' in source_dataset_resource else []
        view_access = {'view': {'projectId': view_project,
                                'datasetId': view_dataset,
                                'tableId': view_table}}
        # check to see if the view we want to add already exists.
        if view_access not in access:
            self.log.info(
                'Granting table %s:%s.%s authorized view access to %s:%s dataset.',
                view_project, view_dataset, view_table, source_project, source_dataset
            )
            access.append(view_access)
            return self.service.datasets().patch(projectId=source_project,
                                                 datasetId=source_dataset,
                                                 body={'access': access}).execute()
        else:
            # if view is already in access, do nothing.
            self.log.info(
                'Table %s:%s.%s already has authorized view access to %s:%s dataset.',
                view_project, view_dataset, view_table, source_project, source_dataset
            )
            return source_dataset_resource


class BigQueryCursor(BigQueryBaseCursor):
    """
    A very basic BigQuery PEP 249 cursor implementation. The PyHive PEP 249
    implementation was used as a reference:

    https://github.com/dropbox/PyHive/blob/master/pyhive/presto.py
    https://github.com/dropbox/PyHive/blob/master/pyhive/common.py
    """

    def __init__(self, service, project_id):
        super(BigQueryCursor, self).__init__(service=service, project_id=project_id)
        self.buffersize = None
        self.page_token = None
        self.job_id = None
        self.buffer = []
        self.all_pages_loaded = False

    @property
    def description(self):
        """ The schema description method is not currently implemented. """
        raise NotImplementedError

    def close(self):
        """ By default, do nothing """
        pass

    @property
    def rowcount(self):
        """ By default, return -1 to indicate that this is not supported. """
        return -1

    def execute(self, operation, parameters=None):
        """
        Executes a BigQuery query, and returns the job ID.

        :param operation: The query to execute.
        :type operation: string
        :param parameters: Parameters to substitute into the query.
        :type parameters: dict
        """
        bql = _bind_parameters(operation, parameters) if parameters else operation
        self.job_id = self.run_query(bql)

    def executemany(self, operation, seq_of_parameters):
        """
        Execute a BigQuery query multiple times with different parameters.

        :param operation: The query to execute.
        :type operation: string
        :param parameters: List of dictionary parameters to substitute into the
            query.
        :type parameters: list
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        """ Fetch the next row of a query result set. """
        return self.next()

    def next(self):
        """
        Helper method for fetchone, which returns the next row from a buffer.
        If the buffer is empty, attempts to paginate through the result set for
        the next page, and load it into the buffer.
        """
        if not self.job_id:
            return None

        if len(self.buffer) == 0:
            if self.all_pages_loaded:
                return None

            query_results = (
                self.service.jobs()
                .getQueryResults(
                    projectId=self.project_id,
                    jobId=self.job_id,
                    pageToken=self.page_token)
                .execute()
            )

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
                self.page_token = None
                self.job_id = None
                self.page_token = None
                return None

        return self.buffer.pop(0)

    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of sequences (e.g. a
        list of tuples). An empty sequence is returned when no more rows are available.
        The number of rows to fetch per call is specified by the parameter. If it is not given, the
        cursor's arraysize determines the number of rows to be fetched. The method should try to
        fetch as many rows as indicated by the size parameter. If this is not possible due to the
        specified number of rows not being available, fewer rows may be returned.
        An :py:class:`~pyhive.exc.Error` (or subclass) exception is raised if the previous call to
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

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a sequence of sequences
        (e.g. a list of tuples).
        """
        result = []
        while True:
            one = self.fetchone()
            if one is None:
                break
            else:
                result.append(one)
        return result

    def get_arraysize(self):
        """ Specifies the number of rows to fetch at a time with .fetchmany() """
        return self._buffersize if self.buffersize else 1

    def set_arraysize(self, arraysize):
        """ Specifies the number of rows to fetch at a time with .fetchmany() """
        self.buffersize = arraysize

    arraysize = property(get_arraysize, set_arraysize)

    def setinputsizes(self, sizes):
        """ Does nothing by default """
        pass

    def setoutputsize(self, size, column=None):
        """ Does nothing by default """
        pass


def _bind_parameters(operation, parameters):
    """ Helper method that binds parameters to a SQL query. """
    # inspired by MySQL Python Connector (conversion.py)
    string_parameters = {}
    for (name, value) in parameters.iteritems():
        if value is None:
            string_parameters[name] = 'NULL'
        elif isinstance(value, basestring):
            string_parameters[name] = "'" + _escape(value) + "'"
        else:
            string_parameters[name] = str(value)
    return operation % string_parameters


def _escape(s):
    """ Helper method that escapes parameters to a SQL query. """
    e = s
    e = e.replace('\\', '\\\\')
    e = e.replace('\n', '\\n')
    e = e.replace('\r', '\\r')
    e = e.replace("'", "\\'")
    e = e.replace('"', '\\"')
    return e


def _bq_cast(string_field, bq_type):
    """
    Helper method that casts a BigQuery row to the appropriate data types.
    This is useful because BigQuery returns all fields as strings.
    """
    if string_field is None:
        return None
    elif bq_type == 'INTEGER' or bq_type == 'TIMESTAMP':
        return int(string_field)
    elif bq_type == 'FLOAT':
        return float(string_field)
    elif bq_type == 'BOOLEAN':
        assert string_field in set(['true', 'false'])
        return string_field == 'true'
    else:
        return string_field


def _split_tablename(table_input, default_project_id, var_name=None):
    assert default_project_id is not None, "INTERNAL: No default project is specified"

    def var_print(var_name):
        if var_name is None:
            return ""
        else:
            return "Format exception for {var}: ".format(var=var_name)

    if table_input.count('.') + table_input.count(':') > 3:
        raise Exception((
            '{var}Use either : or . to specify project '
            'got {input}'
        ).format(var=var_print(var_name), input=table_input))

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
        raise Exception((
            '{var}Expect format of (<project:)<dataset>.<table>, '
            'got {input}'
        ).format(var=var_print(var_name), input=table_input))

    cmpt = rest.split('.')
    if len(cmpt) == 3:
        assert project_id is None, (
            "{var}Use either : or . to specify project"
        ).format(var=var_print(var_name))
        project_id = cmpt[0]
        dataset_id = cmpt[1]
        table_id = cmpt[2]

    elif len(cmpt) == 2:
        dataset_id = cmpt[0]
        table_id = cmpt[1]
    else:
        raise Exception((
            '{var}Expect format of (<project.|<project:)<dataset>.<table>, '
            'got {input}'
        ).format(var=var_print(var_name), input=table_input))

    if project_id is None:
        if var_name is not None:
            log = LoggingMixin().log
            log.info(
                'Project not included in {var}: {input}; using project "{project}"'.format(
                    var=var_name, input=table_input, project=default_project_id
                )
            )
        project_id = default_project_id

    return project_id, dataset_id, table_id
