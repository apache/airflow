"""
This module contains a BigQuery Hook, as well as a very basic PEP 249
implementation for BigQuery.
"""

import httplib2
import logging
import pandas
import time

from airflow.contrib.hooks.gc_base_hook import GoogleCloudBaseHook
from airflow.hooks.dbapi_hook import DbApiHook
from apiclient.discovery import build
from pandas.io.gbq import GbqConnector, _parse_data as gbq_parse_data
from pandas.tools.merge import concat

logging.getLogger("bigquery").setLevel(logging.INFO)

class BigQueryHook(GoogleCloudBaseHook, DbApiHook):
    """
    Interact with BigQuery. Connections must be defined with an extras JSON 
    field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }

    If you have used ``gcloud auth`` to authenticate on the machine that's 
    running Airflow, you can exclude the service_account and key_path 
    parameters.
    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self,
                 scope='https://www.googleapis.com/auth/bigquery',
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None):
        """
        :param scope: The scope of the hook.
        :type scope: string
        """
        super(BigQueryHook, self).__init__(
            scope=scope,
            conn_id=bigquery_conn_id,
            delegate_to=delegate_to)

    def get_conn(self):
        """
        Returns a BigQuery PEP 249 connection object.
        """
        service = self.get_service()
        connection_extras = self._extras_dejson()
        project = connection_extras['project']
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

    def get_pandas_df(self, bql, parameters=None):
        """
        Returns a Pandas DataFrame for the results produced by a BigQuery
        query. The DbApiHook method must be overridden because Pandas
        doesn't support PEP 249 connections, except for SQLite. See:

        https://github.com/pydata/pandas/blob/master/pandas/io/sql.py#L447
        https://github.com/pydata/pandas/issues/6900

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        """
        service = self.get_service()
        connection_extras = self._extras_dejson()
        project = connection_extras['project']
        connector = BigQueryPandasConnector(project, service)
        schema, pages = connector.run_query(bql, verbose=False)
        dataframe_list = []

        while len(pages) > 0:
            page = pages.pop()
            dataframe_list.append(gbq_parse_data(schema, page))

        if len(dataframe_list) > 0:
            return concat(dataframe_list, ignore_index=True)
        else:
            return gbq_parse_data(schema, [])

class BigQueryPandasConnector(GbqConnector):
    """
    This connector behaves identically to GbqConnector (from Pandas), except
    that it allows the service to be injected, and disables a call to
    self.get_credentials(). This allows Airflow to use BigQuery with Pandas
    without forcing a three legged OAuth connection. Instead, we can inject
    service account credentials into the binding.
    """
    def __init__(self, project_id, service, reauth=False):
        self.test_google_api_imports()
        self.project_id = project_id
        self.reauth = reauth
        self.service = service

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
        raise NotSupportedError("BigQueryConnection does not have transactions")

class BigQueryBaseCursor(object):
    """
    The BigQuery base cursor contains helper methods to execute queries against
    BigQuery. The methods can be used directly by operators, in cases where a
    PEP 249 cursor isn't needed.
    """

    def __init__(self, service, project_id):
        self.service = service
        self.project_id = project_id

    def run_query(self, bql, destination_dataset_table = False, write_disposition = 'WRITE_EMPTY', allow_large_results=False):
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
        :param allow_large_results: Whether to allow large results.
        :type allow_large_results: boolean
        """
        configuration = {
            'query': {
                'query': bql,
            }
        }

        if destination_dataset_table:
            assert '.' in destination_dataset_table, \
                'Expected destination_dataset_table in the format of <dataset>.<table>. Got: {}'.format(destination_dataset_table)
            destination_dataset, destination_table = destination_dataset_table.split('.', 1)
            configuration['query'].update({
                'allowLargeResults': allow_large_results,
                'writeDisposition': write_disposition,
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                }
            })

        return self.run_with_configuration(configuration)

    def run_extract(self, source_dataset_table, destination_cloud_storage_uris, compression='NONE', export_format='CSV', field_delimiter=',', print_header=True):
        """
        Executes a BigQuery extract command to copy data from BigQuery to 
        Google Cloud Storage. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param source_dataset_table: The dotted <dataset>.<table> BigQuery table to use as the source data.
        :type source_dataset_table: string
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
        assert '.' in source_dataset_table, \
            'Expected source_dataset_table in the format of <dataset>.<table>. Got: {}'.format(source_dataset_table)

        source_dataset, source_table = source_dataset_table.split('.', 1)
        configuration = {
            'extract': {
                'sourceTable': {
                    'projectId': self.project_id,
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

    def run_copy(self, source_dataset_tables, destination_project_dataset_table, write_disposition='WRITE_EMPTY', create_disposition='CREATE_IF_NEEDED'):
        """
        Executes a BigQuery copy command to copy data from one BigQuery table
        to another. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

        For more details about these parameters.

        :param source_dataset_tables: One or more dotted <dataset>.<table>
            BigQuery tables to use as the source data. Use a list if there are
            multiple source tables.
        :type source_dataset_tables: list|string
        :param destination_project_dataset_table: The destination BigQuery
            table. Format is: <project>.<dataset>.<table>
        :type destination_project_dataset_table: string
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: string
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: string
        """
        source_dataset_tables = [source_dataset_tables] if not isinstance(source_dataset_tables, list) else source_dataset_tables
        source_project_dataset_tables = []

        for source_dataset_table in source_dataset_tables:
            assert '.' in source_dataset_table, \
                'Expected source_dataset_table in the format of <dataset>.<table>. Got: {}'.format(source_dataset_table)

            source_dataset, source_table = source_dataset_table.split('.', 1)
            source_project_dataset_tables.append({
                'projectId': self.project_id,
                'datasetId': source_dataset,
                'tableId': source_table
            })

        assert 3 == len(destination_project_dataset_table.split('.')), \
            'Expected destination_project_dataset_table in the format of <project>.<dataset>.<table>. Got: {}'.format(destination_project_dataset_table)

        destination_project, destination_dataset, destination_table = destination_project_dataset_table.split('.', 2)
        configuration = {
            'copy': {
                'createDisposition': create_disposition,
                'writeDisposition': write_disposition,
                'sourceTables': source_project_dataset_tables,
                'destinationTable': {
                    'projectId': destination_project,
                    'datasetId': destination_dataset,
                    'tableId': destination_table
                }
            }
        }

        return self.run_with_configuration(configuration)

    def run_load(self, destination_dataset_table, schema_fields, source_uris, source_format='CSV', create_disposition='CREATE_IF_NEEDED', skip_leading_rows=0, write_disposition='WRITE_EMPTY', field_delimiter=','):
        """
        Executes a BigQuery load command to load data from Google Cloud Storage
        to BigQuery. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param destination_dataset_table: The dotted <dataset>.<table> BigQuery table to load data into.
        :type destination_dataset_table: string
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
        """
        assert '.' in destination_dataset_table, \
            'Expected destination_dataset_table in the format of <dataset>.<table>. Got: {}'.format(destination_dataset_table)

        destination_dataset, destination_table = destination_dataset_table.split('.', 1)

        configuration = {
            'load': {
                'createDisposition': create_disposition,
                'destinationTable': {
                    'projectId': self.project_id,
                    'datasetId': destination_dataset,
                    'tableId': destination_table,
                },
                'schema': {
                    'fields': schema_fields
                },
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'writeDisposition': write_disposition,
            }
        }

        if source_format == 'CSV':
            configuration['load']['skipLeadingRows'] = skip_leading_rows
            configuration['load']['fieldDelimiter'] = field_delimiter

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
        job_id = query_reply['jobReference']['jobId']
        job = jobs.get(projectId=self.project_id, jobId=job_id).execute()

        # Wait for query to finish.
        while not job['status']['state'] == 'DONE':
            logging.info('Waiting for job to complete: %s, %s', self.project_id, job_id)
            time.sleep(5)
            job = jobs.get(projectId=self.project_id, jobId=job_id).execute()

        # Check if job had errors.
        if 'errorResult' in job['status']:
            raise Exception('BigQuery job failed. Final error was: %s', job['status']['errorResult'])

        return job_id

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
        :param page_token: page token, returned from a previous call, identifying the result set.
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
        return self.service.tabledata() \
            .list(projectId=self.project_id, datasetId=dataset_id, tableId=table_id, **optional_params) \
            .execute()


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
            query_results = self.service.jobs().getQueryResults(projectId=self.project_id, jobId=self.job_id, pageToken=self.page_token).execute()

            if len(query_results['rows']) == 0:
                # Reset all state since we've exhausted the results.
                self.page_token = None
                self.job_id = None
                self.page_token = None
                return None
            else:
                self.page_token = query_results.get('pageToken')
                fields = query_results['schema']['fields']
                col_types = [field['type'] for field in fields]
                rows = query_results['rows']

                for idx, dict_row in enumerate(rows):
                    typed_row = [_bq_cast(vs['v'], col_types[idx]) for vs in dict_row['f']]
                    self.buffer.append(typed_row)

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
        for _ in xrange(size):
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
        return bool(string_field)
    else:
        return string_field