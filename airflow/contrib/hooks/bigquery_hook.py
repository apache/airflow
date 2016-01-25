import httplib2
import logging
import pandas
import time

from airflow.hooks.base_hook import BaseHook
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
from pandas.io.gbq import GbqConnector, _parse_data as gbq_parse_data
from pandas.tools.merge import concat

logging.getLogger("bigquery").setLevel(logging.INFO)

BQ_SCOPE = 'https://www.googleapis.com/auth/bigquery'

class BigQueryHook(BaseHook):
    """
    Interact with BigQuery. Connections must be defined with an extras JSON field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }
    """
    conn_name_attr = 'bigquery_conn_id'

    def __init__(self, bigquery_conn_id='bigquery_default'):
        self.bigquery_conn_id = bigquery_conn_id

    def get_conn(self):
        """
        Returns a BigQuery service object.
        """
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        service_account = connection_extras['service_account']
        key_path = connection_extras['key_path']

        with file(key_path, 'rb') as key_file:
            key = key_file.read()

        credentials = SignedJwtAssertionCredentials(
            service_account,
            key,
            scope=BQ_SCOPE)
            # TODO Support domain delegation, which will allow us to set a sub-account to execute as. We can then
            # pass DAG owner emails into the connection_info, and use it here.
            # sub='some@email.com')

        http = httplib2.Http()
        http_authorized = credentials.authorize(http)
        service = build('bigquery', 'v2', http=http_authorized)

        return service

    def get_pandas_df(self, bql, parameters=None):
        """
        Returns a Pandas DataFrame for the results produced by a BigQuery 
        query.

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        """
        service = self.get_conn()
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
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

    def run(self, bql, destination_dataset_table = False, write_disposition = 'WRITE_EMPTY'):
        """
        Executes a BigQuery SQL query. Either returns results and schema, or
        stores results in a BigQuery table, if destination is set. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param bql: The BigQuery SQL to execute.
        :type bql: string
        :param destination_dataset_table: The dotted <dataset>.<table> 
            BigQuery table to save the query results.
        :param write_disposition: What to do if the table already exists in 
            BigQuery.
        """
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        project = connection_extras['project']
        configuration = {
            'query': {
                'query': bql,
                'writeDisposition': write_disposition,
            }
        }

        if destination_dataset_table:
            assert '.' in destination_dataset_table, \
                'Expected destination_dataset_table in the format of <dataset>.<table>. Got: {}'.format(destination_dataset_table)
            destination_dataset, destination_table = destination_dataset_table.split('.', 1)
            configuration['query']['destinationTable'] = {
                'projectId': project,
                'datasetId': destination_dataset,
                'tableId': destination_table,
            }

        self.run_with_configuration(configuration)

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
        :type field_delimiter: string
        :param field_delimiter: The delimiter to use when extracting to a CSV.
        :type field_delimiter: string
        :param print_header: Whether to print a header for a CSV file extract.
        :type print_header: boolean
        """
        assert '.' in source_dataset_table, \
            'Expected source_dataset_table in the format of <dataset>.<table>. Got: {}'.format(source_dataset_table)

        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        project = connection_extras['project']
        source_dataset, source_table = source_dataset_table.split('.', 1)
        configuration = {
            'extract': {
                'sourceTable': {
                    'projectId': project,
                    'datasetId': source_dataset,
                    'tableId': source_table,
                },
                'compression': compression,
                'destinationUris': destination_cloud_storage_uris,
                'destinationFormat': export_format,
                'fieldDelimiter': field_delimiter,
                'printHeader': print_header,
            }
        }

        self.run_with_configuration(configuration)

    def run_with_configuration(self, configuration):
        """
        Executes a BigQuery SQL query. See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about the configuration parameter.

        :param configuration: The configuration parameter maps directly to
        BigQuery's configuration field in the job object. See
        https://cloud.google.com/bigquery/docs/reference/v2/jobs for details.
        """
        service = self.get_conn()
        connection_info = self.get_connection(self.bigquery_conn_id)
        connection_extras = connection_info.extra_dejson
        project = connection_extras['project']
        jobs = service.jobs()
        job_data = {
            'configuration': configuration
        }

        # Send query and wait for reply.
        query_reply = jobs \
            .insert(projectId=project, body=job_data) \
            .execute()
        job_id = query_reply['jobReference']['jobId']
        job = jobs.get(projectId=project, jobId=job_id).execute()

        # Wait for query to finish.
        while not job['status']['state'] == 'DONE':
            logging.info('Waiting for job to complete: %s, %s', project, job_id)
            time.sleep(5)
            job = jobs.get(projectId=project, jobId=job_id).execute()

        # Check if job had errors.
        if 'errorResult' in job['status']:
            raise Exception('BigQuery job failed. Final error was: %s', job['status']['errorResult'])

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
