import httplib2
import logging
import pandas

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

    def __init__(self, bigquery_conn_id="bigquery_default"):
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
        Returns a Pandas DataFrame for the results produced by a BigQuery query.
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
