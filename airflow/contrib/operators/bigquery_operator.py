import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults

class BigQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database
    """
    template_fields = ('bql',)
    template_ext = ('.sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self, bql, destination_dataset_table = False, write_disposition = 'WRITE_EMPTY', bigquery_conn_id='bigquery_default', *args, **kwargs):
        """
        Create a new BigQueryOperator.

        :param bql: the sql code to be executed
        :type bql: Can receive a str representing a sql statement,
            a list of str (sql statements), or reference to a template file.
            Template reference are recognized by str ending in '.sql'
        :param destination_dataset_table: A dotted dataset.table that, if set,
            will store the results of the query.
        :type destination_dataset_table: string
        :param bigquery_conn_id: reference to a specific BigQuery hook.
        :type bigquery_conn_id: string
        """
        super(BigQueryOperator, self).__init__(*args, **kwargs)
        self.bql = bql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.bigquery_conn_id = bigquery_conn_id

    def execute(self, context):
        logging.info('Executing: %s', str(self.bql))
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id)
        hook.run(self.bql, self.destination_dataset_table, self.write_disposition)
