from airflow.plugins_manager import AirflowPlugin

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.mysql_hook import MySqlHook


class BigQueryToMySqlOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and insert that data into a MySQL table.


    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'`` and passed through this form
        to MySQL

    **Example**: ::

       transfer_data = BigQueryToMySqlOperator(
            task_id='task_id',
            dataset_id='dataset_id',
            table_id='table_name',
            mysql_table='dest_table_name',
            replace=True,
        )

    :param dataset_id: The dataset ID of the requested table. (templated)
    :type dataset_id: string
    :param table_id: The table ID of the requested table. (templated)
    :type table_id: string
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :type max_results: string
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :type selected_fields: string
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param mysql_conn_id: reference to a specific mysql hook
    :type mysql_conn_id: string
    :param database: name of database which overwrite defined one in connection
    :type database: string
    :param replace: Whether to replace instead of insert
    :type replace: bool
    """
    template_fields = ('dataset_id', 'table_id', 'mysql_table')
    ui_color = '#fe66ff'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 mysql_table,
                 selected_fields=None,
                 bigquery_conn_id='bigquery_default',
                 mysql_conn_id='mysql_default',
                 mysql_parameters=None,
                 database=None,
                 delegate_to=None,
                 replace=False,
                 *args,
                 **kwargs):
        super(BigQueryToMySqlOperator, self).__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.selected_fields = selected_fields
        self.bigquery_conn_id = bigquery_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.mysql_parameters = mysql_parameters
        self.database = database
        self.mysql_table = mysql_table
        self.replace = replace
        self.delegate_to = delegate_to

    def bq_get_data(self, context):
        self.log.info('Fetching Data from:')
        self.log.info('Dataset: %s ; Table: %s',
                      self.dataset_id, self.table_id)

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)

        conn = hook.get_conn()
        cursor = conn.cursor()
        i = 0
        while True:
            # Max results is set to 1000 because bq job has an hardcoded limit to 1300 lines so let this be stable.
            response = cursor.get_tabledata(dataset_id=self.dataset_id,
                                            table_id=self.table_id,
                                            max_results=1000,
                                            selected_fields=self.selected_fields,
                                            start_index=i * 1000)

            if 'rows' in response:
                rows = response['rows']
            else:
                self.log.info('Job Finished')
                return

            self.log.info('Total Extracted rows: %s', len(rows) + i * 1000)

            table_data = []
            for dict_row in rows:
                single_row = []
                for fields in dict_row['f']:
                    single_row.append(fields['v'])
                table_data.append(single_row)

            yield table_data
            i += 1

    def insert_rows(self, rows):
        self.mysql_hook.insert_rows(self.mysql_table, rows, replace=self.replace)

    def init_mysql_hook(self):
        self.mysql_hook = MySqlHook(schema=self.database, mysql_conn_id=self.mysql_conn_id)

    def execute(self, context):

        self.init_mysql_hook()
        for rows in self.bq_get_data(context):
            self.insert_rows(rows)


class MySQL_Plugin(AirflowPlugin):
    name = "mysql_plugin"
    operators = [BigQueryToMySqlOperator]
