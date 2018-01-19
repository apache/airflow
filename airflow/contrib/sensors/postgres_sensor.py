import logging
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class PostgresTableSensor(BaseSensorOperator):
    """
    Checks if the postgres table exists.

    :param table: The table to check it's existence.
    :type table: string
    :param conn_id: The connection to run the sensor against
    :type conn_id: string
    """
    template_fields = ('table', 'conn_id')
    ui_color = '#669AC4'

    @apply_defaults
    def __init__(self, table, conn_id='postgres_default', *args, **kwargs):
        self.table = table
        self.conn_id = conn_id
        super(PostgresTableSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        logging.info("Poking: Checking if table '{}' exists.".format(self.table))
        exists_query = "select exists(select * from information_schema.tables where table_name='{}')".format(self.table)
        ret_val = self.hook.get_first(sql=exists_query)
        return ret_val[0]
