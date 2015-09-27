import logging

from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.hooks.base_hook import BaseHook


class GenericTransfer(BaseOperator):
    """
    Moves data from a connection to another, assuming that they both
    provide the required methods in their respective hooks. The source hook
    needs to expose a `get_records` method, and the destination a
    `insert_rows` method.

    This is mean to be used on small-ish datasets that fit in memory.

    :param sql: SQL query to execute against the source database
    :type sql: str
    :param destination_table: target table
    :type destination_table: str
    :param source_conn_id: source connection
    :type source_conn_id: str
    :param destination_conn_id: source connection
    :type destination_conn_id: str
    :param preoperator: sql statement or list of statements to be
        executed prior to loading the data
    :type preoperator: str or list of str
    """

    template_fields = ('sql', 'destination_table', 'preoperator')
    template_ext = ('.sql', '.hql',)
    ui_color = '#b0f07c'

    @apply_defaults
    def __init__(
            self,
            sql,
            destination_table,
            source_conn_id,
            destination_conn_id,
            preoperator=None,
            *args, **kwargs):
        super(GenericTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.preoperator = preoperator

    def execute(self, context):
        source_hook = BaseHook.get_hook(self.source_conn_id)

        logging.info("Extracting data from {}".format(self.source_conn_id))
        logging.info("Executing: \n" + self.sql)
        results = source_hook.get_records(self.sql)

        destination_hook = BaseHook.get_hook(self.destination_conn_id)
        if self.preoperator:
            logging.info("Running preoperator")
            logging.info(self.preoperator)
            destination_hook.run(self.preoperator)

        logging.info("Inserting rows into {}".format(self.destination_conn_id))
        destination_hook.insert_rows(table=self.destination_table, rows=results)
