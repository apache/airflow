
import logging

from airflow.hooks import HiveServer2Hook
from aiflow.contrib.hooks import VerticaHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class HiveToVerticaTransfer(BaseOperator):
    """
    Moves data from Hive to Vertica, note that for now the data is loaded
    into memory before being pushed to Vertica, so this operator should
    be used for smallish amount of data.
    
    :param sql: SQL query to execute against the Vertica database
    :type sql: str
    :param vertica_table: target Vertica table
    :type vertica_table: str
    :param vertica_conn_id: source Vertica connection
    :type vertica_conn_id: str
    :param hiveserver2_conn_id: destination hive connection
    :type hiveserver2_conn_id: str
    :param vertica_preoperator: sql statement to run against Vertica prior to
        import, typically use to truncate of delete in place of the data
        coming in, allowing the task to be idempotent (running the task
        twice won't double load data)
    :type vertica_preoperator: str
    """

    template_fields = ('sql', 'vertica_table', 'vertica_preoperator')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            vertica_table,
            hiveserver2_conn_id='hiveserver2_default',
            vertica_conn_id='vertica_default',
            vertica_preoperator=None,
            *args, **kwargs):
        super(HiveToVerticaTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.vertica_table = vertica_table
        self.vertica_conn_id = vertica_conn_id
        self.vertica_preoperator = vertica_preoperator
        self.hiveserver2_conn_id = hiveserver2_conn_id

    def execute(self, context):
        hive = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)
        logging.info("Extracting data from Hive")
        logging.info(self.sql)
        results = hive.get_records(self.sql)

        vertica = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        if self.vertica_preoperator:
            logging.info("Running Vertica preoperator")
            logging.info(self.vertica_preoperator)
            vertica.run(self.vertica_preoperator)

        logging.info("Inserting rows into Vertica")
        vertica.insert_rows(table=self.vertica_table, rows=results)