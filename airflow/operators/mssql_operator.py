import logging

from airflow.hooks import MsSqlHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class MsSqlOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self, sql, mssql_conn_id='mssql_default', *args, **kwargs):
        super(MsSqlOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql

    def execute(self, context):
        logging.info('Executing: ' + self.sql)
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        hook.run(self.sql)