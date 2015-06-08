import logging

from airflow.hooks import SqliteHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class SqliteOperator(BaseOperator):
    """
    Executes sql code in a specific Sqlite database

    :param sqlite_conn_id: reference to a specific sqlite database
    :type sqlite_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#cdaaed'

    @apply_defaults
    def __init__(self, sql, sqlite_conn_id='sqlite_default', *args, **kwargs):
        super(SqliteOperator, self).__init__(*args, **kwargs)
        self.sqlite_conn_id = sqlite_conn_id
        self.sql = sql

    def execute(self, context):
        logging.info('Executing: ' + self.sql)
        hook = SqliteHook(sqlite_conn_id=self.sqlite_conn_id)
        hook.run(self.sql)
