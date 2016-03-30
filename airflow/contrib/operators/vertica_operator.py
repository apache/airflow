import logging

from airflow.contrib.hooks import VerticaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class VerticaOperator(BaseOperator):
    """
    Executes sql code in a specific Vertica database

    :param vertica_conn_id: reference to a specific Vertica database
    :type vertica_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#b4e0ff'

    @apply_defaults
    def __init__(self, sql, vertica_conn_id='vertica_default', *args, **kwargs):
        super(VerticaOperator, self).__init__(*args, **kwargs)
        self.vertica_conn_id = vertica_conn_id
        self.sql = sql

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        hook.run(self.sql)
