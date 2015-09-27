import logging

from airflow.hooks import PostgresHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class PostgresOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(PostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
