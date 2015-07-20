import logging
from datetime import timedelta

from airflow.hooks import ExasolHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class ExasolOperator(BaseOperator):
    """
    Executes sql code in a specific Exasol database

    :param exasol_conn_id: reference to a specific exasol database
    :type exasol_conn_id: string
    :param sql: the sql code to be executed
    :type sql: string or string pointing to a template file. File must have
        a '.sql' extensions.
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    # default retry parameters for ExasolOperator
    default_retries = 17
    default_retry_delay = timedelta(seconds=600)

    @apply_defaults
    def __init__(
            self, sql,
            exasol_conn_id='exasol_default', autocommit=False,
            *args, **kwargs):
        super(ExasolOperator, self).__init__(*args, **kwargs)

        if not 'retries' in kwargs or not kwargs['retries']:
            self.retries = self.default_retries

        if not 'retry_delay' in kwargs or not kwargs['retry_delay']:
            self.retry_delay = self.default_retry_delay

        self.sql = sql
        self.exasol_conn_id = exasol_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        logging.info('Executing: ' + self.sql)
        self.hook = ExasolHook(exasol_conn_id=self.exasol_conn_id)
        self.hook.run(self.sql, self.autocommit)
