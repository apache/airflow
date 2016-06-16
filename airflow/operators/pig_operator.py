import logging
import re

from airflow.hooks import PigCliHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class PigOperator(BaseOperator):
    """
    Executes pig script.

    :param pig: the pig latin script to be executed
    :type pig: string
    :param pig_cli_conn_id: reference to the Hive database
    :type pig_cli_conn_id: string
    :param pigparams_jinja_translate: when True, pig params-type templating
        ${var} gets translated into jinja-type templating {{ var }}. Note that
        you may want to use this along with the
        ``DAG(user_defined_macros=myargs)`` parameter. View the DAG
        object documentation for more details.
    :type pigparams_jinja_translate: boolean
    """

    template_fields = ('pig',)
    template_ext = ('.pig', '.piglatin',)
    ui_color = '#f0e4ec'

    @apply_defaults
    def __init__(
            self, pig,
            pig_cli_conn_id='pig_cli_default',
            pigparams_jinja_translate=False,
            *args, **kwargs):

        super(PigOperator, self).__init__(*args, **kwargs)
        self.pigparams_jinja_translate = pigparams_jinja_translate
        self.pig = pig
        self.pig_cli_conn_id = pig_cli_conn_id

    def get_hook(self):
        return PigCliHook(pig_cli_conn_id=self.pig_cli_conn_id)

    def prepare_template(self):
        if self.pigparams_jinja_translate:
            self.pig = re.sub(
                "(\$([a-zA-Z_][a-zA-Z0-9_]*))", "{{ \g<2> }}", self.pig)

    def execute(self, context):
        logging.info('Executing: ' + self.pig)
        self.hook = self.get_hook()
        self.hook.run_cli(pig=self.pig)

    def on_kill(self):
        self.hook.kill()
