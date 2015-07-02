import logging

from airflow.hooks import HttpHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults, AirflowException


class SimpleHttpOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    """

    template_fields = ('endpoint',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers={},
                 response_check=None,
                 http_conn_id='http_default', *args, **kwargs):
        super(SimpleHttpOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.data = data
        self.headers = headers
        self.response_check = response_check

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        logging.info("Calling HTTP method")
        response = http.run(self.endpoint, self.data, self.headers)
        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
