import logging

from airflow.hooks import HttpHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.utils import AirflowException

class SimpleHttpOperator(BaseOperator):
    """
    Calls a url 
    """

    template_fields = ('url',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self, url, method='POST', data=None, headers={}, http_conn_id='http_default', *args, **kwargs):
        super(SimpleHttpOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.url = url
        self.data = data
        self.headers = headers

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        logging.info("Calling HTTP method")
        result, content = http.run( self.url, self.data, self.headers )
        if not result:
            raise AirflowException( "HTTP call failed" )


