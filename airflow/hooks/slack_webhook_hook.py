import logging
import requests
from airflow.hooks.base_hook import BaseHook

class SlackWebHookHook(BaseHook):
    """Slack WebHook hook. Connects to a slack webhook URL using a Connection.

    The Slack WebHook URL should be defined in the 'extra' field of the connection as defined
    in the Airflow admin interface.
    """
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def _get_webhook_url(self):
        webhook_url = self.get_connection(self.conn_id).extra
        if not webhook_url:
            logging.error("'extra' not defined on connection '%s'. SlackWebHookHook will fail" % self.conn_id)
        return webhook_url

    def post(self, json):
        response = requests.post(self._get_webhook_url(), json=json,
                                 headers={'Content-Type': 'application/json'})
        response.raise_for_status()

