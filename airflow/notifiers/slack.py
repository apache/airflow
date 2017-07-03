import logging
from airflow.notifiers import AirflowNotifier

from slackclient import SlackClient
from airflow.exceptions import AirflowException
from airflow import configuration


class SlackAPI(object):
    
    @staticmethod
    def send_message(method, token, body):
        sc = SlackClient(token)
        rc = sc.api_call(method, **body)
        if not rc['ok']:
            logging.error("Slack API call failed ({})".format(rc['error']))
            raise AirflowException("Slack API call failed: ({})".format(rc['error']))
        

class SlackNotifier(AirflowNotifier):
    
    def __init__(self, channel, notify_on = {}):
        self.channel = channel
        self.notify_on = notify_on
    
    def send_notification(self, task_instance, state, message, **kwargs):
        if state in self.notify_on:
            text_tpl = self.notify_on[state]
            text = self.render_template(text_tpl, task_instance, message=message, **kwargs)
            
            token = configuration.get('slack', 'token')
            body = {'channel': self.channel,
                    'username': 'Airflow',
                    'text': text,
                    'icon_url': 'https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png'}
            
            SlackAPI.send_message('chat.postMessage', token, body)
