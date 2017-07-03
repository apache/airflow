import logging
import requests

from airflow import configuration
from airflow.notifiers import AirflowNotifier
from airflow.exceptions import AirflowException


class HipChatAPI(object):
    
    @staticmethod
    def send_message(url, token, body):
        response = requests.request('POST',
                                    url,
                                    headers={
                                        'Content-Type': 'application/json',
                                        'Authorization': 'Bearer %s' % token},
                                    data=body)
        
        if response.status_code >= 400:
            logging.error('HipChat API call failed: %s %s',
                          response.status_code, response.reason)
            raise AirflowException('HipChat API call failed: %s %s' %
                                   (response.status_code, response.reason))


class HipChatNotifier(AirflowNotifier):
    
    def __init__(self, room_id, notify_on = []):
        self.room_id = room_id
        self.notify_on = notify_on
    
    def send_notification(self, task_instance, state, message, **kwargs):
        if state in self.notify_on:
            text_tpl = self.notify_on[state]
            text = self.render_template(text_tpl, task_instance, message=message, **kwargs)
            
            
            if configuration.has_option('hipchat', 'base_url'):
                base_url = configuration.get('hipchat', 'base_url')
            else:
                base_url = 'https://api.hipchat.com/v2'
            
            url = '%s/room/%s/notification' % (base_url, self.room_id)
            token = configuration.get('hipchat', 'token')
            body = {'message': text,
                    'message_format': 'text',
                    'frm': 'Airflow'}
            
            HipChatAPI.send_message(url, token, body)
