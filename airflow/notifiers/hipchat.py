# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import requests

from airflow import configuration
from airflow.notifiers import AirflowNotifier
from airflow.exceptions import AirflowException
from airflow.utils.state import State


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

    def __init__(self, room_id, notify_on=None):
        self.room_id = room_id

        if notify_on is None:
            self.notify_on = {State.UP_FOR_RETRY: 'html_body.tpl',
                              State.FAILED: 'html_body.tpl'}
        else:
            self.notify_on = notify_on

    def send_notification(self, task_instance, state, message, **kwargs):
        if state in self.notify_on:
            text_tpl = self.notify_on[state]
            text = self.render_template(text_tpl,
                                        task_instance,
                                        message=message,
                                        **kwargs)

            if configuration.has_option('hipchat', 'base_url'):
                base_url = configuration.get('hipchat', 'base_url')
            else:
                base_url = 'https://api.hipchat.com/v2'

            if configuration.has_option('hipchat', 'from'):
                frm = configuration.get('hipchat', 'from')
            else:
                frm = 'Airflow'

            if configuration.has_option('hipchat', 'color'):
                color = configuration.get('hipchat', 'color')
            else:
                color = 'yellow'

            url = '%s/room/%s/notification' % (base_url, self.room_id)
            token = configuration.get('hipchat', 'token')
            body = {'message': text,
                    'message_format': 'html',
                    'from': frm,
                    'color': color}

            HipChatAPI.send_message(url, token, body)
