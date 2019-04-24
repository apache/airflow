# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import str

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
import requests
import json


class HipChatAPIOperator(BaseOperator):
    """
    Base HipChat Operator.
    All derived HipChat operators reference from HipChat's official REST API documentation
    at https://www.hipchat.com/docs/apiv2. Before using any HipChat API operators you need
    to get an authentication token at https://www.hipchat.com/docs/apiv2/auth.
    In the future additional HipChat operators will be derived from this class as well.

    :param token: HipChat REST API authentication token
    :type token: str
    :param base_url: HipChat REST API base url.
    :type base_url: str
    """
    @apply_defaults
    def __init__(self,
                 token,
                 base_url='https://api.hipchat.com/v2',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.base_url = base_url
        self.method = None
        self.url = None
        self.body = None

    def prepare_request(self):
        """
        Used by the execute function. Set the request method, url, and body of HipChat's
        REST API call.
        Override in child class. Each HipChatAPI child operator is responsible for having
        a prepare_request method call which sets self.method, self.url, and self.body.
        """
        pass

    def execute(self, context):
        self.prepare_request()

        response = requests.request(self.method,
                                    self.url,
                                    headers={
                                        'Content-Type': 'application/json',
                                        'Authorization': 'Bearer %s' % self.token},
                                    data=self.body)
        if response.status_code >= 400:
            self.log.error('HipChat API call failed: %s %s',
                           response.status_code, response.reason)
            raise AirflowException('HipChat API call failed: %s %s' %
                                   (response.status_code, response.reason))


class HipChatAPISendRoomNotificationOperator(HipChatAPIOperator):
    """
    Send notification to a specific HipChat room.
    More info: https://www.hipchat.com/docs/apiv2/method/send_room_notification

    :param room_id: Room in which to send notification on HipChat. (templated)
    :type room_id: str
    :param message: The message body. (templated)
    :type message: str
    :param frm: Label to be shown in addition to sender's name
    :type frm: str
    :param message_format: How the notification is rendered: html or text
    :type message_format: str
    :param color: Background color of the msg: yellow, green, red, purple, gray, or random
    :type color: str
    :param attach_to: The message id to attach this notification to
    :type attach_to: str
    :param notify: Whether this message should trigger a user notification
    :type notify: bool
    :param card: HipChat-defined card object
    :type card: dict
    """
    template_fields = ('token', 'room_id', 'message', 'message_format',
                       'color', 'frm', 'attach_to', 'notify', 'card')
    ui_color = '#2980b9'

    @apply_defaults
    def __init__(self, room_id, message, message_format='html',
                 color='yellow', frm='airflow', attach_to=None,
                 notify=False, card=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.room_id = room_id
        self.message = message
        self.message_format = message_format
        self.color = color
        self.frm = frm
        self.attach_to = attach_to
        self.notify = notify
        self.card = card

    def prepare_request(self):
        params = {
            'message': self.message,
            'message_format': self.message_format,
            'color': self.color,
            'from': self.frm,
            'attach_to': self.attach_to,
            'notify': self.notify,
            'card': self.card
        }

        self.method = 'POST'
        self.url = '%s/room/%s/notification' % (self.base_url, self.room_id)
        self.body = json.dumps(dict(
            (str(k), str(v)) for k, v in params.items() if v))
