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

import json

import requests

from airflow import AirflowException
from airflow.hooks.http_hook import HttpHook


class DingdingHook(HttpHook):
    """
    This hook allows you send Dingding message using Dingding custom bot.
    Get Dingding token from conn_id.password. And prefer set domain to
    conn_id.host, if not will use default ``https://oapi.dingtalk.com``.

    For more detail message in
    `Dingding custom bot <https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq>`_

    :param dingding_conn_id: The name of the Dingding connection to use
    :type dingding_conn_id: str
    :param message_type: Message type you want to send to Dingding, support five type so far
        including text, link, markdown, actionCard, feedCard
    :type message_type: str
    :param message: The message send to Dingding chat group
    :type message: str or dict
    :param at_mobiles: Remind specific users with this message
    :type at_mobiles: list[str]
    :param at_all: Remind all people in group or not. If True, will overwrite ``at_mobiles``
    :type at_all: bool
    """

    def __init__(self,
                 dingding_conn_id='dingding_default',
                 message_type='text',
                 message=None,
                 at_mobiles=None,
                 at_all=False,
                 *args,
                 **kwargs
                 ):
        super(DingdingHook, self).__init__(http_conn_id=dingding_conn_id, *args, **kwargs)
        self.message_type = message_type
        self.message = message
        self.at_mobiles = at_mobiles
        self.at_all = at_all

    def _get_endpoint(self):
        """
        Get Dingding endpoint for sending message.
        """
        conn = self.get_connection(self.http_conn_id)
        token = conn.password
        if not token:
            raise AirflowException('Dingding token is requests but get nothing, '
                                   'check you conn_id configuration.')
        return 'robot/send?access_token={}'.format(token)

    def _build_message(self):
        """
        Build different type of Dingding message
        As most commonly used type, text message just need post message content
        rather than a dict like ``{'content': 'message'}``
        """
        if self.message_type in ['text', 'markdown']:
            data = {
                'msgtype': self.message_type,
                self.message_type: {
                    'content': self.message
                } if self.message_type == 'text' else self.message,
                'at': {
                    'atMobiles': self.at_mobiles,
                    'isAtAll': self.at_all
                }
            }
        else:
            data = {
                'msgtype': self.message_type,
                self.message_type: self.message
            }
        return json.dumps(data)

    def get_conn(self, headers=None):
        """
        Overwrite HttpHook get_conn because just need base_url and headers and
        not don't need generic params

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        conn = self.get_connection(self.http_conn_id)
        self.base_url = conn.host if conn.host else 'https://oapi.dingtalk.com'
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session

    def send(self):
        """
        Send Dingding message
        """
        support_type = ['text', 'link', 'markdown', 'actionCard', 'feedCard']
        if self.message_type not in support_type:
            raise ValueError('DingdingWebhookHook only support {} '
                             'so far, but receive {}'.format(support_type, self.message_type))

        data = self._build_message()
        self.log.info('Sending Dingding type %s message %s', self.message_type, data)
        resp = self.run(endpoint=self._get_endpoint(),
                        data=data,
                        headers={'Content-Type': 'application/json'})

        # Dingding success send message will with errcode equal to 0
        if int(resp.json().get('errcode')) != 0:
            raise AirflowException('Send Dingding message failed, receive error '
                                   'message %s', resp.text)
        self.log.info('Success Send Dingding message')
