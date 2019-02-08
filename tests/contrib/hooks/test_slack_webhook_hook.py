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
#
import json
import unittest

from airflow import configuration
from airflow.models.connection import Connection
from airflow.utils import db

from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook


class TestSlackWebhookHook(unittest.TestCase):

    _config = {
        'http_conn_id': 'slack-webhook-default',
        'webhook_token': 'manual_token',
        'message': 'Awesome message to put on Slack',
        'attachments': [{'fallback': 'Required plain-text summary'}],
        'channel': '#general',
        'username': 'SlackMcSlackFace',
        'icon_emoji': ':hankey:',
        'link_names': True,
        'proxy': 'https://my-horrible-proxy.proxyist.com:8080'
    }
    expected_message_dict = {
        'channel': _config['channel'],
        'username': _config['username'],
        'icon_emoji': _config['icon_emoji'],
        'link_names': 1,
        'attachments': _config['attachments'],
        'text': _config['message']
    }
    expected_message = json.dumps(expected_message_dict)

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            Connection(
                conn_id='slack-webhook-default',
                extra='{"webhook_token": "your_token_here"}')
        )

    def test_get_token_manual_token(self):
        # Given
        manual_token = 'manual_token_here'
        hook = SlackWebhookHook(webhook_token=manual_token)

        # When
        webhook_token = hook._get_token(manual_token, None)

        # Then
        self.assertEqual(webhook_token, manual_token)

    def test_get_token_conn_id(self):
        # Given
        conn_id = 'slack-webhook-default'
        hook = SlackWebhookHook(http_conn_id=conn_id)
        expected_webhook_token = 'your_token_here'

        # When
        webhook_token = hook._get_token(None, conn_id)

        # Then
        self.assertEqual(webhook_token, expected_webhook_token)

    def test_build_slack_message(self):
        # Given
        hook = SlackWebhookHook(**self._config)

        # When
        message = hook._build_slack_message()

        # Then
        self.assertEqual(self.expected_message, message)


if __name__ == '__main__':
    unittest.main()
