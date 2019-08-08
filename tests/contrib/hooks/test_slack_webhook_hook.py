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
from requests.exceptions import MissingSchema
import unittest
from unittest import mock

from airflow.models import Connection
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
        'icon_url': 'https://airflow.apache.org/_images/pin_large.png',
        'link_names': True,
        'proxy': 'https://my-horrible-proxy.proxyist.com:8080'
    }
    expected_message_dict = {
        'channel': _config['channel'],
        'username': _config['username'],
        'icon_emoji': _config['icon_emoji'],
        'icon_url': _config['icon_url'],
        'link_names': 1,
        'attachments': _config['attachments'],
        'text': _config['message']
    }
    expected_message = json.dumps(expected_message_dict)
    expected_url = 'https://hooks.slack.com/services/T000/B000/XXX'
    expected_method = 'POST'

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='slack-webhook-default',
                extra='{"webhook_token": "your_token_here"}')
        )
        db.merge_conn(
            Connection(
                conn_id='slack-webhook-url',
                host='https://hooks.slack.com/services/T000/B000/XXX')
        )
        db.merge_conn(
            Connection(
                conn_id='slack-webhook-host',
                host='https://hooks.slack.com/services/T000/')
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

    @mock.patch('requests.Session')
    @mock.patch('requests.Request')
    def test_url_generated_by_http_conn_id(self, mock_request, mock_session):
        hook = SlackWebhookHook(http_conn_id='slack-webhook-url')
        try:
            hook.execute()
        except MissingSchema:
            pass
        mock_request.assert_called_once_with(
            self.expected_method,
            self.expected_url,
            headers=mock.ANY,
            data=mock.ANY
        )
        mock_request.reset_mock()

    @mock.patch('requests.Session')
    @mock.patch('requests.Request')
    def test_url_generated_by_endpoint(self, mock_request, mock_session):
        hook = SlackWebhookHook(webhook_token=self.expected_url)
        try:
            hook.execute()
        except MissingSchema:
            pass
        mock_request.assert_called_once_with(
            self.expected_method,
            self.expected_url,
            headers=mock.ANY,
            data=mock.ANY
        )
        mock_request.reset_mock()

    @mock.patch('requests.Session')
    @mock.patch('requests.Request')
    def test_url_generated_by_http_conn_id_and_endpoint(self, mock_request, mock_session):
        hook = SlackWebhookHook(http_conn_id='slack-webhook-host',
                                webhook_token='B000/XXX')
        try:
            hook.execute()
        except MissingSchema:
            pass
        mock_request.assert_called_once_with(
            self.expected_method,
            self.expected_url,
            headers=mock.ANY,
            data=mock.ANY
        )
        mock_request.reset_mock()


if __name__ == '__main__':
    unittest.main()
