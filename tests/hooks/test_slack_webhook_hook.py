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

import mock
import unittest
import requests
from airflow.hooks.slack_webhook_hook import SlackWebHookHook

class SlackWebhookHookTest(unittest.TestCase):
    @mock.patch('requests.post')
    def test_post(self, requests_post):
        webhook_url = object()

        hook = SlackWebHookHook(None)
        hook._get_webhook_url = mock.Mock()
        hook._get_webhook_url.return_value = webhook_url

        data = {'attr':'value'}
        hook.post(data)

        requests_post.assert_called_once_with(webhook_url, json=data,
                                              headers={'Content-Type': 'application/json'})

    @mock.patch('logging.error')
    def test_get_webhook_url(self, error):
        conn_id = object()
        webhook_url = mock.Mock()
        connection = mock.Mock()
        connection.extra = webhook_url

        hook = SlackWebHookHook(conn_id)

        hook.get_connection = mock.Mock()
        hook.get_connection.return_value = connection

        self.assertEqual(hook._get_webhook_url(), webhook_url)
        hook.get_connection.assert_called_once_with(conn_id)

        error.assert_not_called()

    @mock.patch('logging.error')
    def test_get_webhook_url_no_extra(self, error):
        conn_id = 'connection name'
        webhook_url = None
        connection = mock.Mock()
        connection.extra = None

        hook = SlackWebHookHook(conn_id)

        hook.get_connection = mock.Mock()
        hook.get_connection.return_value = connection

        self.assertEqual(hook._get_webhook_url(), None)
        hook.get_connection.assert_called_once_with(conn_id)

        error.assert_called_with("'extra' not defined on connection 'connection name'. SlackWebHookHook will fail")













