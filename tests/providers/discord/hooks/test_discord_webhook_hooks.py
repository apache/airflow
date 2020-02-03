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

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.utils import db


class TestDiscordWebhookHook(unittest.TestCase):

    _config = {
        'http_conn_id': 'default-discord-webhook',
        'webhook_endpoint': 'webhooks/11111/some-discord-token_111',
        'message': 'your message here',
        'username': 'Airflow Webhook',
        'avatar_url': 'https://static-cdn.avatars.com/my-avatar-path',
        'tts': False,
        'proxy': 'https://proxy.proxy.com:8888'
    }

    expected_payload_dict = {
        'username': _config['username'],
        'avatar_url': _config['avatar_url'],
        'tts': _config['tts'],
        'content': _config['message']
    }

    expected_payload = json.dumps(expected_payload_dict)

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='default-discord-webhook',
                host='https://discordapp.com/api/',
                extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}')
        )

    def test_get_webhook_endpoint_manual_token(self):
        # Given
        provided_endpoint = 'webhooks/11111/some-discord-token_111'
        hook = DiscordWebhookHook(webhook_endpoint=provided_endpoint)

        # When
        webhook_endpoint = hook._get_webhook_endpoint(None, provided_endpoint)

        # Then
        self.assertEqual(webhook_endpoint, provided_endpoint)

    def test_get_webhook_endpoint_invalid_url(self):
        # Given
        provided_endpoint = 'https://discordapp.com/some-invalid-webhook-url'

        # When/Then
        expected_message = 'Expected Discord webhook endpoint in the form of'
        with self.assertRaisesRegex(AirflowException, expected_message):
            DiscordWebhookHook(webhook_endpoint=provided_endpoint)

    def test_get_webhook_endpoint_conn_id(self):
        # Given
        conn_id = 'default-discord-webhook'
        hook = DiscordWebhookHook(http_conn_id=conn_id)
        expected_webhook_endpoint = 'webhooks/00000/some-discord-token_000'

        # When
        webhook_endpoint = hook._get_webhook_endpoint(conn_id, None)

        # Then
        self.assertEqual(webhook_endpoint, expected_webhook_endpoint)

    def test_build_discord_payload(self):
        # Given
        hook = DiscordWebhookHook(**self._config)

        # When
        payload = hook._build_discord_payload()

        # Then
        self.assertEqual(self.expected_payload, payload)

    def test_build_discord_payload_message_length(self):
        # Given
        config = self._config.copy()
        # create message over the character limit
        config["message"] = 'c' * 2001
        hook = DiscordWebhookHook(**config)

        # When/Then
        expected_message = 'Discord message length must be 2000 or fewer characters'
        with self.assertRaisesRegex(AirflowException, expected_message):
            hook._build_discord_payload()


if __name__ == '__main__':
    unittest.main()
