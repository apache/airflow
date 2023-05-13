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

import unittest
from unittest.mock import MagicMock, patch
from airflow.providers.discord.notifications.discord_notifier import DiscordNotifier
from airflow.models import Connection
from airflow.utils import db

class TestDiscordNotifier(unittest.TestCase):

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='my_discord_conn_id',
                conn_type='HTTP',
                host='https://discordapp.com/api/',
                extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}'
            )
        )

    @patch("airflow.providers.discord.notifications.discord_notifier.DiscordWebhookHook.execute")
    def test_notify(self, mock_execute):
        notifier = DiscordNotifier(
            discord_conn_id="my_discord_conn_id",
            text="This is a test message",
            username="test_user",
            avatar_url="https://example.com/avatar.png",
            tts=False
        )
        context = MagicMock()

        notifier.notify(context)

        mock_execute.assert_called_once()
        self.assertEqual(notifier.hook.username, "test_user")
        self.assertEqual(notifier.hook.message, "This is a test message")
        self.assertEqual(notifier.hook.avatar_url, "https://example.com/avatar.png")
        self.assertEqual(notifier.hook.tts, False)
