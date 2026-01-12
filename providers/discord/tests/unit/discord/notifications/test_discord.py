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
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from airflow.models import Connection
from airflow.providers.discord.notifications.discord import DiscordNotifier

long_embed_fixture = {
    "title": "Dag {{ dag.dag_id }}",
    "type": "rich",
    "description": "🚨 Dag description for Dag: {{ dag.dag_id }}",
    "url": "https://example.com",
    "timestamp": "2024-01-15T10:30:00Z",
    "color": 0x3498DB,
    "footer": {"text": "Dag {{ dag.dag_id }}", "icon_url": "https://example.com/footer-icon.png"},
    "provider": {"name": "Dag {{ dag.dag_id }}", "url": "https://example.com/blog"},
    "author": {
        "name": "Foo",
        "url": "https://example.com/author/foo",
        "icon_url": "https://example.com/foo-avatar.png",
    },
    "fields": [
        {"name": "Field 1", "value": "Value for {{ dag.dag_id }}", "inline": True},
        {"name": "Field 2", "value": "Value for {{ dag.dag_id }}", "inline": True},
    ],
}


exp_long_embed_fixture = {
    "title": "Dag test_discord_webhook_notification_templated",
    "type": "rich",
    "description": "🚨 Dag description for Dag: test_discord_webhook_notification_templated",
    "url": "https://example.com",
    "timestamp": "2024-01-15T10:30:00Z",
    "color": 0x3498DB,
    "footer": {
        "text": "Dag test_discord_webhook_notification_templated",
        "icon_url": "https://example.com/footer-icon.png",
    },
    "provider": {
        "name": "Dag test_discord_webhook_notification_templated",
        "url": "https://example.com/blog",
    },
    "author": {
        "name": "Foo",
        "url": "https://example.com/author/foo",
        "icon_url": "https://example.com/foo-avatar.png",
    },
    "fields": [
        {"name": "Field 1", "value": "Value for test_discord_webhook_notification_templated", "inline": True},
        {"name": "Field 2", "value": "Value for test_discord_webhook_notification_templated", "inline": True},
    ],
}


@pytest.fixture(autouse=True)
def setup_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id="my_discord_conn_id",
            conn_type="discord",
            host="https://discordapp.com/api/",
            extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}',
        )
    )


@patch("airflow.providers.discord.notifications.discord.DiscordWebhookHook.execute")
def test_discord_notifier_notify(mock_execute):
    notifier = DiscordNotifier(
        discord_conn_id="my_discord_conn_id",
        text="This is a test message",
        username="test_user",
        avatar_url="https://example.com/avatar.png",
        tts=False,
    )
    context = MagicMock()

    notifier.notify(context)

    mock_execute.assert_called_once()
    assert notifier.hook.username == "test_user"
    assert notifier.hook.message == "This is a test message"
    assert notifier.hook.avatar_url == "https://example.com/avatar.png"
    assert notifier.hook.tts is False


@pytest.mark.asyncio
@patch(
    "airflow.providers.discord.notifications.discord.DiscordWebhookAsyncHook.execute",
    new_callable=AsyncMock,
)
async def test_async_notifier(mock_async_hook):
    notifier = DiscordNotifier(
        discord_conn_id="my_discord_conn_id",
        text="This is a test message",
        username="test_user",
        avatar_url="https://example.com/avatar.png",
        tts=False,
    )
    await notifier.async_notify({})
    assert mock_async_hook.mock_calls == [call()]


@patch("airflow.providers.discord.notifications.discord.DiscordWebhookHook")
def test_discord_hook_templated(mock_hook, create_dag_without_db):
    notifier = DiscordNotifier(embed=long_embed_fixture)
    notifier(
        {
            "dag": create_dag_without_db("test_discord_webhook_notification_templated"),
        }
    )
    mock_hook.return_value.execute.assert_called_once()
    assert notifier.embed == exp_long_embed_fixture
