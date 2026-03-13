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

import json
from contextlib import nullcontext
from unittest import mock

import pytest
from aioresponses import aioresponses

from airflow.models import Connection
from airflow.providers.discord.hooks.discord_webhook import (
    DiscordCommonHandler,
    DiscordWebhookAsyncHook,
    DiscordWebhookHook,
)


@pytest.fixture
def aioresponse():
    """
    Creates mock async API response.
    """
    with aioresponses() as async_response:
        yield async_response


class TestDiscordCommonHandler:
    _config = {
        "message": "your message here",
        "username": "Airflow Webhook",
        "avatar_url": "https://static-cdn.avatars.com/my-avatar-path",
        "tts": False,
    }

    expected_payload_dict = {
        "content": _config["message"],
        "tts": _config["tts"],
        "username": _config["username"],
        "avatar_url": _config["avatar_url"],
    }

    expected_payload = json.dumps(expected_payload_dict)

    def test_get_webhook_endpoint_manual_token(self):
        provided_endpoint = "webhooks/11111/some-discord-token_111"
        handler = DiscordCommonHandler()
        webhook_endpoint = handler.get_webhook_endpoint(None, provided_endpoint)
        assert webhook_endpoint == provided_endpoint

    def test_get_webhook_endpoint_invalid_url(self):
        provided_endpoint = "https://discordapp.com/some-invalid-webhook-url"
        handler = DiscordCommonHandler()
        expected_message = "Expected Discord webhook endpoint in the form of"
        with pytest.raises(ValueError, match=expected_message):
            handler.get_webhook_endpoint(None, provided_endpoint)

    def test_get_webhook_endpoint_conn_id(self):
        conn = Connection(
            conn_id="default-discord-webhook",
            conn_type="discord",
            host="https://discordapp.com/api/",
            extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}',
        )
        expected_webhook_endpoint = "webhooks/00000/some-discord-token_000"
        handler = DiscordCommonHandler()
        webhook_endpoint = handler.get_webhook_endpoint(conn, None)
        assert webhook_endpoint == expected_webhook_endpoint

    def test_build_discord_payload(self):
        handler = DiscordCommonHandler()
        payload = handler.build_discord_payload(**self._config)
        assert self.expected_payload == payload

    def test_build_discord_payload_with_embed(self):
        config = self._config.copy()
        embed = {
            "title": "This is a title",
            "type": "rich",
            "description": "A test description",
            "url": "https://example.com",
        }
        config["embed"] = embed
        expected_payload = self.expected_payload_dict.copy()
        expected_payload["embeds"] = [embed]
        handler = DiscordCommonHandler()
        payload = handler.build_discord_payload(**config)
        assert json.dumps(expected_payload) == payload

    @pytest.mark.parametrize(
        ("embed", "expectation"),
        [
            pytest.param(
                {
                    "title": "This is a title",
                    "type": "rich",
                    "description": "A test description",
                    "url": "https://example.com",
                },
                nullcontext(),
                id="valid-embed",
            ),
            pytest.param(
                {"title": "t" * 257},
                pytest.raises(ValueError, match="Discord embed title must be 256 or fewer characters"),
                id="fail-on-title",
            ),
            pytest.param(
                {"description": "t" * 4097},
                pytest.raises(ValueError, match="Discord embed description must be 4096 or fewer characters"),
                id="fail-on-description",
            ),
            pytest.param(
                {"fields": [[] for _ in range(26)]},
                pytest.raises(ValueError, match="Discord embed fields must be 25 or fewer items"),
                id="fail-on-fields",
            ),
            pytest.param(
                {"title": "This is a title", "author": {"name": "t" * 2049}},
                pytest.raises(ValueError, match="Discord embed author name must be 256 or fewer characters"),
                id="fail-on-author-name",
            ),
            pytest.param(
                {"title": "This is a title", "footer": {"text": "t" * 2049}},
                pytest.raises(ValueError, match="Discord embed footer text must be 2048 or fewer characters"),
                id="fail-on-footer-text",
            ),
            pytest.param(
                {"title": "This is a title", "fields": [{"name": "t" * 257, "value": "test"}]},
                pytest.raises(ValueError, match="Discord embed field name must be 256 or fewer characters"),
                id="fail-on-field-name",
            ),
            pytest.param(
                {"title": "This is a title", "fields": [{"name": "test", "value": "t" * 1025}]},
                pytest.raises(ValueError, match="Discord embed field value must be 1024 or fewer characters"),
                id="fail-on-field-value",
            ),
        ],
    )
    def test_build_embed(self, embed, expectation):
        handler = DiscordCommonHandler()
        with expectation:
            handler.validate_embed(embed=embed)

    def test_build_discord_payload_message_length(self):
        # Given
        config = self._config.copy()
        # create message over the character limit
        config["message"] = "c" * 2001
        handler = DiscordCommonHandler()
        expected_message = "Discord message length must be 2000 or fewer characters"
        with pytest.raises(ValueError, match=expected_message):
            handler.build_discord_payload(**config)


class TestDiscordWebhookHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="default-discord-webhook",
                conn_type="discord",
                host="https://discordapp.com/api/",
                extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}',
            )
        )

    def test_get_webhook_endpoint_manual_token(self):
        # Given
        provided_endpoint = "webhooks/11111/some-discord-token_111"
        hook = DiscordWebhookHook(webhook_endpoint=provided_endpoint)

        # When
        webhook_endpoint = hook._get_webhook_endpoint(None, provided_endpoint)

        # Then
        assert webhook_endpoint == provided_endpoint

    def test_get_webhook_endpoint_conn_id(self):
        # Given
        conn_id = "default-discord-webhook"
        hook = DiscordWebhookHook(http_conn_id=conn_id)
        expected_webhook_endpoint = "webhooks/00000/some-discord-token_000"

        # When
        webhook_endpoint = hook._get_webhook_endpoint(conn_id, None)

        # Then
        assert webhook_endpoint == expected_webhook_endpoint


class TestDiscordWebhookAsyncHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="default-discord-webhook",
                conn_type="discord",
                host="https://discordapp.com/api/",
                extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}',
            )
        )

    @pytest.fixture(autouse=True)
    def mock_get_connection(self):
        """Mock the async connection retrieval."""
        with mock.patch(
            "airflow.providers.discord.hooks.discord_webhook.get_async_connection",
            new_callable=mock.AsyncMock,
        ) as mock_conn:
            mock_conn.return_value = Connection(
                conn_id="default-discord-webhook",
                conn_type="discord",
                host="https://discordapp.com/api/",
                extra='{"webhook_endpoint": "webhooks/00000/some-discord-token_000"}',
            )
            yield mock_conn

    @pytest.mark.asyncio
    async def test_manual_token_overrides_conn(self):
        provided_endpoint = "webhooks/11111/some-discord-token_111"
        hook = DiscordWebhookAsyncHook(webhook_endpoint=provided_endpoint)
        webhook_endpoint = await hook._get_webhook_endpoint()
        assert webhook_endpoint == provided_endpoint

    @pytest.mark.asyncio
    async def test_get_webhook_endpoint_conn_id(self):
        conn_id = "default-discord-webhook"
        hook = DiscordWebhookAsyncHook(http_conn_id=conn_id)
        expected_webhook_endpoint = "webhooks/00000/some-discord-token_000"
        webhook_endpoint = await hook._get_webhook_endpoint()
        assert webhook_endpoint == expected_webhook_endpoint

    @pytest.mark.asyncio
    async def test_execute_with_payload(self):
        conn_id = "default-discord-webhook"
        hook = DiscordWebhookAsyncHook(
            http_conn_id=conn_id,
            message="your message here",
            username="Airflow Webhook",
            avatar_url="https://static-cdn.avatars.com/my-avatar-path",
            tts=False,
        )
        expected_payload_dict = {
            "content": "your message here",
            "tts": False,
            "username": "Airflow Webhook",
            "avatar_url": "https://static-cdn.avatars.com/my-avatar-path",
        }

        with mock.patch("aiohttp.ClientSession.post", new_callable=mock.AsyncMock) as mocked_function:
            await hook.execute()
            assert mocked_function.call_args.kwargs.get("data") == json.dumps(expected_payload_dict)

    @pytest.mark.asyncio
    async def test_execute_with_success(self, aioresponse):
        conn_id = "default-discord-webhook"
        hook = DiscordWebhookAsyncHook(
            http_conn_id=conn_id,
            message="your message here",
            username="Airflow Webhook",
            avatar_url="https://static-cdn.avatars.com/my-avatar-path",
            tts=False,
        )
        aioresponse.post("https://discordapp.com/api/webhooks/00000/some-discord-token_000", status=200)
        await hook.execute()
