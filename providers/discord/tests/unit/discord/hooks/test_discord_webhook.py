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

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.utils import db

pytestmark = pytest.mark.db_test


class TestDiscordWebhookHook:
    _config = {
        "http_conn_id": "default-discord-webhook",
        "webhook_endpoint": "webhooks/11111/some-discord-token_111",
        "message": "your message here",
        "username": "Airflow Webhook",
        "avatar_url": "https://static-cdn.avatars.com/my-avatar-path",
        "tts": False,
        "proxy": "https://proxy.proxy.com:8888",
    }

    expected_payload_dict = {
        "username": _config["username"],
        "avatar_url": _config["avatar_url"],
        "tts": _config["tts"],
        "content": _config["message"],
        "mention_everyone": False,
        "application_id": "airflow",
    }

    expected_payload = json.dumps(expected_payload_dict)

    def setup_method(self):
        db.merge_conn(
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

    def test_get_webhook_endpoint_invalid_url(self):
        # Given
        provided_endpoint = "https://discordapp.com/some-invalid-webhook-url"

        # When/Then
        expected_message = "Expected Discord webhook endpoint in the form of"
        with pytest.raises(AirflowException, match=expected_message):
            DiscordWebhookHook(webhook_endpoint=provided_endpoint)

    def test_get_webhook_endpoint_conn_id(self):
        # Given
        conn_id = "default-discord-webhook"
        hook = DiscordWebhookHook(http_conn_id=conn_id)
        expected_webhook_endpoint = "webhooks/00000/some-discord-token_000"

        # When
        webhook_endpoint = hook._get_webhook_endpoint(conn_id, None)

        # Then
        assert webhook_endpoint == expected_webhook_endpoint

    def test_build_discord_payload(self):
        # Given
        hook = DiscordWebhookHook(**self._config)

        # When
        payload = hook._build_discord_payload()

        # Then
        assert self.expected_payload == payload

    def test_build_discord_payload_message_length(self):
        # Given
        config = self._config.copy()
        # create message over the character limit
        config["message"] = "c" * 2001
        hook = DiscordWebhookHook(**config)

        # When/Then
        expected_message = "Discord message length must be 2000 or fewer characters"
        with pytest.raises(AirflowException, match=expected_message):
            hook._build_discord_payload()

    def test_validate_embeds_valid_embed(self):
        # Given
        hook = DiscordWebhookHook(**self._config)
        valid_embeds = [
            {
                "title": "Test Embed",
                "description": "This is a test embed",
                "color": 16711680,  # Red color as integer
                "url": "https://example.com",
                "author": {"name": "Test Author"},
            }
        ]

        # When/Then - should not raise any exception
        hook._validate_embeds(valid_embeds)

    def test_validate_embeds_invalid_non_dict(self):
        # Given
        hook = DiscordWebhookHook(**self._config)
        invalid_embeds = [
            "not a dictionary"  # Invalid: not a dict
        ]

        # When/Then
        expected_message = "Each embed must be a dictionary"
        with pytest.raises(AirflowException, match=expected_message):
            hook._validate_embeds(invalid_embeds)

    def test_validate_embeds_invalid_color_type_string(self):
        # Given
        hook = DiscordWebhookHook(**self._config)
        invalid_embeds = [
            {
                "title": "Test Embed",
                "color": "red",  # Invalid: color must be integer, not string
            }
        ]

        # When/Then
        expected_message = "Embed color must be an integer"
        with pytest.raises(AirflowException, match=expected_message):
            hook._validate_embeds(invalid_embeds)

    def test_validate_embeds_invalid_url_not_string(self):
        # Given
        hook = DiscordWebhookHook(**self._config)
        invalid_embeds = [
            {
                "title": "Test Embed",
                "url": 123,  # Invalid: URL must be string, not integer
            }
        ]

        # When/Then
        expected_message = "Embed URL must be a string starting with 'https://'"
        with pytest.raises(AirflowException, match=expected_message):
            hook._validate_embeds(invalid_embeds)

    def test_validate_embeds_empty_list(self):
        # Given
        hook = DiscordWebhookHook(**self._config)
        empty_embeds = []

        # When/Then - should not raise any exception for empty list
        hook._validate_embeds(empty_embeds)
