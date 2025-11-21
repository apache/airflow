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

from __future__ import annotations

import json
import os
from typing import Any
from unittest import mock
from unittest.mock import patch

import pytest
from slack_sdk.http_retry.builtin_handlers import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler
from slack_sdk.webhook.async_client import AsyncWebhookClient
from slack_sdk.webhook.webhook_response import WebhookResponse

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.slack.hooks.slack_webhook import (
    SlackWebhookHook,
    async_check_webhook_response,
    check_webhook_response,
)

TEST_TOKEN = "T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
TEST_WEBHOOK_URL = f"https://hooks.slack.com/services/{TEST_TOKEN}"
TEST_CUSTOM_SCHEMA = "http"
TEST_CUSTOM_ENDPOINT = "example.org/slack/webhooks"
TEST_CUSTOM_WEBHOOK_URL = f"{TEST_CUSTOM_SCHEMA}://{TEST_CUSTOM_ENDPOINT}/{TEST_TOKEN}"
TEST_CONN_ID = SlackWebhookHook.default_conn_name
CONN_TYPE = "slackwebhook"
TEST_CONN_ERROR_RETRY_HANDLER = ConnectionErrorRetryHandler(max_retry_count=42)
TEST_RATE_LIMIT_RETRY_HANDLER = RateLimitErrorRetryHandler()
MOCK_WEBHOOK_RESPONSE = WebhookResponse(url="foo://bar", status_code=200, body="ok", headers={})


@pytest.fixture(scope="module", autouse=True)
def slack_webhook_connections():
    """Create tests connections."""
    connections = [
        Connection(
            conn_id=SlackWebhookHook.default_conn_name,
            conn_type=CONN_TYPE,
            password=TEST_TOKEN,
        ),
        Connection(
            conn_id="conn_full_url_connection",
            conn_type=CONN_TYPE,
            password=TEST_WEBHOOK_URL,
        ),
        Connection(
            conn_id="conn_full_url_connection_with_host",
            conn_type=CONN_TYPE,
            host="http://example.org/hooks/",
            password=TEST_WEBHOOK_URL,
        ),
        Connection(
            conn_id="conn_host_with_schema",
            conn_type=CONN_TYPE,
            host="https://hooks.slack.com/services/",
            password=f"/{TEST_TOKEN}",
        ),
        Connection(
            conn_id="conn_host_without_schema",
            conn_type=CONN_TYPE,
            host="hooks.slack.com/services/",
            password=f"/{TEST_TOKEN}",
        ),
        Connection(
            conn_id="conn_parts",
            conn_type=CONN_TYPE,
            host="hooks.slack.com/services",
            schema="https",
            password=f"/{TEST_TOKEN}",
        ),
        Connection(
            conn_id="conn_deprecated_extra",
            conn_type=CONN_TYPE,
            host="https://hooks.slack.com/services/",
            extra={"webhook_token": TEST_TOKEN},
        ),
        Connection(
            conn_id="conn_custom_endpoint_1",
            conn_type=CONN_TYPE,
            schema=TEST_CUSTOM_SCHEMA,
            host=TEST_CUSTOM_ENDPOINT,
            password=TEST_TOKEN,
        ),
        Connection(
            conn_id="conn_custom_endpoint_2",
            conn_type=CONN_TYPE,
            host=f"{TEST_CUSTOM_SCHEMA}://{TEST_CUSTOM_ENDPOINT}",
            password=TEST_TOKEN,
        ),
        Connection(
            conn_id="conn_custom_endpoint_3",
            conn_type=CONN_TYPE,
            password=TEST_CUSTOM_WEBHOOK_URL,
        ),
        Connection(
            conn_id="conn_empty",
            conn_type=CONN_TYPE,
        ),
        Connection(
            conn_id="conn_password_empty_1",
            conn_type=CONN_TYPE,
            host="https://hooks.slack.com/services/",
        ),
        Connection(
            conn_id="conn_password_empty_2",
            conn_type=CONN_TYPE,
            schema="http",
            host="some.netloc",
        ),
        # Not supported anymore
        Connection(conn_id="conn_token_in_host_1", conn_type=CONN_TYPE, host=TEST_WEBHOOK_URL),
        Connection(
            conn_id="conn_token_in_host_2",
            conn_type=CONN_TYPE,
            schema="https",
            host="hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
        ),
    ]
    with pytest.MonkeyPatch.context() as mp:
        for conn in connections:
            mp.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", conn.get_uri())
        yield


class TestCheckWebhookResponseDecorator:
    def test_ok_response(self):
        """Test OK response."""

        @check_webhook_response
        def decorated():
            return MOCK_WEBHOOK_RESPONSE

        assert decorated() is MOCK_WEBHOOK_RESPONSE

    @pytest.mark.parametrize(
        ("status_code", "body"),
        [
            (400, "invalid_payload"),
            (403, "action_prohibited"),
            (404, "channel_not_found"),
            (410, "channel_is_archived"),
            (500, "rollup_error"),
            (418, "i_am_teapot"),
        ],
    )
    def test_error_response(self, status_code, body):
        """Test error response."""
        test_response = WebhookResponse(url="foo://bar", status_code=status_code, body=body, headers={})

        @check_webhook_response
        def decorated():
            return test_response

        error_message = rf"Response body: '{body}', Status Code: {status_code}\."
        with pytest.raises(AirflowException, match=error_message):
            assert decorated()


class TestAsyncCheckWebhookResponseDecorator:
    @pytest.mark.asyncio
    async def test_ok_response(self):
        """Test async decorator with OK response."""

        @async_check_webhook_response
        async def decorated():
            return MOCK_WEBHOOK_RESPONSE

        assert await decorated() is MOCK_WEBHOOK_RESPONSE

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status_code", "body"),
        [
            (400, "invalid_payload"),
            (403, "action_prohibited"),
            (404, "channel_not_found"),
            (410, "channel_is_archived"),
            (500, "rollup_error"),
            (418, "i_am_teapot"),
        ],
    )
    async def test_error_response(self, status_code, body):
        """Test async decorator with error response."""
        test_response = WebhookResponse(url="foo://bar", status_code=status_code, body=body, headers={})

        @async_check_webhook_response
        async def decorated():
            return test_response

        error_message = rf"Response body: '{body}', Status Code: {status_code}\."
        with pytest.raises(AirflowException, match=error_message):
            await decorated()


class TestSlackWebhookHook:
    @pytest.mark.parametrize(
        "conn_id",
        [
            TEST_CONN_ID,
            "conn_full_url_connection",
            "conn_full_url_connection_with_host",
            "conn_host_with_schema",
            "conn_host_without_schema",
            "conn_parts",
        ],
    )
    def test_construct_webhook_url(self, conn_id):
        """Test valid connections."""
        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        conn_params = hook._get_conn_params()
        assert not hasattr(hook, "http_conn_id")
        assert not hasattr(hook, "webhook_token")
        assert "url" in conn_params
        assert conn_params["url"] == TEST_WEBHOOK_URL

    def test_ignore_webhook_token(self):
        """Test that we only use token from Slack API Connection ID."""
        with pytest.warns(
            UserWarning, match="Provide `webhook_token` as part of .* parameters is disallowed"
        ):
            hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID, webhook_token="foo-bar")
        assert "webhook_token" not in hook.extra_client_args
        assert hook._get_conn_params()["url"] == TEST_WEBHOOK_URL

    @pytest.mark.parametrize("conn_id", ["conn_token_in_host_1", "conn_token_in_host_2"])
    def test_wrong_connections(self, conn_id):
        """Test previously valid connections, but now support of it is dropped."""
        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        with pytest.raises(AirflowNotFoundException, match="does not contain password"):
            hook._get_conn_params()

    @pytest.mark.parametrize(
        "conn_id", ["conn_custom_endpoint_1", "conn_custom_endpoint_2", "conn_custom_endpoint_3"]
    )
    def test_construct_webhook_url_with_non_default_host(self, conn_id):
        """Test valid connections with endpoint != https://hooks.slack.com/hooks."""
        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        conn_params = hook._get_conn_params()
        assert "url" in conn_params
        assert conn_params["url"] == TEST_CUSTOM_WEBHOOK_URL

    @pytest.mark.parametrize(
        "conn_id",
        [
            "conn_empty",
            "conn_password_empty_1",
            "conn_password_empty_2",
        ],
    )
    def test_no_password_in_connection_field(self, conn_id):
        """Test connection which missing password field in connection."""
        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        error_message = r"Connection ID .* does not contain password \(Slack Webhook Token\)"
        with pytest.raises(AirflowNotFoundException, match=error_message):
            hook._get_conn_params()

    @pytest.mark.parametrize(
        ("hook_config", "conn_extra", "expected"),
        [
            (  # Test Case: hook config
                {
                    "timeout": 42,
                    "proxy": "https://hook-proxy:1234",
                    "retry_handlers": [TEST_CONN_ERROR_RETRY_HANDLER, TEST_RATE_LIMIT_RETRY_HANDLER],
                },
                {},
                {
                    "timeout": 42,
                    "proxy": "https://hook-proxy:1234",
                    "retry_handlers": [TEST_CONN_ERROR_RETRY_HANDLER, TEST_RATE_LIMIT_RETRY_HANDLER],
                },
            ),
            (  # Test Case: connection config
                {},
                {
                    "timeout": 9000,
                    "proxy": "https://conn-proxy:4321",
                },
                {
                    "timeout": 9000,
                    "proxy": "https://conn-proxy:4321",
                },
            ),
            (  # Test Case: Connection from the UI
                {},
                {
                    "extra__slackwebhook__timeout": 9000,
                    "extra__slackwebhook__proxy": "https://conn-proxy:4321",
                },
                {
                    "timeout": 9000,
                    "proxy": "https://conn-proxy:4321",
                },
            ),
            (  # Test Case: Merge configs - hook args overwrite conn config
                {
                    "timeout": 1,
                    "proxy": "https://hook-proxy:777",
                },
                {
                    "timeout": 9000,
                    "proxy": "https://conn-proxy:4321",
                },
                {
                    "timeout": 1,
                    "proxy": "https://hook-proxy:777",
                },
            ),
            (  # Test Case: Merge configs - resolve config
                {
                    "timeout": 1,
                },
                {
                    "timeout": 9000,
                    "proxy": "https://conn-proxy:4334",
                },
                {
                    "timeout": 1,
                    "proxy": "https://conn-proxy:4334",
                },
            ),
            (  # Test Case: empty configs
                {},
                {},
                {},
            ),
            (  # Test Case: extra_client_args
                {"foo": "bar"},
                {},
                {"foo": "bar"},
            ),
            (  # Test Case: ignored not expected connection extra
                {},
                {"spam": "egg"},
                {},
            ),
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_client_configuration(
        self, mock_webhook_client_cls, hook_config, conn_extra, expected: dict[str, Any]
    ):
        """Test read/parse/merge WebhookClient config from connection and hook arguments."""
        expected["url"] = TEST_WEBHOOK_URL
        test_conn = Connection(
            conn_id="test-slack-incoming-webhook-conn",
            conn_type=CONN_TYPE,
            password=TEST_WEBHOOK_URL,
            extra=conn_extra,
        )
        test_conn_env = f"AIRFLOW_CONN_{test_conn.conn_id.upper()}"
        mock_webhook_client = mock_webhook_client_cls.return_value

        with mock.patch.dict("os.environ", values={test_conn_env: test_conn.get_uri()}):
            hook = SlackWebhookHook(slack_webhook_conn_id=test_conn.conn_id, **hook_config)
            expected["logger"] = hook.log
            conn_params = hook._get_conn_params()
            assert conn_params == expected

            client = hook.client
            assert client == mock_webhook_client
            assert hook.get_conn() == mock_webhook_client
            assert hook.get_conn() is client  # cached
            mock_webhook_client_cls.assert_called_once_with(**expected)

    @pytest.mark.parametrize("headers", [None, {"User-Agent": "Airflow"}])
    @pytest.mark.parametrize(
        "send_body",
        [
            {"text": "Test Text"},
            {"text": "Fallback Text", "blocks": ["Dummy Block"]},
            {"text": "Fallback Text", "blocks": ["Dummy Block"], "unfurl_media": True, "unfurl_links": True},
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_hook_send_dict(self, mock_webhook_client_cls, send_body, headers):
        """Test `SlackWebhookHook.send_dict` method."""
        mock_webhook_client = mock_webhook_client_cls.return_value
        mock_webhook_client_send_dict = mock_webhook_client.send_dict
        mock_webhook_client_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        # Test with regular dictionary
        hook.send_dict(body=send_body, headers=headers)
        mock_webhook_client_send_dict.assert_called_once_with(send_body, headers=headers)

        # Test with JSON-string
        mock_webhook_client_send_dict.reset_mock()
        hook.send_dict(body=json.dumps(send_body), headers=headers)
        mock_webhook_client_send_dict.assert_called_once_with(send_body, headers=headers)

    @pytest.mark.parametrize("send_body", [("text", "Test Text"), 42, "null", "42"])
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_hook_send_dict_invalid_type(self, mock_webhook_client_cls, send_body):
        """Test invalid body type for `SlackWebhookHook.send_dict` method."""
        mock_webhook_client = mock_webhook_client_cls.return_value
        mock_webhook_client_send_dict = mock_webhook_client.send_dict
        mock_webhook_client_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        with pytest.raises(TypeError, match=r"Body expected dictionary, got .*\."):
            hook.send_dict(body=send_body)
        assert mock_webhook_client_send_dict.assert_not_called

    @pytest.mark.parametrize("json_string", ["{'text': 'Single quotes'}", '{"text": "Missing }"'])
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_hook_send_dict_invalid_json_string(self, mock_webhook_client_cls, json_string):
        """Test invalid JSON-string passed to `SlackWebhookHook.send_dict` method."""
        mock_webhook_client = mock_webhook_client_cls.return_value
        mock_webhook_client_send_dict = mock_webhook_client.send_dict
        mock_webhook_client_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        error_message = r"Body expected valid JSON string, got .*\. Original error:.*"
        with pytest.raises(AirflowException, match=error_message):
            hook.send_dict(body=json_string)
        assert mock_webhook_client_send_dict.assert_not_called

    @pytest.mark.parametrize(
        "legacy_attr",
        [
            "channel",
            "username",
            "icon_emoji",
            "icon_url",
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_hook_send_dict_legacy_slack_integration(self, mock_webhook_client_cls, legacy_attr):
        """Test `SlackWebhookHook.send_dict` warn users about Legacy Slack Integrations."""
        mock_webhook_client = mock_webhook_client_cls.return_value
        mock_webhook_client_send_dict = mock_webhook_client.send_dict
        mock_webhook_client_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        legacy_slack_integration_body = {legacy_attr: "test-value"}
        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        warning_message = (
            r"You cannot override the default channel \(chosen by the user who installed your app\), "
            r"username, or icon when you're using Incoming Webhooks to post messages\. "
            r"Instead, these values will always inherit from the associated Slack app configuration\. "
            r"See: .*\. It is possible to change this values only in "
            r"Legacy Slack Integration Incoming Webhook: .*"
        )
        with pytest.warns(UserWarning, match=warning_message):
            hook.send_dict(body=legacy_slack_integration_body)
        mock_webhook_client_send_dict.assert_called_once_with(legacy_slack_integration_body, headers=None)

    @pytest.mark.parametrize("headers", [None, {"User-Agent": "Airflow"}])
    @pytest.mark.parametrize(
        "send_params",
        [
            {"text": "Test Text"},
            {"text": "Fallback Text", "blocks": ["Dummy Block"]},
            {"text": "Fallback Text", "blocks": ["Dummy Block"], "unfurl_media": True, "unfurl_links": True},
            {"legacy": "value"},
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook.send_dict")
    def test_hook_send(self, mock_hook_send_dict, send_params, headers):
        """Test `SlackWebhookHook.send` method."""
        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        hook.send(**send_params, headers=headers)
        mock_hook_send_dict.assert_called_once_with(body=send_params, headers=headers)

    @pytest.mark.parametrize("headers", [None, {"User-Agent": "Airflow"}])
    @pytest.mark.parametrize("unfurl_links", [None, False, True])
    @pytest.mark.parametrize("unfurl_media", [None, False, True])
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook.send")
    def test_hook_send_text(self, mock_hook_send, headers, unfurl_links, unfurl_media):
        """Test `SlackWebhookHook.send_text` method."""
        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        hook.send_text("Test Text", headers=headers, unfurl_links=unfurl_links, unfurl_media=unfurl_media)
        mock_hook_send.assert_called_once_with(
            text="Test Text", headers=headers, unfurl_links=unfurl_links, unfurl_media=unfurl_media
        )

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param(
                "a://:abc@?extra__slackwebhook__timeout=123&extra__slackwebhook__proxy=proxy",
                id="prefix",
            ),
            pytest.param("a://:abc@?timeout=123&proxy=proxy", id="no-prefix"),
        ],
    )
    def test_backcompat_prefix_works(self, uri):
        with patch.dict(in_dict=os.environ, AIRFLOW_CONN_MY_CONN=uri):
            hook = SlackWebhookHook(slack_webhook_conn_id="my_conn")
            params = hook._get_conn_params()
            assert params["url"] == "https://hooks.slack.com/services/abc"
            assert params["timeout"] == 123
            assert params["proxy"] == "proxy"

    def test_backcompat_prefix_both_causes_warning(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN="a://:abc@?extra__slackwebhook__timeout=111&timeout=222",
        ):
            hook = SlackWebhookHook(slack_webhook_conn_id="my_conn")
            with pytest.warns(Warning, match="Using value for `timeout`"):
                params = hook._get_conn_params()
            assert params["timeout"] == 222

    def test_empty_string_ignored_prefixed(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN=json.dumps(
                {
                    "password": "hi",
                    "extra": {"extra__slackwebhook__proxy": ""},
                }
            ),
        ):
            hook = SlackWebhookHook(slack_webhook_conn_id="my_conn")
            params = hook._get_conn_params()
            assert "proxy" not in params

    def test_empty_string_ignored_non_prefixed(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN=json.dumps({"password": "hi", "extra": {"proxy": ""}}),
        ):
            hook = SlackWebhookHook(slack_webhook_conn_id="my_conn")
            params = hook._get_conn_params()
            assert "proxy" not in params


class TestSlackWebhookHookAsync:
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook._async_get_conn_params")
    async def test_async_client(self, mock_async_get_conn_params):
        """Test async_client property creates AsyncWebhookClient with correct params."""
        mock_async_get_conn_params.return_value = {"url": TEST_WEBHOOK_URL}

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        client = await hook.get_async_client()

        assert isinstance(client, AsyncWebhookClient)
        assert client.url == TEST_WEBHOOK_URL
        mock_async_get_conn_params.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("headers", [None, {"User-Agent": "Airflow"}])
    @pytest.mark.parametrize(
        "send_body",
        [
            {"text": "Test Text"},
            {"text": "Fallback Text", "blocks": ["Dummy Block"]},
            {"text": "Fallback Text", "blocks": ["Dummy Block"], "unfurl_media": True, "unfurl_links": True},
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.AsyncWebhookClient")
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook._async_get_conn_params")
    async def test_async_send_dict(
        self, mock_async_get_conn_params, mock_async_webhook_client_cls, send_body, headers
    ):
        """Test async_send_dict method with dict input."""
        mock_async_get_conn_params.return_value = {"url": TEST_WEBHOOK_URL}
        mock_async_client = mock_async_webhook_client_cls.return_value
        mock_async_client.send_dict = mock.AsyncMock(return_value=MOCK_WEBHOOK_RESPONSE)

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        resp = await hook.async_send_dict(body=send_body, headers=headers)

        assert resp == MOCK_WEBHOOK_RESPONSE
        mock_async_client.send_dict.assert_called_once_with(send_body, headers=headers)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("headers", [None, {"User-Agent": "Airflow"}])
    @pytest.mark.parametrize(
        "send_body",
        [
            {"text": "Test Text"},
            {"text": "Fallback Text", "blocks": ["Dummy Block"]},
            {"text": "Fallback Text", "blocks": ["Dummy Block"], "unfurl_media": True, "unfurl_links": True},
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.AsyncWebhookClient")
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook._async_get_conn_params")
    async def test_async_send_dict_json_string(
        self, mock_async_get_conn_params, mock_async_webhook_client_cls, send_body, headers
    ):
        """Test async_send_dict method with JSON string input."""
        mock_async_get_conn_params.return_value = {"url": TEST_WEBHOOK_URL}
        mock_async_client = mock_async_webhook_client_cls.return_value
        mock_async_client.send_dict = mock.AsyncMock(return_value=MOCK_WEBHOOK_RESPONSE)

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        resp = await hook.async_send_dict(body=json.dumps(send_body), headers=headers)

        assert resp == MOCK_WEBHOOK_RESPONSE
        mock_async_client.send_dict.assert_called_once_with(send_body, headers=headers)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("headers", [None, {"User-Agent": "Airflow"}])
    @pytest.mark.parametrize(
        "send_params",
        [
            {"text": "Test Text"},
            {"text": "Fallback Text", "blocks": ["Dummy Block"]},
            {"text": "Fallback Text", "blocks": ["Dummy Block"], "unfurl_media": True, "unfurl_links": True},
            {"legacy": "value"},
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook.async_send_dict")
    async def test_async_send(self, mock_async_send_dict, send_params, headers):
        """Test at async_send method."""
        mock_async_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        resp = await hook.async_send(**send_params, headers=headers)

        assert resp == MOCK_WEBHOOK_RESPONSE
        mock_async_send_dict.assert_called_once_with(body=send_params, headers=headers)
