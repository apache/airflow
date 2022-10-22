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
from pytest import param
from slack_sdk.http_retry.builtin_handlers import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler
from slack_sdk.webhook.webhook_response import WebhookResponse

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook, check_webhook_response
from tests.test_utils.providers import get_provider_min_airflow_version, object_exists

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
        Connection(conn_id="conn_token_in_host_1", conn_type=CONN_TYPE, host=TEST_WEBHOOK_URL),
        Connection(
            conn_id="conn_token_in_host_2",
            conn_type=CONN_TYPE,
            schema="https",
            host="hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
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
    ]

    conn_uris = {f"AIRFLOW_CONN_{c.conn_id.upper()}": c.get_uri() for c in connections}

    with mock.patch.dict("os.environ", values=conn_uris):
        yield


class TestCheckWebhookResponseDecorator:
    def test_ok_response(self):
        """Test OK response."""

        @check_webhook_response
        def decorated():
            return MOCK_WEBHOOK_RESPONSE

        assert decorated() is MOCK_WEBHOOK_RESPONSE

    @pytest.mark.parametrize(
        "status_code,body",
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

        error_message = fr"Response body: '{body}', Status Code: {status_code}\."
        with pytest.raises(AirflowException, match=error_message):
            assert decorated()


class TestSlackWebhookHook:
    def test_no_credentials(self):
        """Test missing credentials."""
        error_message = r"Either `slack_webhook_conn_id` or `webhook_token` should be provided\."
        with pytest.raises(AirflowException, match=error_message):
            SlackWebhookHook(slack_webhook_conn_id=None, webhook_token=None)

    @mock.patch("airflow.providers.slack.hooks.slack_webhook.mask_secret")
    def test_webhook_token(self, mock_mask_secret):
        webhook_token = "test-value"
        warning_message = (
            r"Provide `webhook_token` as hook argument deprecated by security reason and will be removed "
            r"in a future releases. Please specify it in `Slack Webhook` connection\."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            SlackWebhookHook(webhook_token=webhook_token)
        mock_mask_secret.assert_called_once_with(webhook_token)

    def test_conn_id(self):
        """Different conn_id arguments and options."""
        hook = SlackWebhookHook(slack_webhook_conn_id=SlackWebhookHook.default_conn_name, http_conn_id=None)
        assert hook.slack_webhook_conn_id == SlackWebhookHook.default_conn_name
        assert not hasattr(hook, "http_conn_id")

        hook = SlackWebhookHook(slack_webhook_conn_id=None, http_conn_id=SlackWebhookHook.default_conn_name)
        assert hook.slack_webhook_conn_id == SlackWebhookHook.default_conn_name
        assert not hasattr(hook, "http_conn_id")

        error_message = "You cannot provide both `slack_webhook_conn_id` and `http_conn_id`."
        with pytest.raises(AirflowException, match=error_message):
            SlackWebhookHook(
                slack_webhook_conn_id=SlackWebhookHook.default_conn_name,
                http_conn_id=SlackWebhookHook.default_conn_name,
            )

    @pytest.mark.parametrize(
        "conn_id",
        [
            TEST_CONN_ID,
            "conn_full_url_connection",
            "conn_full_url_connection_with_host",
            "conn_host_with_schema",
            "conn_host_without_schema",
            "conn_parts",
            "conn_token_in_host_1",
            "conn_token_in_host_2",
        ],
    )
    def test_construct_webhook_url(self, conn_id):
        """Test valid connections."""
        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        conn_params = hook._get_conn_params()
        assert "url" in conn_params
        assert conn_params["url"] == TEST_WEBHOOK_URL

    @mock.patch("airflow.providers.slack.hooks.slack_webhook.mask_secret")
    @pytest.mark.parametrize("conn_id", ["conn_token_in_host_1", "conn_token_in_host_2"])
    def test_construct_webhook_url_deprecated_full_url_in_host(self, mock_mask_secret, conn_id):
        """Test deprecated option with full URL in host/schema and empty password."""
        hook = SlackWebhookHook(slack_webhook_conn_id=conn_id)
        warning_message = (
            r"Found Slack Webhook Token URL in Connection .* `host` and `password` field is empty\."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            conn_params = hook._get_conn_params()
        mock_mask_secret.assert_called_once_with(mock.ANY)
        assert "url" in conn_params
        assert conn_params["url"] == TEST_WEBHOOK_URL

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
        error_message = r"Cannot get token\: No valid Slack token nor valid Connection ID supplied\."
        with pytest.raises(AirflowException, match=error_message):
            hook._get_conn_params()

    @pytest.mark.parametrize("conn_id", [None, "conn_empty"])
    @pytest.mark.parametrize("token", [TEST_TOKEN, TEST_WEBHOOK_URL, f"/{TEST_TOKEN}"])
    def test_empty_connection_field_with_token(self, conn_id, token):
        """Test connections which is empty or not set and valid webhook_token specified."""
        hook = SlackWebhookHook(slack_webhook_conn_id="conn_empty", webhook_token=token)
        conn_params = hook._get_conn_params()
        assert "url" in conn_params
        assert conn_params["url"] == TEST_WEBHOOK_URL

    @pytest.mark.parametrize(
        "hook_config,conn_extra,expected",
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
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook.send_dict")
    def test_hook_send(self, mock_hook_send_dict, send_params, headers):
        """Test `SlackWebhookHook.send` method."""
        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        hook.send(**send_params, headers=headers)
        mock_hook_send_dict.assert_called_once_with(body=send_params, headers=headers)

    @pytest.mark.parametrize(
        "deprecated_hook_attr",
        [
            "message",
            "attachments",
            "blocks",
            "channel",
            "username",
            "icon_emoji",
            "icon_url",
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook.send_dict")
    def test_hook_send_by_hook_attributes(self, mock_hook_send_dict, deprecated_hook_attr):
        """Test `SlackWebhookHook.send` with parameters set in hook attributes."""
        send_params = {deprecated_hook_attr: "test-value"}
        expected_body = {deprecated_hook_attr if deprecated_hook_attr != "message" else "text": "test-value"}
        warning_message = (
            r"Provide .* as hook argument\(s\) is deprecated and will be removed in a future releases\. "
            r"Please specify attributes in `SlackWebhookHook\.send` method instead\."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID, **send_params)
        assert getattr(hook, deprecated_hook_attr) == "test-value"
        if deprecated_hook_attr == "message":
            assert getattr(hook, "text") == "test-value"
        # Test ``.send()`` method
        hook.send()
        mock_hook_send_dict.assert_called_once_with(body=expected_body, headers=None)

        # Test deprecated ``.execute()`` method
        mock_hook_send_dict.reset_mock()
        warning_message = (
            "`SlackWebhookHook.execute` method deprecated and will be removed in a future releases. "
            "Please use `SlackWebhookHook.send` or `SlackWebhookHook.send_dict` or "
            "`SlackWebhookHook.send_text` methods instead."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            hook.execute()
        mock_hook_send_dict.assert_called_once_with(body=expected_body, headers=None)

    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_hook_ignored_attributes(self, mock_webhook_client_cls, recwarn):
        """Test hook constructor warn users about ignored attributes."""
        mock_webhook_client = mock_webhook_client_cls.return_value
        mock_webhook_client_send_dict = mock_webhook_client.send_dict
        mock_webhook_client_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID, link_names="test-value")
        assert len(recwarn) == 2
        assert str(recwarn.pop(UserWarning).message).startswith(
            "`link_names` has no affect, if you want to mention user see:"
        )
        assert str(recwarn.pop(DeprecationWarning).message).startswith(
            "Provide 'link_names' as hook argument(s) is deprecated and will be removed in a future releases."
        )
        hook.send()
        mock_webhook_client_send_dict.assert_called_once_with({}, headers=None)

    @mock.patch("airflow.providers.slack.hooks.slack_webhook.WebhookClient")
    def test_hook_send_unexpected_arguments(self, mock_webhook_client_cls, recwarn):
        """Test `SlackWebhookHook.send` unexpected attributes."""
        mock_webhook_client = mock_webhook_client_cls.return_value
        mock_webhook_client_send_dict = mock_webhook_client.send_dict
        mock_webhook_client_send_dict.return_value = MOCK_WEBHOOK_RESPONSE

        hook = SlackWebhookHook(slack_webhook_conn_id=TEST_CONN_ID)
        warning_message = (
            r"Found unexpected keyword-argument\(s\) 'link_names', 'as_user' "
            r"in `send` method\. This argument\(s\) have no effect\."
        )
        with pytest.warns(UserWarning, match=warning_message):
            hook.send(link_names="foo-bar", as_user="root", text="Awesome!")

        mock_webhook_client_send_dict.assert_called_once_with({"text": "Awesome!"}, headers=None)

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

    def test__ensure_prefixes_removal(self):
        """Ensure that _ensure_prefixes is removed from snowflake when airflow min version >= 2.5.0."""
        path = 'airflow.providers.slack.hooks.slack_webhook._ensure_prefixes'
        if not object_exists(path):
            raise Exception(
                "You must remove this test. It only exists to "
                "remind us to remove decorator `_ensure_prefixes`."
            )

        if get_provider_min_airflow_version('apache-airflow-providers-slack') >= (2, 5):
            raise Exception(
                "You must now remove `_ensure_prefixes` from SlackWebhookHook."
                " The functionality is now taken care of by providers manager."
            )

    def test___ensure_prefixes(self):
        """
        Check that ensure_prefixes decorator working properly

        Note: remove this test when removing ensure_prefixes (after min airflow version >= 2.5.0
        """
        assert list(SlackWebhookHook.get_ui_field_behaviour()['placeholders'].keys()) == [
            'schema',
            'host',
            'password',
            'extra__slackwebhook__timeout',
            'extra__slackwebhook__proxy',
        ]

    @pytest.mark.parametrize(
        'uri',
        [
            param(
                'a://:abc@?extra__slackwebhook__timeout=123&extra__slackwebhook__proxy=proxy',
                id='prefix',
            ),
            param('a://:abc@?timeout=123&proxy=proxy', id='no-prefix'),
        ],
    )
    def test_backcompat_prefix_works(self, uri):
        with patch.dict(in_dict=os.environ, AIRFLOW_CONN_MY_CONN=uri):
            hook = SlackWebhookHook(slack_webhook_conn_id='my_conn')
            params = hook._get_conn_params()
            assert params["url"] == "https://hooks.slack.com/services/abc"
            assert params["timeout"] == 123
            assert params["proxy"] == "proxy"

    def test_backcompat_prefix_both_causes_warning(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN='a://:abc@?extra__slackwebhook__timeout=111&timeout=222',
        ):
            hook = SlackWebhookHook(slack_webhook_conn_id='my_conn')
            with pytest.warns(Warning, match='Using value for `timeout`'):
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
            hook = SlackWebhookHook(slack_webhook_conn_id='my_conn')
            params = hook._get_conn_params()
            assert 'proxy' not in params

    def test_empty_string_ignored_non_prefixed(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN=json.dumps({"password": "hi", "extra": {"proxy": ""}}),
        ):
            hook = SlackWebhookHook(slack_webhook_conn_id='my_conn')
            params = hook._get_conn_params()
            assert 'proxy' not in params
