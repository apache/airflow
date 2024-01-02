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
import os
from typing import Any
from unittest import mock
from unittest.mock import patch

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler
from slack_sdk.web.slack_response import SlackResponse

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.slack.hooks.slack import SlackHook

MOCK_SLACK_API_TOKEN = "xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx"
SLACK_API_DEFAULT_CONN_ID = SlackHook.default_conn_name
SLACK_CONN_TYPE = "slack"
TEST_CONN_ERROR_RETRY_HANDLER = ConnectionErrorRetryHandler(max_retry_count=42)
TEST_RATE_LIMIT_RETRY_HANDLER = RateLimitErrorRetryHandler()

CONN_TYPE = "slack-incoming-webhook"
VALID_CONN_IDS = [
    SlackHook.default_conn_name,
    "conn_full_url_connection",
    "conn_host_with_schema",
    "conn_parts",
]


@pytest.fixture(scope="module", autouse=True)
def slack_api_connections():
    """Create tests connections."""
    connections = [
        Connection(
            conn_id=SLACK_API_DEFAULT_CONN_ID,
            conn_type=CONN_TYPE,
            password=MOCK_SLACK_API_TOKEN,
        ),
        Connection(
            conn_id="compat_http_type",
            conn_type="http",
            password=MOCK_SLACK_API_TOKEN,
        ),
        Connection(
            conn_id="empty_slack_connection",
            conn_type=CONN_TYPE,
        ),
    ]

    with pytest.MonkeyPatch.context() as mp:
        for conn in connections:
            mp.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", conn.get_uri())
        yield


class TestSlackHook:
    @pytest.mark.parametrize(
        "conn_id",
        [
            SLACK_API_DEFAULT_CONN_ID,
            "compat_http_type",  # In case if users use Slack Webhook Connection ID from UI for Slack API
        ],
    )
    def test_get_token_from_connection(self, conn_id):
        """Test retrieve token from Slack API Connection ID."""
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        assert hook._get_conn_params()["token"] == MOCK_SLACK_API_TOKEN

    def test_resolve_token(self):
        """Test that we only use token from Slack API Connection ID."""
        with pytest.warns(UserWarning, match="Provide `token` as part of .* parameters is disallowed"):
            hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID, token="foo-bar")
            assert "token" not in hook.extra_client_args
            assert hook._get_conn_params()["token"] == MOCK_SLACK_API_TOKEN

    def test_empty_password(self):
        """Test password field defined in the connection."""
        hook = SlackHook(slack_conn_id="empty_slack_connection")
        error_message = r"Connection ID '.*' does not contain password \(Slack API Token\)\."
        with pytest.raises(AirflowNotFoundException, match=error_message):
            hook._get_conn_params()

    @pytest.mark.parametrize(
        "hook_config,conn_extra,expected",
        [
            (  # Test Case: hook config
                {
                    "timeout": 42,
                    "base_url": "http://hook-base-url:1234",
                    "proxy": "https://hook-proxy:1234",
                    "retry_handlers": [TEST_CONN_ERROR_RETRY_HANDLER, TEST_RATE_LIMIT_RETRY_HANDLER],
                },
                {},
                {
                    "timeout": 42,
                    "base_url": "http://hook-base-url:1234",
                    "proxy": "https://hook-proxy:1234",
                    "retry_handlers": [TEST_CONN_ERROR_RETRY_HANDLER, TEST_RATE_LIMIT_RETRY_HANDLER],
                },
            ),
            (  # Test Case: connection config
                {},
                {
                    "timeout": "9000",
                    "base_url": "http://conn-base-url:4321",
                    "proxy": "https://conn-proxy:4321",
                },
                {
                    "timeout": 9000,
                    "base_url": "http://conn-base-url:4321",
                    "proxy": "https://conn-proxy:4321",
                },
            ),
            (  # Test Case: Connection from the UI
                {},
                {
                    "extra__slack__timeout": 9000,
                    "extra__slack__base_url": "http://conn-base-url:4321",
                    "extra__slack__proxy": "https://conn-proxy:4321",
                },
                {
                    "timeout": 9000,
                    "base_url": "http://conn-base-url:4321",
                    "proxy": "https://conn-proxy:4321",
                },
            ),
            (  # Test Case: Merge configs - hook args overwrite conn config
                {
                    "timeout": 1,
                    "proxy": "https://hook-proxy:777",
                    "retry_handlers": [TEST_RATE_LIMIT_RETRY_HANDLER],
                },
                {
                    "timeout": 9000,
                    "proxy": "https://conn-proxy:4321",
                },
                {
                    "timeout": 1,
                    "proxy": "https://hook-proxy:777",
                    "retry_handlers": [TEST_RATE_LIMIT_RETRY_HANDLER],
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
    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    def test_client_configuration(
        self, mock_webclient_cls, hook_config, conn_extra, expected: dict[str, Any]
    ):
        """Test read/parse/merge WebClient config from connection and hook arguments."""
        expected["token"] = MOCK_SLACK_API_TOKEN
        test_conn = Connection(
            conn_id="test-slack-conn",
            conn_type=CONN_TYPE,
            password=MOCK_SLACK_API_TOKEN,
            extra=conn_extra,
        )
        test_conn_env = f"AIRFLOW_CONN_{test_conn.conn_id.upper()}"
        mock_webclient = mock_webclient_cls.return_value

        with mock.patch.dict("os.environ", values={test_conn_env: test_conn.get_uri()}):
            hook = SlackHook(slack_conn_id=test_conn.conn_id, **hook_config)
            expected["logger"] = hook.log
            conn_params = hook._get_conn_params()
            assert conn_params == expected

            client = hook.client
            assert client == mock_webclient
            assert hook.get_conn() == mock_webclient
            assert hook.get_conn() is client  # cached
            mock_webclient_cls.assert_called_once_with(**expected)

    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    def test_call_with_failure(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        expected_exception = SlackApiError(message="foo", response="bar")
        slack_client_mock.api_call = mock.Mock(side_effect=expected_exception)

        slack_hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        test_method = "test_method"
        test_api_params = {"key1": "value1", "key2": "value2"}

        with pytest.raises(SlackApiError):
            slack_hook.call(test_method, data=test_api_params)

    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    def test_api_call(self, slack_client_class_mock):
        slack_client_mock = mock.Mock()
        slack_client_class_mock.return_value = slack_client_mock
        slack_client_mock.api_call.return_value = {"ok": True}

        slack_hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        test_api_json = {"channel": "test_channel"}

        slack_hook.call("chat.postMessage", json=test_api_json)
        slack_client_mock.api_call.assert_called_with("chat.postMessage", json=test_api_json)

    @pytest.mark.parametrize(
        "response_data",
        [
            {
                "ok": True,
                "url": "https://subarachnoid.slack.com/",
                "team": "Subarachnoid Workspace",
                "user": "grace",
                "team_id": "T12345678",
                "user_id": "W12345678",
            },
            {
                "ok": True,
                "url": "https://subarachnoid.slack.com/",
                "team": "Subarachnoid Workspace",
                "user": "bot",
                "team_id": "T0G9PQBBK",
                "user_id": "W23456789",
                "bot_id": "BZYBOTHED",
            },
            b"some-binary-data",
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    def test_hook_connection_success(self, mock_webclient_cls, response_data):
        """Test SlackHook success connection."""
        mock_webclient = mock_webclient_cls.return_value
        mock_webclient_call = mock_webclient.api_call
        mock_webclient_call.return_value = SlackResponse(
            status_code=200,
            data=response_data,
            # Mock other mandatory SlackResponse arguments
            **{ma: mock.MagicMock for ma in ("client", "http_verb", "api_url", "req_args", "headers")},
        )
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        conn_test = hook.test_connection()
        mock_webclient_call.assert_called_once_with("auth.test")
        assert conn_test[0]

    @pytest.mark.parametrize(
        "response_data",
        [
            {"ok": False, "error": "invalid_auth"},
            {"ok": False, "error": "not_authed"},
            b"some-binary-data",
        ],
    )
    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    def test_hook_connection_failed(self, mock_webclient_cls, response_data):
        """Test SlackHook failure connection."""
        mock_webclient = mock_webclient_cls.return_value
        mock_webclient_call = mock_webclient.api_call
        mock_webclient_call.return_value = SlackResponse(
            status_code=401,
            data=response_data,
            # Mock other mandatory SlackResponse arguments
            **{ma: mock.MagicMock for ma in ("client", "http_verb", "api_url", "req_args", "headers")},
        )
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        conn_test = hook.test_connection()
        mock_webclient_call.assert_called_once_with("auth.test")
        assert not conn_test[0]

    @pytest.mark.parametrize("file,content", [(None, None), ("", ""), ("foo.bar", "test-content")])
    def test_send_file_wrong_parameters(self, file, content):
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        error_message = r"Either `file` or `content` must be provided, not both\."
        with pytest.raises(ValueError, match=error_message):
            hook.send_file(file=file, content=content)

    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    @pytest.mark.parametrize("initial_comment", [None, "test comment"])
    @pytest.mark.parametrize("title", [None, "test title"])
    @pytest.mark.parametrize("filetype", [None, "auto"])
    @pytest.mark.parametrize("channels", [None, "#random", "#random,#general", ("#random", "#general")])
    def test_send_file_path(
        self, mock_webclient_cls, tmp_path_factory, initial_comment, title, filetype, channels
    ):
        """Test send file by providing filepath."""
        mock_files_upload = mock.MagicMock()
        mock_webclient_cls.return_value.files_upload = mock_files_upload

        tmp = tmp_path_factory.mktemp("test_send_file_path")
        file = tmp / "test.json"
        file.write_text('{"foo": "bar"}')

        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        hook.send_file(
            channels=channels,
            file=file,
            filename="filename.mock",
            initial_comment=initial_comment,
            title=title,
            filetype=filetype,
        )

        mock_files_upload.assert_called_once_with(
            channels=channels,
            file=mock.ANY,  # Validate file properties later
            filename="filename.mock",
            initial_comment=initial_comment,
            title=title,
            filetype=filetype,
        )

        # Validate file properties
        mock_file = mock_files_upload.call_args.kwargs["file"]
        assert mock_file.mode == "rb"
        assert mock_file.name == str(file)

    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    @pytest.mark.parametrize("filename", ["test.json", "1.parquet.snappy"])
    def test_send_file_path_set_filename(self, mock_webclient_cls, tmp_path_factory, filename):
        """Test set filename in send_file method if it not set."""
        mock_files_upload = mock.MagicMock()
        mock_webclient_cls.return_value.files_upload = mock_files_upload

        tmp = tmp_path_factory.mktemp("test_send_file_path_set_filename")
        file = tmp / filename
        file.write_text('{"foo": "bar"}')

        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        hook.send_file(file=file)

        assert mock_files_upload.call_count == 1
        call_args = mock_files_upload.call_args.kwargs
        assert "filename" in call_args
        assert call_args["filename"] == filename

    @mock.patch("airflow.providers.slack.hooks.slack.WebClient")
    @pytest.mark.parametrize("initial_comment", [None, "test comment"])
    @pytest.mark.parametrize("title", [None, "test title"])
    @pytest.mark.parametrize("filetype", [None, "auto"])
    @pytest.mark.parametrize("filename", [None, "foo.bar"])
    @pytest.mark.parametrize("channels", [None, "#random", "#random,#general", ("#random", "#general")])
    def test_send_file_content(
        self, mock_webclient_cls, initial_comment, title, filetype, channels, filename
    ):
        """Test send file by providing content."""
        mock_files_upload = mock.MagicMock()
        mock_webclient_cls.return_value.files_upload = mock_files_upload
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        hook.send_file(
            channels=channels,
            content='{"foo": "bar"}',
            filename=filename,
            initial_comment=initial_comment,
            title=title,
            filetype=filetype,
        )
        mock_files_upload.assert_called_once_with(
            channels=channels,
            content='{"foo": "bar"}',
            filename=filename,
            initial_comment=initial_comment,
            title=title,
            filetype=filetype,
        )

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param(
                "a://:abc@?extra__slack__timeout=123"
                "&extra__slack__base_url=base_url"
                "&extra__slack__proxy=proxy",
                id="prefix",
            ),
            pytest.param("a://:abc@?timeout=123&base_url=base_url&proxy=proxy", id="no-prefix"),
        ],
    )
    def test_backcompat_prefix_works(self, uri):
        with patch.dict(os.environ, AIRFLOW_CONN_MY_CONN=uri):
            hook = SlackHook(slack_conn_id="my_conn")
            params = hook._get_conn_params()
            assert params["token"] == "abc"
            assert params["timeout"] == 123
            assert params["base_url"] == "base_url"
            assert params["proxy"] == "proxy"

    def test_backcompat_prefix_both_causes_warning(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN="a://:abc@?extra__slack__timeout=111&timeout=222",
        ):
            hook = SlackHook(slack_conn_id="my_conn")
            with pytest.warns(Warning, match="Using value for `timeout`"):
                params = hook._get_conn_params()
                assert params["timeout"] == 222

    def test_empty_string_ignored_prefixed(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN=json.dumps(
                {"password": "hi", "extra": {"extra__slack__base_url": "", "extra__slack__proxy": ""}}
            ),
        ):
            hook = SlackHook(slack_conn_id="my_conn")
            params = hook._get_conn_params()
            assert "proxy" not in params
            assert "base_url" not in params

    def test_empty_string_ignored_non_prefixed(self):
        with patch.dict(
            in_dict=os.environ,
            AIRFLOW_CONN_MY_CONN=json.dumps({"password": "hi", "extra": {"base_url": "", "proxy": ""}}),
        ):
            hook = SlackHook(slack_conn_id="my_conn")
            params = hook._get_conn_params()
            assert "proxy" not in params
            assert "base_url" not in params

    def test_default_conn_name(self):
        hook = SlackHook()
        assert hook.slack_conn_id == SlackHook.default_conn_name
