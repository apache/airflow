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
from typing import Any
from unittest import mock

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import ConnectionErrorRetryHandler, RateLimitErrorRetryHandler
from slack_sdk.web.slack_response import SlackResponse

from airflow.exceptions import AirflowNotFoundException, AirflowProviderDeprecationWarning
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


@pytest.fixture
def mocked_client():
    with mock.patch.object(SlackHook, "client") as m:
        yield m


class TestSlackHook:
    @staticmethod
    def fake_slack_response(*, data: dict | bytes, status_code: int = 200, **kwargs):
        """Helper for generate fake slack response."""
        # Mock other mandatory ``SlackResponse`` arguments
        for mandatory_param in ("client", "http_verb", "api_url", "req_args", "headers"):
            if mandatory_param not in kwargs:
                kwargs[mandatory_param] = mock.MagicMock(name=f"fake-{mandatory_param}")

        return SlackResponse(status_code=status_code, data=data, **kwargs)

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

    def test_call_with_failure(self, mocked_client):
        mocked_client.api_call.side_effect = SlackApiError(message="foo", response="bar")

        slack_hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        test_method = "test_method"
        test_api_params = {"key1": "value1", "key2": "value2"}

        with pytest.raises(SlackApiError):
            slack_hook.call(test_method, data=test_api_params)

    def test_api_call(self, mocked_client):
        mocked_client.api_call.return_value = {"ok": True}

        slack_hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        test_api_json = {"channel": "test_channel"}

        slack_hook.call("chat.postMessage", json=test_api_json)
        mocked_client.api_call.assert_called_with("chat.postMessage", json=test_api_json)

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
    def test_hook_connection_success(self, mocked_client, response_data):
        """Test SlackHook success connection."""
        mocked_client.api_call.return_value = self.fake_slack_response(status_code=200, data=response_data)
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        conn_test = hook.test_connection()
        mocked_client.api_call.assert_called_once_with("auth.test")
        assert conn_test[0]

    @pytest.mark.parametrize(
        "response_data",
        [
            {"ok": False, "error": "invalid_auth"},
            {"ok": False, "error": "not_authed"},
            b"some-binary-data",
        ],
    )
    def test_hook_connection_failed(self, mocked_client, response_data):
        """Test SlackHook failure connection."""
        mocked_client.api_call.return_value = self.fake_slack_response(status_code=401, data=response_data)
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        conn_test = hook.test_connection()
        mocked_client.api_call.assert_called_once_with("auth.test")
        assert not conn_test[0]

    @pytest.mark.parametrize(
        "file,content",
        [
            pytest.param(None, None, id="both-none"),
            pytest.param("", "", id="both-empty"),
            pytest.param("foo.bar", "test-content", id="both-specified"),
        ],
    )
    def test_send_file_wrong_parameters(self, file, content):
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        warning_message = "use `send_file_v2` or `send_file_v1_to_v2"
        error_message = r"Either `file` or `content` must be provided, not both\."
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            with pytest.raises(ValueError, match=error_message):
                hook.send_file(file=file, content=content)

    @pytest.mark.parametrize("initial_comment", [None, "test comment"])
    @pytest.mark.parametrize("title", [None, "test title"])
    @pytest.mark.parametrize("filetype", [None, "auto"])
    @pytest.mark.parametrize("channels", [None, "#random", "#random,#general", ("#random", "#general")])
    def test_send_file_path(
        self, mocked_client, tmp_path_factory, initial_comment, title, filetype, channels
    ):
        """Test send file by providing filepath."""
        tmp = tmp_path_factory.mktemp("test_send_file_path")
        file = tmp / "test.json"
        file.write_text('{"foo": "bar"}')

        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        warning_message = "use `send_file_v2` or `send_file_v1_to_v2"
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            hook.send_file(
                channels=channels,
                file=file,
                filename="filename.mock",
                initial_comment=initial_comment,
                title=title,
                filetype=filetype,
            )

        mocked_client.files_upload.assert_called_once_with(
            channels=channels,
            file=mock.ANY,  # Validate file properties later
            filename="filename.mock",
            initial_comment=initial_comment,
            title=title,
            filetype=filetype,
        )

        # Validate file properties
        mock_file = mocked_client.files_upload.call_args.kwargs["file"]
        assert mock_file.mode == "rb"
        assert mock_file.name == str(file)

    @pytest.mark.parametrize("filename", ["test.json", "1.parquet.snappy"])
    def test_send_file_path_set_filename(self, mocked_client, tmp_path_factory, filename):
        """Test set filename in send_file method if it not set."""
        tmp = tmp_path_factory.mktemp("test_send_file_path_set_filename")
        file = tmp / filename
        file.write_text('{"foo": "bar"}')

        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        warning_message = "use `send_file_v2` or `send_file_v1_to_v2"
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            hook.send_file(file=file)

        assert mocked_client.files_upload.call_count == 1
        call_args = mocked_client.files_upload.call_args.kwargs
        assert "filename" in call_args
        assert call_args["filename"] == filename

    @pytest.mark.parametrize("initial_comment", [None, "test comment"])
    @pytest.mark.parametrize("title", [None, "test title"])
    @pytest.mark.parametrize("filetype", [None, "auto"])
    @pytest.mark.parametrize("filename", [None, "foo.bar"])
    @pytest.mark.parametrize("channels", [None, "#random", "#random,#general", ("#random", "#general")])
    def test_send_file_content(self, mocked_client, initial_comment, title, filetype, channels, filename):
        """Test send file by providing content."""
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        warning_message = "use `send_file_v2` or `send_file_v1_to_v2"
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            hook.send_file(
                channels=channels,
                content='{"foo": "bar"}',
                filename=filename,
                initial_comment=initial_comment,
                title=title,
                filetype=filetype,
            )
        mocked_client.files_upload.assert_called_once_with(
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
    def test_backcompat_prefix_works(self, uri, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_MY_CONN", uri)
        hook = SlackHook(slack_conn_id="my_conn")
        params = hook._get_conn_params()
        assert params["token"] == "abc"
        assert params["timeout"] == 123
        assert params["base_url"] == "base_url"
        assert params["proxy"] == "proxy"

    def test_backcompat_prefix_both_causes_warning(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_MY_CONN", "a://:abc@?extra__slack__timeout=111&timeout=222")
        hook = SlackHook(slack_conn_id="my_conn")
        with pytest.warns(Warning, match="Using value for `timeout`"):
            params = hook._get_conn_params()
            assert params["timeout"] == 222

    def test_empty_string_ignored_prefixed(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_MY_CONN",
            json.dumps(
                {"password": "hi", "extra": {"extra__slack__base_url": "", "extra__slack__proxy": ""}}
            ),
        )
        hook = SlackHook(slack_conn_id="my_conn")
        params = hook._get_conn_params()
        assert "proxy" not in params
        assert "base_url" not in params

    def test_empty_string_ignored_non_prefixed(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_MY_CONN",
            json.dumps({"password": "hi", "extra": {"base_url": "", "proxy": ""}}),
        )
        hook = SlackHook(slack_conn_id="my_conn")
        params = hook._get_conn_params()
        assert "proxy" not in params
        assert "base_url" not in params

    def test_default_conn_name(self):
        hook = SlackHook()
        assert hook.slack_conn_id == SlackHook.default_conn_name

    def test_get_channel_id(self, mocked_client):
        fake_responses = [
            self.fake_slack_response(
                data={
                    "channels": [
                        {"id": "C0000000000", "name": "development-first-pr-support"},
                        {"id": "C0000000001", "name": "development"},
                    ],
                    "response_metadata": {"next_cursor": "FAKE"},
                }
            ),
            self.fake_slack_response(data={"response_metadata": {"next_cursor": "FAKE"}}),
            self.fake_slack_response(data={"channels": [{"id": "C0000000002", "name": "random"}]}),
            # Below should not reach, because previous one doesn't contain ``next_cursor``
            self.fake_slack_response(data={"channels": [{"id": "C0000000003", "name": "troubleshooting"}]}),
        ]
        mocked_client.conversations_list.side_effect = fake_responses

        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)

        assert hook.get_channel_id("development") == "C0000000001"
        mocked_client.conversations_list.assert_called()

        mocked_client.conversations_list.reset_mock()
        mocked_client.conversations_list.side_effect = fake_responses
        assert hook.get_channel_id("development-first-pr-support") == "C0000000000"
        # It should use cached values, so there is no new calls expected here
        mocked_client.assert_not_called()

        # Test pagination
        mocked_client.conversations_list.side_effect = fake_responses
        assert hook.get_channel_id("random") == "C0000000002"

        # Test non-existed channels
        mocked_client.conversations_list.side_effect = fake_responses
        with pytest.raises(LookupError, match="Unable to find slack channel"):
            hook.get_channel_id("troubleshooting")

    def test_send_file_v2(self, mocked_client):
        SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID).send_file_v2(
            channel_id="C00000000", file_uploads={"file": "/foo/bar/file.txt", "filename": "foo.txt"}
        )
        mocked_client.files_upload_v2.assert_called_once_with(
            channel="C00000000",
            file_uploads=[{"file": "/foo/bar/file.txt", "filename": "foo.txt"}],
            initial_comment=None,
        )

    def test_send_file_v2_multiple_files(self, mocked_client):
        SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID).send_file_v2(
            file_uploads=[
                {"file": "/foo/bar/file.txt"},
                {"content": "Some Text", "filename": "foo.txt"},
            ],
            initial_comment="Awesome File",
        )
        mocked_client.files_upload_v2.assert_called_once_with(
            channel=None,
            file_uploads=[
                {"file": "/foo/bar/file.txt", "filename": "Uploaded file"},
                {"content": "Some Text", "filename": "foo.txt"},
            ],
            initial_comment="Awesome File",
        )

    def test_send_file_v2_channel_name(self, mocked_client, caplog):
        with mock.patch.object(SlackHook, "get_channel_id", return_value="C00") as mocked_get_channel_id:
            warning_message = "consider replacing '#random' with the corresponding Channel ID 'C00'"
            with pytest.warns(UserWarning, match=warning_message):
                SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID).send_file_v2(
                    channel_id="#random", file_uploads={"file": "/foo/bar/file.txt"}
                )
            mocked_get_channel_id.assert_called_once_with("random")
            mocked_client.files_upload_v2.assert_called_once_with(
                channel="C00",
                file_uploads=mock.ANY,
                initial_comment=mock.ANY,
            )

    @pytest.mark.parametrize("initial_comment", [None, "test comment"])
    @pytest.mark.parametrize("title", [None, "test title"])
    @pytest.mark.parametrize("filename", [None, "foo.bar"])
    @pytest.mark.parametrize("channel", [None, "#random"])
    @pytest.mark.parametrize("filetype", [None, "auto"])
    def test_send_file_v1_to_v2_content(self, initial_comment, title, filename, channel, filetype):
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        with mock.patch.object(SlackHook, "send_file_v2") as mocked_send_file_v2:
            hook.send_file_v1_to_v2(
                channels=channel,
                content='{"foo": "bar"}',
                filename=filename,
                initial_comment=initial_comment,
                title=title,
                filetype=filetype,
            )
            mocked_send_file_v2.assert_called_once_with(
                channel_id=channel,
                file_uploads={
                    "content": '{"foo": "bar"}',
                    "filename": filename,
                    "title": title,
                    "snippet_type": filetype,
                },
                initial_comment=initial_comment,
            )

    @pytest.mark.parametrize("initial_comment", [None, "test comment"])
    @pytest.mark.parametrize("title", [None, "test title"])
    @pytest.mark.parametrize("filename", [None, "foo.bar"])
    @pytest.mark.parametrize("channel", [None, "#random"])
    @pytest.mark.parametrize("filetype", [None, "auto"])
    def test_send_file_v1_to_v2_file(self, initial_comment, title, filename, channel, filetype):
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        with mock.patch.object(SlackHook, "send_file_v2") as mocked_send_file_v2:
            hook.send_file_v1_to_v2(
                channels=channel,
                file="/foo/bar/spam.egg",
                filename=filename,
                initial_comment=initial_comment,
                title=title,
                filetype=filetype,
            )
            mocked_send_file_v2.assert_called_once_with(
                channel_id=channel,
                file_uploads={
                    "file": "/foo/bar/spam.egg",
                    "filename": filename or "spam.egg",
                    "title": title,
                    "snippet_type": filetype,
                },
                initial_comment=initial_comment,
            )

    @pytest.mark.parametrize(
        "file,content",
        [
            pytest.param(None, None, id="both-none"),
            pytest.param("", "", id="both-empty"),
            pytest.param("foo.bar", "test-content", id="both-specified"),
        ],
    )
    def test_send_file_v1_to_v2_wrong_parameters(self, file, content):
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        error_message = "Either `file` or `content` must be provided, not both"
        with pytest.raises(ValueError, match=error_message):
            hook.send_file_v1_to_v2(file=file, content=content)

    @pytest.mark.parametrize(
        "channels, expected_calls",
        [
            pytest.param("#foo, #bar", 2, id="comma-separated-string"),
            pytest.param(["#random", "#development", "#airflow-upgrades"], 3, id="list"),
        ],
    )
    def test_send_file_v1_to_v2_multiple_channels(self, channels, expected_calls):
        hook = SlackHook(slack_conn_id=SLACK_API_DEFAULT_CONN_ID)
        with mock.patch.object(SlackHook, "send_file_v2") as mocked_send_file_v2:
            hook.send_file_v1_to_v2(channels=channels, content="Fake")
            assert mocked_send_file_v2.call_count == expected_calls
