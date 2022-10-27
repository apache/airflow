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
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.models import Connection
from airflow.providers.slack.operators.slack import (
    SlackAPIFileOperator,
    SlackAPIOperator,
    SlackAPIPostOperator,
)

SLACK_API_TEST_CONNECTION_ID = "test_slack_conn_id"


@pytest.fixture(scope="module", autouse=True)
def slack_api_connections():
    """Create tests connections."""
    connections = [
        Connection(
            conn_id=SLACK_API_TEST_CONNECTION_ID,
            conn_type="slack",
            password="xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx",
        ),
    ]
    conn_uris = {f"AIRFLOW_CONN_{c.conn_id.upper()}": c.get_uri() for c in connections}

    with mock.patch.dict("os.environ", values=conn_uris):
        yield


class TestSlackAPIOperator:
    @mock.patch("airflow.providers.slack.operators.slack.mask_secret")
    def test_mask_token(self, mock_mask_secret):
        SlackAPIOperator(task_id="test-mask-token", token="super-secret-token")
        mock_mask_secret.assert_called_once_with("super-secret-token")

    @mock.patch("airflow.providers.slack.operators.slack.SlackHook")
    @pytest.mark.parametrize(
        "token,conn_id",
        [
            ("token", SLACK_API_TEST_CONNECTION_ID),
            ("token", None),
            (None, SLACK_API_TEST_CONNECTION_ID),
        ],
    )
    def test_hook(self, mock_slack_hook_cls, token, conn_id):
        mock_slack_hook = mock_slack_hook_cls.return_value
        op = SlackAPIOperator(task_id="test-mask-token", token=token, slack_conn_id=conn_id)
        hook = op.hook
        assert hook == mock_slack_hook
        assert hook is op.hook
        mock_slack_hook_cls.assert_called_once_with(token=token, slack_conn_id=conn_id)


class TestSlackAPIPostOperator:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.test_username = "test_username"
        self.test_channel = "#test_slack_channel"
        self.test_text = "test_text"
        self.test_icon_url = "test_icon_url"
        self.test_attachments = [
            {
                "fallback": "Required plain-text summary of the attachment.",
                "color": "#36a64f",
                "pretext": "Optional text that appears above the attachment block",
                "author_name": "Bobby Tables",
                "author_link": "http://flickr.com/bobby/",
                "author_icon": "http://flickr.com/icons/bobby.jpg",
                "title": "Slack API Documentation",
                "title_link": "https://api.slack.com/",
                "text": "Optional text that appears within the attachment",
                "fields": [{"title": "Priority", "value": "High", "short": "false"}],
                "image_url": "http://my-website.com/path/to/image.jpg",
                "thumb_url": "http://example.com/path/to/thumb.png",
                "footer": "Slack API",
                "footer_icon": "https://platform.slack-edge.com/img/default_application_icon.png",
                "ts": 123456789,
            }
        ]
        self.test_blocks = [
            {
                "type": "section",
                "text": {
                    "text": "A message *with some bold text* and _some italicized text_.",
                    "type": "mrkdwn",
                },
                "fields": [
                    {"type": "mrkdwn", "text": "High"},
                    {"type": "plain_text", "emoji": True, "text": "String"},
                ],
            }
        ]
        self.test_attachments_in_json = json.dumps(self.test_attachments)
        self.test_blocks_in_json = json.dumps(self.test_blocks)
        self.test_api_params = {"key": "value"}

        self.expected_method = "chat.postMessage"
        self.expected_api_params = {
            "channel": self.test_channel,
            "username": self.test_username,
            "text": self.test_text,
            "icon_url": self.test_icon_url,
            "attachments": self.test_attachments_in_json,
            "blocks": self.test_blocks_in_json,
        }

    def __construct_operator(self, test_token, test_slack_conn_id, test_api_params=None):
        return SlackAPIPostOperator(
            task_id="slack",
            username=self.test_username,
            token=test_token,
            slack_conn_id=test_slack_conn_id,
            channel=self.test_channel,
            text=self.test_text,
            icon_url=self.test_icon_url,
            attachments=self.test_attachments,
            blocks=self.test_blocks,
            api_params=test_api_params,
        )

    def test_init_with_valid_params(self):
        test_token = "test_token"

        slack_api_post_operator = self.__construct_operator(test_token, None, self.test_api_params)
        assert slack_api_post_operator.token == test_token
        assert slack_api_post_operator.slack_conn_id is None
        assert slack_api_post_operator.method == self.expected_method
        assert slack_api_post_operator.text == self.test_text
        assert slack_api_post_operator.channel == self.test_channel
        assert slack_api_post_operator.api_params == self.test_api_params
        assert slack_api_post_operator.username == self.test_username
        assert slack_api_post_operator.icon_url == self.test_icon_url
        assert slack_api_post_operator.attachments == self.test_attachments
        assert slack_api_post_operator.blocks == self.test_blocks

        slack_api_post_operator = self.__construct_operator(None, SLACK_API_TEST_CONNECTION_ID)
        assert slack_api_post_operator.token is None
        assert slack_api_post_operator.slack_conn_id == SLACK_API_TEST_CONNECTION_ID

    @mock.patch("airflow.providers.slack.operators.slack.SlackHook")
    def test_api_call_params_with_default_args(self, mock_hook):
        slack_api_post_operator = SlackAPIPostOperator(
            task_id="slack",
            username=self.test_username,
            slack_conn_id=SLACK_API_TEST_CONNECTION_ID,
        )

        slack_api_post_operator.execute(context=MagicMock())

        expected_api_params = {
            "channel": "#general",
            "username": self.test_username,
            "text": "No message has been set.\n"
            "Here is a cat video instead\n"
            "https://www.youtube.com/watch?v=J---aiyznGQ",
            "icon_url": "https://raw.githubusercontent.com/apache/"
            "airflow/main/airflow/www/static/pin_100.png",
            "attachments": "[]",
            "blocks": "[]",
        }
        assert expected_api_params == slack_api_post_operator.api_params


class TestSlackAPIFileOperator:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.test_username = "test_username"
        self.test_channel = "#test_slack_channel"
        self.test_initial_comment = "test text file test_filename.txt"
        self.filename = "test_filename.txt"
        self.test_filetype = "text"
        self.test_content = "This is a test text file!"
        self.test_api_params = {"key": "value"}
        self.expected_method = "files.upload"

    def __construct_operator(self, test_token, test_slack_conn_id, test_api_params=None):
        return SlackAPIFileOperator(
            task_id="slack",
            token=test_token,
            slack_conn_id=test_slack_conn_id,
            channels=self.test_channel,
            initial_comment=self.test_initial_comment,
            filename=self.filename,
            filetype=self.test_filetype,
            content=self.test_content,
            api_params=test_api_params,
        )

    def test_init_with_valid_params(self):
        test_token = "test_token"

        slack_api_post_operator = self.__construct_operator(test_token, None, self.test_api_params)
        assert slack_api_post_operator.token == test_token
        assert slack_api_post_operator.slack_conn_id is None
        assert slack_api_post_operator.method == self.expected_method
        assert slack_api_post_operator.initial_comment == self.test_initial_comment
        assert slack_api_post_operator.channels == self.test_channel
        assert slack_api_post_operator.api_params == self.test_api_params
        assert slack_api_post_operator.filename == self.filename
        assert slack_api_post_operator.filetype == self.test_filetype
        assert slack_api_post_operator.content == self.test_content

        slack_api_post_operator = self.__construct_operator(None, SLACK_API_TEST_CONNECTION_ID)
        assert slack_api_post_operator.token is None
        assert slack_api_post_operator.slack_conn_id == SLACK_API_TEST_CONNECTION_ID

    @mock.patch("airflow.providers.slack.operators.slack.SlackHook.send_file")
    @pytest.mark.parametrize("initial_comment", [None, "foo-bar"])
    @pytest.mark.parametrize("title", [None, "Spam Egg"])
    def test_api_call_params_with_content_args(self, mock_send_file, initial_comment, title):
        SlackAPIFileOperator(
            task_id="slack",
            slack_conn_id=SLACK_API_TEST_CONNECTION_ID,
            content="test-content",
            channels="#test-channel",
            initial_comment=initial_comment,
            title=title,
        ).execute(context=MagicMock())

        mock_send_file.assert_called_once_with(
            channels="#test-channel",
            content="test-content",
            file=None,
            initial_comment=initial_comment,
            title=title,
        )

    @mock.patch("airflow.providers.slack.operators.slack.SlackHook.send_file")
    @pytest.mark.parametrize("initial_comment", [None, "foo-bar"])
    @pytest.mark.parametrize("title", [None, "Spam Egg"])
    def test_api_call_params_with_file_args(self, mock_send_file, initial_comment, title):
        SlackAPIFileOperator(
            task_id="slack",
            slack_conn_id=SLACK_API_TEST_CONNECTION_ID,
            channels="C1234567890",
            filename="/dev/null",
            initial_comment=initial_comment,
            title=title,
        ).execute(context=MagicMock())

        mock_send_file.assert_called_once_with(
            channels="C1234567890",
            content=None,
            file="/dev/null",
            initial_comment=initial_comment,
            title=title,
        )

    def test_channel_deprecated(self):
        warning_message = (
            r"Argument `channel` is deprecated and will removed in a future releases\. "
            r"Please use `channels` instead\."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            op = SlackAPIFileOperator(
                task_id="slack",
                slack_conn_id=SLACK_API_TEST_CONNECTION_ID,
                channel="#random",
                channels=None,
            )
        assert op.channels == "#random"

    def test_both_channel_and_channels_set(self):
        error_message = r"Cannot set both arguments: channel=.* and channels=.*\."
        with pytest.raises(ValueError, match=error_message):
            SlackAPIFileOperator(
                task_id="slack",
                slack_conn_id=SLACK_API_TEST_CONNECTION_ID,
                channel="#random",
                channels="#general",
            )
