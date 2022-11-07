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

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


class TestSlackWebhookOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "channel": None,
            "username": None,
            "icon_emoji": None,
            "icon_url": None,
        }

    @pytest.mark.parametrize(
        "simple_http_op_attr",
        [
            "endpoint",
            "method",
            "data",
            "headers",
            "response_check",
            "response_filter",
            "extra_options",
            "log_response",
            "auth_type",
            "tcp_keep_alive",
            "tcp_keep_alive_idle",
            "tcp_keep_alive_count",
            "tcp_keep_alive_interval",
        ],
    )
    def test_unused_deprecated_http_operator_kwargs(self, simple_http_op_attr):
        """
        Test remove deprecated (and unused) SimpleHttpOperator keyword arguments.
        No error should happen if provide any of attribute, unless operator allow to provide this attributes.
        """
        kw = {simple_http_op_attr: "foo-bar"}
        warning_message = rf"Provide '{simple_http_op_attr}' is deprecated and as has no affect"
        with pytest.warns(DeprecationWarning, match=warning_message):
            SlackWebhookOperator(task_id="test_unused_args", **kw)

    def test_deprecated_http_conn_id(self):
        """Test resolve deprecated http_conn_id."""
        warning_message = (
            r"Parameter `http_conn_id` is deprecated. Please use `slack_webhook_conn_id` instead."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            op = SlackWebhookOperator(
                task_id="test_deprecated_http_conn_id", slack_webhook_conn_id=None, http_conn_id="http_conn"
            )
        assert op.slack_webhook_conn_id == "http_conn"

        error_message = "You cannot provide both `slack_webhook_conn_id` and `http_conn_id`."
        with pytest.raises(AirflowException, match=error_message):
            with pytest.warns(DeprecationWarning, match=warning_message):
                SlackWebhookOperator(
                    task_id="test_both_conn_ids",
                    slack_webhook_conn_id="slack_webhook_conn_id",
                    http_conn_id="http_conn",
                )

    @pytest.mark.parametrize(
        "slack_webhook_conn_id,webhook_token",
        [
            ("test_conn_id", None),
            (None, "https://hooks.slack.com/services/T000/B000/XXX"),
            ("test_conn_id", "https://hooks.slack.com/services/T000/B000/XXX"),
        ],
    )
    @pytest.mark.parametrize("proxy", [None, "https://localhost:9999"])
    @mock.patch("airflow.providers.slack.operators.slack_webhook.SlackWebhookHook")
    def test_hook(self, mock_slackwebhook_cls, slack_webhook_conn_id, webhook_token, proxy):
        """Test get cached ``SlackWebhookHook`` hook."""
        op_kw = {
            "slack_webhook_conn_id": slack_webhook_conn_id,
            "proxy": proxy,
            "webhook_token": webhook_token,
        }
        op = SlackWebhookOperator(task_id="test_hook", **op_kw)
        hook = op.hook
        assert hook is op.hook, "Expected cached hook"
        mock_slackwebhook_cls.assert_called_once_with(**op_kw)

    def test_assert_templated_fields(self):
        """Test expected templated fields."""
        operator = SlackWebhookOperator(task_id="test_assert_templated_fields", **self.default_op_kwargs)
        template_fields = (
            "webhook_token",
            "message",
            "attachments",
            "blocks",
            "channel",
            "username",
            "proxy",
        )
        assert operator.template_fields == template_fields

    @pytest.mark.parametrize(
        "message,blocks,attachments",
        [
            ("Test Text", ["Dummy Block"], ["Test Attachments"]),
            ("Test Text", ["Dummy Block"], None),
            ("Test Text", None, None),
            (None, ["Dummy Block"], None),
            (None, ["Dummy Block"], ["Test Attachments"]),
            (None, None, ["Test Attachments"]),
        ],
    )
    @pytest.mark.parametrize(
        "channel,username,icon_emoji,icon_url",
        [
            (None, None, None, None),
            ("legacy-channel", "legacy-username", "legacy-icon_emoji", "legacy-icon-url"),
        ],
        ids=["webhook-attrs", "legacy-webhook-attrs"],
    )
    @mock.patch("airflow.providers.slack.operators.slack_webhook.SlackWebhookHook")
    def test_execute_operator(
        self, mock_slackwebhook_cls, message, blocks, attachments, channel, username, icon_emoji, icon_url
    ):
        mock_slackwebhook = mock_slackwebhook_cls.return_value
        mock_slackwebhook_send = mock_slackwebhook.send
        op = SlackWebhookOperator(
            task_id="test_execute",
            slack_webhook_conn_id="test_conn_id",
            message=message,
            blocks=blocks,
            attachments=attachments,
            channel=channel,
            username=username,
            icon_emoji=icon_emoji,
            icon_url=icon_url,
        )
        op.execute(mock.MagicMock())
        mock_slackwebhook_send.assert_called_once_with(
            text=message,
            blocks=blocks,
            attachments=attachments,
            channel=channel,
            username=username,
            icon_emoji=icon_emoji,
            icon_url=icon_url,
            link_names=mock.ANY,
        )
