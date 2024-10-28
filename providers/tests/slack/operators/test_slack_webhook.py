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

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

DEFAULT_HOOKS_PARAMETERS = {"timeout": None, "proxy": None, "retry_handlers": None}


class TestSlackWebhookOperator:
    def setup_method(self):
        self.default_op_kwargs = {
            "slack_webhook_conn_id": "test_conn_id",
            "channel": None,
            "username": None,
            "icon_emoji": None,
            "icon_url": None,
        }

    @mock.patch("airflow.providers.slack.operators.slack_webhook.SlackWebhookHook")
    @pytest.mark.parametrize(
        "slack_op_kwargs, hook_extra_kwargs",
        [
            pytest.param({}, DEFAULT_HOOKS_PARAMETERS, id="default-hook-parameters"),
            pytest.param(
                {"timeout": 42, "proxy": "http://spam.egg", "retry_handlers": []},
                {"timeout": 42, "proxy": "http://spam.egg", "retry_handlers": []},
                id="with-extra-hook-parameters",
            ),
        ],
    )
    def test_hook(self, mock_slackwebhook_cls, slack_op_kwargs, hook_extra_kwargs):
        """Test get cached ``SlackWebhookHook`` hook."""
        op = SlackWebhookOperator(
            task_id="test_hook", slack_webhook_conn_id="test_conn_id", **slack_op_kwargs
        )
        hook = op.hook
        assert hook is op.hook, "Expected cached hook"
        mock_slackwebhook_cls.assert_called_once_with(
            slack_webhook_conn_id="test_conn_id", **hook_extra_kwargs
        )

    def test_assert_templated_fields(self):
        """Test expected templated fields."""
        operator = SlackWebhookOperator(
            task_id="test_assert_templated_fields", **self.default_op_kwargs
        )
        template_fields = (
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
        self,
        mock_slackwebhook_cls,
        message,
        blocks,
        attachments,
        channel,
        username,
        icon_emoji,
        icon_url,
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
        )
