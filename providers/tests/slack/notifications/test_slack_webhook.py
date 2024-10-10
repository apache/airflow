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

from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.notifications.slack_webhook import (
    SlackWebhookNotifier,
    send_slack_webhook_notification,
)

pytestmark = pytest.mark.db_test

DEFAULT_HOOKS_PARAMETERS = {"timeout": None, "proxy": None, "retry_handlers": None}


class TestSlackNotifier:
    def test_class_and_notifier_are_same(self):
        assert send_slack_webhook_notification is SlackWebhookNotifier

    @mock.patch("airflow.providers.slack.notifications.slack_webhook.SlackWebhookHook")
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
    def test_slack_webhook_notifier(self, mock_slack_hook, slack_op_kwargs, hook_extra_kwargs):
        notifier = send_slack_webhook_notification(
            slack_webhook_conn_id="test_conn_id",
            text="foo-bar",
            blocks="spam-egg",
            attachments="baz-qux",
            unfurl_links=True,
            unfurl_media=False,
            **slack_op_kwargs,
        )
        notifier.notify({})
        mock_slack_hook.return_value.send.assert_called_once_with(
            text="foo-bar",
            blocks="spam-egg",
            unfurl_links=True,
            unfurl_media=False,
            attachments="baz-qux",
        )
        mock_slack_hook.assert_called_once_with(slack_webhook_conn_id="test_conn_id", **hook_extra_kwargs)

    @mock.patch("airflow.providers.slack.notifications.slack_webhook.SlackWebhookHook")
    def test_slack_webhook_templated(self, mock_slack_hook, dag_maker):
        with dag_maker("test_send_slack_webhook_notification_templated") as dag:
            EmptyOperator(task_id="task1")

        notifier = send_slack_webhook_notification(
            text="Who am I? {{ username }}",
            blocks=[{"type": "header", "text": {"type": "plain_text", "text": "{{ dag.dag_id }}"}}],
            attachments=[{"image_url": "{{ dag.dag_id }}.png"}],
        )
        notifier({"dag": dag, "username": "not-a-root"})
        mock_slack_hook.return_value.send.assert_called_once_with(
            text="Who am I? not-a-root",
            blocks=[
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "test_send_slack_webhook_notification_templated"},
                }
            ],
            attachments=[{"image_url": "test_send_slack_webhook_notification_templated.png"}],
            unfurl_links=None,
            unfurl_media=None,
        )
