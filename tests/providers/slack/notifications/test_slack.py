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
from airflow.providers.slack.notifications.slack import SlackNotifier, send_slack_notification

pytestmark = pytest.mark.db_test

DEFAULT_HOOKS_PARAMETERS = {"base_url": None, "timeout": None, "proxy": None, "retry_handlers": None}


class TestSlackNotifier:
    @mock.patch("airflow.providers.slack.notifications.slack.SlackHook")
    @pytest.mark.parametrize(
        "extra_kwargs, hook_extra_kwargs",
        [
            pytest.param({}, DEFAULT_HOOKS_PARAMETERS, id="default-hook-parameters"),
            pytest.param(
                {
                    "base_url": "https://foo.bar",
                    "timeout": 42,
                    "proxy": "http://spam.egg",
                    "retry_handlers": [],
                },
                {
                    "base_url": "https://foo.bar",
                    "timeout": 42,
                    "proxy": "http://spam.egg",
                    "retry_handlers": [],
                },
                id="with-extra-hook-parameters",
            ),
        ],
    )
    def test_slack_notifier(self, mock_slack_hook, dag_maker, extra_kwargs, hook_extra_kwargs):
        with dag_maker("test_slack_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = send_slack_notification(slack_conn_id="test_conn_id", text="test", **extra_kwargs)
        notifier({"dag": dag})
        mock_slack_hook.return_value.call.assert_called_once_with(
            "chat.postMessage",
            json={
                "channel": "#general",
                "username": "Airflow",
                "text": "test",
                "icon_url": "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static"
                "/pin_100.png",
                "attachments": "[]",
                "blocks": "[]",
            },
        )
        mock_slack_hook.assert_called_once_with(slack_conn_id="test_conn_id", **hook_extra_kwargs)

    @mock.patch("airflow.providers.slack.notifications.slack.SlackHook")
    def test_slack_notifier_with_notifier_class(self, mock_slack_hook, dag_maker):
        with dag_maker("test_slack_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = SlackNotifier(text="test")
        notifier({"dag": dag})
        mock_slack_hook.return_value.call.assert_called_once_with(
            "chat.postMessage",
            json={
                "channel": "#general",
                "username": "Airflow",
                "text": "test",
                "icon_url": "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static"
                "/pin_100.png",
                "attachments": "[]",
                "blocks": "[]",
            },
        )

    @mock.patch("airflow.providers.slack.notifications.slack.SlackHook")
    def test_slack_notifier_templated(self, mock_slack_hook, dag_maker):
        with dag_maker("test_slack_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = send_slack_notification(
            text="test {{ username }}",
            channel="#test-{{dag.dag_id}}",
            attachments=[{"image_url": "{{ dag.dag_id }}.png"}],
        )
        context = {"dag": dag}
        notifier(context)
        mock_slack_hook.return_value.call.assert_called_once_with(
            "chat.postMessage",
            json={
                "channel": "#test-test_slack_notifier",
                "username": "Airflow",
                "text": "test Airflow",
                "icon_url": "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static"
                "/pin_100.png",
                "attachments": '[{"image_url": "test_slack_notifier.png"}]',
                "blocks": "[]",
            },
        )
