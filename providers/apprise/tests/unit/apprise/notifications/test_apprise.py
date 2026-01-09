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
from apprise import NotifyFormat, NotifyType

from airflow.models import Connection
from airflow.providers.apprise.notifications.apprise import (
    AppriseNotifier,
    send_apprise_notification,
)


class TestAppriseNotifier:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        extra = {"config": {"path": "http://some_path_that_dont_exist/", "tag": "alert"}}
        create_connection_without_db(
            Connection(
                conn_id="apprise_default",
                conn_type="apprise",
                extra=extra,
            )
        )

    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    def test_notifier(self, mock_apprise_hook, create_dag_without_db):
        notifier = send_apprise_notification(body="DISK at 99%", notify_type=NotifyType.FAILURE)
        notifier({"dag": create_dag_without_db("test_notifier")})
        call_args = mock_apprise_hook.return_value.notify.call_args.kwargs

        assert call_args == {
            "body": "DISK at 99%",
            "notify_type": NotifyType.FAILURE,
            "title": None,
            "body_format": NotifyFormat.TEXT,
            "tag": "all",
            "attach": None,
            "interpret_escapes": None,
            "config": None,
        }
        mock_apprise_hook.return_value.notify.assert_called_once()

    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    def test_notifier_with_notifier_class(self, mock_apprise_hook, create_dag_without_db):
        notifier = AppriseNotifier(body="DISK at 99%", notify_type=NotifyType.FAILURE)
        notifier({"dag": create_dag_without_db("test_notifier")})
        call_args = mock_apprise_hook.return_value.notify.call_args.kwargs

        assert call_args == {
            "body": "DISK at 99%",
            "notify_type": NotifyType.FAILURE,
            "title": None,
            "body_format": NotifyFormat.TEXT,
            "tag": "all",
            "attach": None,
            "interpret_escapes": None,
            "config": None,
        }
        mock_apprise_hook.return_value.notify.assert_called_once()

    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    def test_notifier_templated(self, mock_apprise_hook, create_dag_without_db):
        notifier = AppriseNotifier(
            notify_type=NotifyType.FAILURE,
            title="DISK at 99% {{dag.dag_id}}",
            body="System can crash soon {{dag.dag_id}}",
        )
        context = {"dag": create_dag_without_db("test_notifier")}
        notifier(context)
        call_args = mock_apprise_hook.return_value.notify.call_args.kwargs
        assert call_args == {
            "body": "System can crash soon test_notifier",
            "title": "DISK at 99% test_notifier",
            "notify_type": NotifyType.FAILURE,
            "body_format": NotifyFormat.TEXT,
            "tag": "all",
            "attach": None,
            "interpret_escapes": None,
            "config": None,
        }
        mock_apprise_hook.return_value.notify.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    async def test_async_apprise_notifier(self, mock_apprise_hook, create_dag_without_db):
        mock_apprise_hook.return_value.async_notify = mock.AsyncMock()

        notifier = send_apprise_notification(body="DISK at 99%", notify_type=NotifyType.FAILURE)

        await notifier.async_notify({"dag": create_dag_without_db("test_notifier")})

        call_args = mock_apprise_hook.return_value.async_notify.call_args.kwargs

        assert call_args == {
            "body": "DISK at 99%",
            "notify_type": NotifyType.FAILURE,
            "title": None,
            "body_format": NotifyFormat.TEXT,
            "tag": "all",
            "attach": None,
            "interpret_escapes": None,
            "config": None,
        }
        mock_apprise_hook.return_value.async_notify.assert_called_once()
