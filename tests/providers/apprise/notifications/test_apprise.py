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
from apprise import NotifyType

from airflow.operators.empty import EmptyOperator
from airflow.providers.apprise.notifications.apprise import (
    AppriseNotifier,
    send_apprise_notification,
)

pytestmark = pytest.mark.db_test


class TestAppriseNotifier:
    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    def test_notifier(self, mock_apprise_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        notifier = send_apprise_notification(body="DISK at 99%", notify_type=NotifyType.FAILURE)
        notifier({"dag": dag})
        mock_apprise_hook.return_value.notify.assert_called_once_with(
            body="DISK at 99%",
            notify_type=NotifyType.FAILURE,
            title=None,
            body_format=None,
            tag=None,
            attach=None,
            interpret_escapes=None,
            config=None,
        )

    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    def test_notifier_with_notifier_class(self, mock_apprise_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        notifier = AppriseNotifier(body="DISK at 99%", notify_type=NotifyType.FAILURE)
        notifier({"dag": dag})
        mock_apprise_hook.return_value.notify.assert_called_once_with(
            body="DISK at 99%",
            notify_type=NotifyType.FAILURE,
            title=None,
            body_format=None,
            tag=None,
            attach=None,
            interpret_escapes=None,
            config=None,
        )

    @mock.patch("airflow.providers.apprise.notifications.apprise.AppriseHook")
    def test_notifier_templated(self, mock_apprise_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = AppriseNotifier(
            notify_type=NotifyType.FAILURE,
            title="DISK at 99% {{dag.dag_id}}",
            body="System can crash soon {{dag.dag_id}}",
        )
        context = {"dag": dag}
        notifier(context)
        mock_apprise_hook.return_value.notify.assert_called_once_with(
            notify_type=NotifyType.FAILURE,
            title="DISK at 99% test_notifier",
            body="System can crash soon test_notifier",
            body_format=None,
            tag=None,
            attach=None,
            interpret_escapes=None,
            config=None,
        )
