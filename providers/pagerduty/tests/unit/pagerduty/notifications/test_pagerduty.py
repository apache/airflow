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

from airflow import DAG
from airflow.providers.pagerduty.hooks.pagerduty_events import PagerdutyEventsHook
from airflow.providers.pagerduty.notifications.pagerduty import (
    PagerdutyNotifier,
    send_pagerduty_notification,
)

PAGERDUTY_API_DEFAULT_CONN_ID = PagerdutyEventsHook.default_conn_name


class TestPagerdutyNotifier:
    @mock.patch("airflow.providers.pagerduty.notifications.pagerduty.PagerdutyEventsHook")
    def test_notifier(self, mock_pagerduty_event_hook):
        dag = DAG("test_notifier")
        notifier = send_pagerduty_notification(summary="DISK at 99%", severity="critical", action="trigger")
        notifier({"dag": dag})
        mock_pagerduty_event_hook.return_value.send_event.assert_called_once_with(
            summary="DISK at 99%",
            severity="critical",
            action="trigger",
            source="airflow",
            class_type=None,
            component=None,
            custom_details=None,
            group=None,
            images=None,
            links=None,
            dedup_key=None,
        )

    @mock.patch("airflow.providers.pagerduty.notifications.pagerduty.PagerdutyEventsHook")
    def test_notifier_with_notifier_class(self, mock_pagerduty_event_hook):
        dag = DAG("test_notifier")
        notifier = PagerdutyNotifier(summary="DISK at 99%", severity="critical", action="trigger")
        notifier({"dag": dag})
        mock_pagerduty_event_hook.return_value.send_event.assert_called_once_with(
            summary="DISK at 99%",
            severity="critical",
            action="trigger",
            source="airflow",
            class_type=None,
            component=None,
            custom_details=None,
            group=None,
            images=None,
            links=None,
            dedup_key=None,
        )

    @mock.patch("airflow.providers.pagerduty.notifications.pagerduty.PagerdutyEventsHook")
    def test_notifier_templated(self, mock_pagerduty_event_hook):
        dag = DAG("test_notifier")

        notifier = PagerdutyNotifier(
            summary="DISK at 99% {{dag.dag_id}}",
            severity="critical {{dag.dag_id}}",
            source="database {{dag.dag_id}}",
            dedup_key="srv0555-{{dag.dag_id}}",
            custom_details={
                "free space": "1%",
                "ping time": "1500ms",
                "load avg": 0.75,
                "template": "{{dag.dag_id}}",
            },
            group="prod-datapipe {{dag.dag_id}}",
            component="database {{dag.dag_id}}",
            class_type="disk {{dag.dag_id}}",
        )
        context = {"dag": dag}
        notifier(context)
        mock_pagerduty_event_hook.return_value.send_event.assert_called_once_with(
            action="trigger",
            summary="DISK at 99% test_notifier",
            severity="critical test_notifier",
            source="database test_notifier",
            dedup_key="srv0555-test_notifier",
            custom_details={
                "free space": "1%",
                "ping time": "1500ms",
                "load avg": 0.75,
                "template": "test_notifier",
            },
            group="prod-datapipe test_notifier",
            component="database test_notifier",
            class_type="disk test_notifier",
            images=None,
            links=None,
        )

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.pagerduty.notifications.pagerduty.PagerdutyEventsAsyncHook.send_event",
        new_callable=mock.AsyncMock,
    )
    async def test_async_notifier(self, mock_async_hook, create_dag_without_db):
        notifier = send_pagerduty_notification(summary="DISK at 99%", severity="critical", action="trigger")
        await notifier.async_notify({"dag": create_dag_without_db("test_pagerduty_notifier")})
        mock_async_hook.assert_called_once_with(
            summary="DISK at 99%",
            severity="critical",
            action="trigger",
            source="airflow",
            class_type=None,
            component=None,
            custom_details=None,
            group=None,
            images=None,
            links=None,
            dedup_key=None,
        )
