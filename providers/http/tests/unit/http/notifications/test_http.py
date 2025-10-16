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

from airflow.providers.http.notifications.http import HttpNotifier, send_http_notification


class TestHttpNotifier:
    def test_class_and_notifier_are_same(self):
        assert send_http_notification is HttpNotifier

    @mock.patch("airflow.providers.http.notifications.http.HttpHook")
    def test_http_notifier(self, mock_http_hook):
        notifier = HttpNotifier(
            http_conn_id="test_conn_id",
            endpoint="/testing",
            method="POST",
            json={"message": "testing"},
            headers={"Content-Type": "application/json"},
        )
        notifier.notify({})

        mock_http_hook.return_value.run.assert_called_once_with(
            endpoint="/testing",
            data=None,
            headers={"Content-Type": "application/json"},
            extra_options={},
            json={"message": "testing"},
        )
        mock_http_hook.assert_called_once_with(method="POST", http_conn_id="test_conn_id")

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.http.notifications.http.HttpAsyncHook")
    @mock.patch("aiohttp.ClientSession")
    async def test_async_http_notifier(self, mock_session, mock_http_async_hook):
        mock_hook = mock_http_async_hook.return_value
        mock_hook.run = mock.AsyncMock()

        notifier = HttpNotifier(
            http_conn_id="test_conn_id",
            endpoint="/test",
            method="POST",
            json={"message": "test"},
        )

        await notifier.async_notify({})

        mock_hook.run.assert_called_once_with(
            session=mock_session.return_value.__aenter__.return_value,
            endpoint="/test",
            data=None,
            json={"message": "test"},
            headers=None,
            extra_options={},
        )

    @mock.patch("airflow.providers.http.notifications.http.HttpHook")
    def test_http_notifier_templated(self, mock_http_hook, create_dag_without_db):
        notifier = HttpNotifier(
            endpoint="/{{ dag.dag_id }}",
            json={"dag_id": "{{ dag.dag_id }}", "user": "{{ username }}"},
        )
        notifier(
            {
                "dag": create_dag_without_db("test_http_notification_templated"),
                "username": "test-user",
            }
        )

        mock_http_hook.return_value.run.assert_called_once_with(
            endpoint="/test_http_notification_templated",
            data=None,
            headers=None,
            extra_options={},
            json={"dag_id": "test_http_notification_templated", "user": "test-user"},
        )
