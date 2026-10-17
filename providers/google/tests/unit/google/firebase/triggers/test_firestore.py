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

from airflow.providers.google.firebase.triggers.firestore import CloudFirestoreExportDatabaseTrigger
from airflow.triggers.base import TriggerEvent

TEST_OPERATION_NAME = "projects/test-project/databases/(default)/operations/op-123"
TEST_CONN_ID = "google_cloud_default"
TEST_API_VERSION = "v1"
TEST_IMPERSONATION_CHAIN = "sa@test-project.iam.gserviceaccount.com"
TEST_POLL_INTERVAL = 5.0


@pytest.fixture
def trigger():
    return CloudFirestoreExportDatabaseTrigger(
        operation_name=TEST_OPERATION_NAME,
        gcp_conn_id=TEST_CONN_ID,
        api_version=TEST_API_VERSION,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
        poll_interval=TEST_POLL_INTERVAL,
    )


class TestCloudFirestoreExportDatabaseTrigger:
    def test_serialize(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.firebase.triggers.firestore.CloudFirestoreExportDatabaseTrigger"
        )
        assert kwargs == {
            "operation_name": TEST_OPERATION_NAME,
            "gcp_conn_id": TEST_CONN_ID,
            "api_version": TEST_API_VERSION,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "poll_interval": TEST_POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.firebase.triggers.firestore.asyncio.sleep")
    @mock.patch("airflow.providers.google.firebase.triggers.firestore.CloudFirestoreAsyncHook.get_operation")
    async def test_run_success(self, mock_get_operation, mock_sleep, trigger):
        mock_get_operation.side_effect = [
            {"name": TEST_OPERATION_NAME, "done": False},
            {
                "name": TEST_OPERATION_NAME,
                "done": True,
                "response": {"outputUriPrefix": "gs://bucket/export"},
            },
        ]

        events = [event async for event in trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "operation_name": TEST_OPERATION_NAME,
                    "status": "success",
                    "response": {"outputUriPrefix": "gs://bucket/export"},
                }
            )
        ]
        mock_sleep.assert_awaited_once_with(TEST_POLL_INTERVAL)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.firebase.triggers.firestore.CloudFirestoreAsyncHook.get_operation")
    async def test_run_operation_error(self, mock_get_operation, trigger):
        mock_get_operation.return_value = {
            "name": TEST_OPERATION_NAME,
            "done": True,
            "error": {"code": 13, "message": "Internal error"},
        }

        events = [event async for event in trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "operation_name": TEST_OPERATION_NAME,
                    "status": "error",
                    "message": "{'code': 13, 'message': 'Internal error'}",
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.firebase.triggers.firestore.CloudFirestoreAsyncHook.get_operation")
    async def test_run_exception(self, mock_get_operation, trigger):
        mock_get_operation.side_effect = RuntimeError("Connection error")

        events = [event async for event in trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "operation_name": TEST_OPERATION_NAME,
                    "status": "error",
                    "message": "Connection error",
                }
            )
        ]
