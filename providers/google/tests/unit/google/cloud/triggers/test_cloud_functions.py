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

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.triggers.cloud_functions import CloudFunctionInvokeFunctionTrigger
from airflow.triggers.base import TriggerEvent


class TestCloudFunctionInvokeFunctionTrigger:
    @pytest.fixture
    def trigger(self):
        return CloudFunctionInvokeFunctionTrigger(
            function_id="test-function",
            input_data={"key": "value"},
            location="us-central1",
            project_id="test-project",
            gcp_conn_id="google_cloud_default",
            api_version="v1",
            impersonation_chain=None,
        )

    def test_serialize(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.cloud_functions.CloudFunctionInvokeFunctionTrigger"
        )
        assert kwargs == {
            "function_id": "test-function",
            "input_data": {"key": "value"},
            "location": "us-central1",
            "project_id": "test-project",
            "gcp_conn_id": "google_cloud_default",
            "api_version": "v1",
            "impersonation_chain": None,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_functions.CloudFunctionsHook")
    async def test_run_success(self, mock_hook_class, trigger):
        mock_hook = mock_hook_class.return_value
        mock_hook.call_function.return_value = {"executionId": "exec-123", "result": "ok"}

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "success"
        assert event.payload["result"] == {"executionId": "exec-123", "result": "ok"}
        assert event.payload["execution_id"] == "exec-123"

        mock_hook_class.assert_called_once_with(
            api_version="v1",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=None,
        )
        mock_hook.call_function.assert_called_once_with(
            function_id="test-function",
            input_data={"key": "value"},
            location="us-central1",
            project_id="test-project",
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_functions.CloudFunctionsHook")
    async def test_run_airflow_exception(self, mock_hook_class, trigger):
        mock_hook = mock_hook_class.return_value
        mock_hook.call_function.side_effect = AirflowException("Function error")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert event.payload["message"] == "Function error"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_functions.CloudFunctionsHook")
    async def test_run_generic_exception(self, mock_hook_class, trigger):
        mock_hook = mock_hook_class.return_value
        mock_hook.call_function.side_effect = RuntimeError("Unexpected error")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        event = events[0]
        assert event.payload["status"] == "error"
        assert event.payload["message"] == "Unexpected error"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_functions.CloudFunctionsHook")
    async def test_run_with_impersonation_chain(self, mock_hook_class):
        trigger = CloudFunctionInvokeFunctionTrigger(
            function_id="test-function",
            input_data={"key": "value"},
            location="us-central1",
            project_id="test-project",
            gcp_conn_id="google_cloud_default",
            api_version="v1",
            impersonation_chain=["account1", "account2"],
        )

        mock_hook = mock_hook_class.return_value
        mock_hook.call_function.return_value = {"executionId": "exec-123"}

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        mock_hook_class.assert_called_once_with(
            api_version="v1",
            gcp_conn_id="google_cloud_default",
            impersonation_chain=["account1", "account2"],
        )
