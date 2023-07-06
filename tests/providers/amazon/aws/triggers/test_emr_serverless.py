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
from unittest.mock import AsyncMock
from airflow.providers.amazon.aws.triggers.emr import EmrServerlessAppicationTrigger
from airflow.triggers.base import TriggerEvent

import pytest
from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook

TEST_APPLICATION_ID = "test-application-id"
TEST_AWS_CONN_ID = "aws_emr_conn"
TEST_WAITER_MAX_ATTEMPTS = 10
TEST_WAITER_DELAY = 1
TEST_WAITER_NAME = "test-waiter-name"



class TestEmrServerlessApplicationTrigger:
    def test_emr_serverless_application_trigger_serialize(self):
        emr_serverless_create_application_trigger = EmrServerlessAppicationTrigger(
            application_id=TEST_APPLICATION_ID,
            waiter_name=TEST_WAITER_NAME,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        class_path, args = emr_serverless_create_application_trigger.serialize()

        assert class_path == "airflow.providers.amazon.aws.triggers.emr.EmrServerlessAppicationTrigger"
        assert args["application_id"] == TEST_APPLICATION_ID
        assert args["waiter_name"] == TEST_WAITER_NAME
        assert args["waiter_delay"] == str(TEST_WAITER_DELAY)
        assert args["waiter_max_attempts"] == str(TEST_WAITER_MAX_ATTEMPTS)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
    

    @pytest.mark.asyncio
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "async_conn")
    async def test_emr_serverless_application_trigger_run(self, mock_async_conn, mock_get_waiter):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        mock_get_waiter().wait = AsyncMock()

        emr_serverless_create_application_trigger = EmrServerlessAppicationTrigger(
            application_id=TEST_APPLICATION_ID,
            waiter_name=TEST_WAITER_NAME,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = emr_serverless_create_application_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "application_id": TEST_APPLICATION_ID}
        )
    
    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "async_conn")
    async def test_emr_serverless_application_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_get_waiter, mock_sleep
    ):
        """
        Test run method with multiple attempts to make sure the waiter retries
        are working as expected.
        """
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"application": {"state": "STARTING"}},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        emr_serverless_create_application_trigger = EmrServerlessAppicationTrigger(
            application_id=TEST_APPLICATION_ID,
            waiter_name=TEST_WAITER_NAME,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = emr_serverless_create_application_trigger.run()
        response = await generator.asend(None)

        assert mock_get_waiter().wait.call_count == 3
        assert response == TriggerEvent(
            {"status": "success", "application_id": TEST_APPLICATION_ID}
        )
    
    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "async_conn")
    async def test_emr_serverless_application_trigger_run_attempts_exceeded(self, mock_async_conn, mock_get_waiter, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"application": {"state": "STARTING"}},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error, error, error])
        mock_sleep.return_value = True

        emr_serverless_create_application_trigger = EmrServerlessAppicationTrigger(
            application_id=TEST_APPLICATION_ID,
            waiter_name=TEST_WAITER_NAME,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=2,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        with pytest.raises(AirflowException) as exc:
            generator = emr_serverless_create_application_trigger.run()
            await generator.asend(None)
        
        assert str(exc.value) == "Waiter error: max attempts reached"
        assert mock_get_waiter().wait.call_count == 2
    
    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EmrServerlessHook, "get_waiter")
    @mock.patch.object(EmrServerlessHook, "async_conn")
    async def test_emr_serverless_application_trigger_run_attempts_failed(self, mock_async_conn, mock_get_waiter, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_starting = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"application": {"state": "STARTING"}},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state",
            last_response={"application": {"state": "FAILED"}},
        )
        mock_get_waiter().wait.side_effect = AsyncMock(side_effect=[error_starting, error_starting, error_failed])
        mock_sleep.return_value = True

        emr_serverless_create_application_trigger = EmrServerlessAppicationTrigger(
            application_id=TEST_APPLICATION_ID,
            waiter_name=TEST_WAITER_NAME,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        with pytest.raises(AirflowException) as exc:
            generator = emr_serverless_create_application_trigger.run()
            await generator.asend(None)
        
        assert str(exc.value) == f"Error while waiting for application {TEST_APPLICATION_ID} to complete: Waiter test_name failed: Waiter encountered a terminal failure state"
    
