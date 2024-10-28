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

import pytest

from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.triggers.kinesis_analytics import (
    KinesisAnalyticsV2ApplicationOperationCompleteTrigger,
)
from airflow.triggers.base import TriggerEvent

from providers.tests.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.kinesis_analytics."


class TestKinesisAnalyticsV2ApplicationOperationCompleteTrigger:
    APPLICATION_NAME = "demo"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            waiter_name="application_start_complete",
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == BASE_TRIGGER_CLASSPATH
            + "KinesisAnalyticsV2ApplicationOperationCompleteTrigger"
        )
        assert kwargs.get("application_name") == self.APPLICATION_NAME

    @pytest.mark.asyncio
    @mock.patch.object(KinesisAnalyticsV2Hook, "get_waiter")
    @mock.patch.object(KinesisAnalyticsV2Hook, "async_conn")
    async def test_run_success_with_application_start_complete_waiter(
        self, mock_async_conn, mock_get_waiter
    ):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME,
            waiter_name="application_start_complete",
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "application_name": self.APPLICATION_NAME}
        )
        assert_expected_waiter_type(mock_get_waiter, "application_start_complete")
        mock_get_waiter().wait.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch.object(KinesisAnalyticsV2Hook, "get_waiter")
    @mock.patch.object(KinesisAnalyticsV2Hook, "async_conn")
    async def test_run_success_with_application_stop_complete_waiter(
        self, mock_async_conn, mock_get_waiter
    ):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = KinesisAnalyticsV2ApplicationOperationCompleteTrigger(
            application_name=self.APPLICATION_NAME, waiter_name="application_stop_waiter"
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "application_name": self.APPLICATION_NAME}
        )
        assert_expected_waiter_type(mock_get_waiter, "application_stop_waiter")
        mock_get_waiter().wait.assert_called_once()
