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

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.triggers.quicksight import QuickSightIngestionCompletedTrigger
from airflow.triggers.base import TriggerEvent

from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.quicksight."
EXPECTED_WAITER_NAME = "ingestion_complete"
DATA_SET_ID = "DemoDataSet"
INGESTION_ID = "DemoDataSet_Ingestion"
AWS_ACCOUNT_ID = "123456789012"


class TestQuickSightIngestionCompletedTrigger:
    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = QuickSightIngestionCompletedTrigger(
            data_set_id=DATA_SET_ID, ingestion_id=INGESTION_ID, aws_account_id=AWS_ACCOUNT_ID
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "QuickSightIngestionCompletedTrigger"
        assert kwargs.get("data_set_id") == DATA_SET_ID
        assert kwargs.get("ingestion_id") == INGESTION_ID
        assert kwargs.get("aws_account_id") == AWS_ACCOUNT_ID

    @pytest.mark.asyncio
    @mock.patch.object(QuickSightHook, "get_waiter")
    @mock.patch.object(QuickSightHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = QuickSightIngestionCompletedTrigger(
            data_set_id=DATA_SET_ID, ingestion_id=INGESTION_ID, aws_account_id=AWS_ACCOUNT_ID
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "ingestion_id": INGESTION_ID})
        assert_expected_waiter_type(mock_get_waiter, EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()
