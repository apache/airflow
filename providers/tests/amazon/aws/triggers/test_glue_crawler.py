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

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.triggers.glue_crawler import GlueCrawlerCompleteTrigger
from airflow.triggers.base import TriggerEvent

from providers.tests.amazon.aws.utils.test_waiter import assert_expected_waiter_type


class TestGlueCrawlerCompleteTrigger:
    def test_serialization(self):
        crawler_name = "test_crawler"
        poll_interval = 10
        aws_conn_id = "aws_default"

        trigger = GlueCrawlerCompleteTrigger(
            crawler_name=crawler_name,
            waiter_delay=poll_interval,
            aws_conn_id=aws_conn_id,
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.glue_crawler.GlueCrawlerCompleteTrigger"
        assert kwargs == {
            "crawler_name": "test_crawler",
            "waiter_delay": 10,
            "waiter_max_attempts": 1500,
            "aws_conn_id": "aws_default",
        }

    @pytest.mark.asyncio
    @mock.patch.object(GlueCrawlerHook, "get_waiter")
    @mock.patch.object(GlueCrawlerHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        crawler_name = "test_crawler"
        trigger = GlueCrawlerCompleteTrigger(crawler_name=crawler_name)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "value": None})
        assert_expected_waiter_type(mock_get_waiter, "crawler_ready")
        mock_get_waiter().wait.assert_called_once()
