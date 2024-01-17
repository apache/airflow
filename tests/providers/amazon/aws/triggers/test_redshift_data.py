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

from airflow.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger
from airflow.triggers.base import TriggerEvent

TEST_CONN_ID = "aws_default"
TEST_TASK_ID = "123"
POLL_INTERVAL = 4.0


class TestRedshiftDataTrigger:
    def test_redshift_data_trigger_serialization(self):
        """
        Asserts that the RedshiftDataTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = RedshiftDataTrigger(
            statement_id=[],
            task_id=TEST_TASK_ID,
            aws_conn_id=TEST_CONN_ID,
            poll_interval=POLL_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.redshift_data.RedshiftDataTrigger"
        assert kwargs == {
            "statement_id": [],
            "task_id": TEST_TASK_ID,
            "poll_interval": POLL_INTERVAL,
            "aws_conn_id": TEST_CONN_ID,
            "region_name": None,
            "botocore_config": None,
            "verify": None,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "return_value, response",
        [
            (
                {"status": "error", "message": "test error", "statement_id": "uuid", "type": "failed"},
                TriggerEvent(
                    {"status": "error", "message": "test error", "statement_id": "uuid", "type": "failed"}
                ),
            ),
            (
                False,
                TriggerEvent(
                    {"status": "error", "message": f"{TEST_TASK_ID} failed", "statement_id": "uuid"}
                ),
            ),
        ],
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished_async"
    )
    async def test_redshift_data_trigger_run(self, mock_get_query_status, return_value, response):
        """
        Tests that RedshiftDataTrigger only fires once the query execution reaches a successful state.
        """
        mock_get_query_status.return_value = return_value
        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert response == actual

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.check_query_is_finished_async"
    )
    async def test_redshift_data_trigger_exception(self, mock_get_query_status):
        """
        Test that RedshiftDataTrigger fires the correct event in case of an error.
        """
        mock_get_query_status.side_effect = Exception("Test exception")

        trigger = RedshiftDataTrigger(
            statement_id="uuid",
            task_id=TEST_TASK_ID,
            poll_interval=POLL_INTERVAL,
            aws_conn_id=TEST_CONN_ID,
        )
        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception", "statement_id": "uuid"}) in task
