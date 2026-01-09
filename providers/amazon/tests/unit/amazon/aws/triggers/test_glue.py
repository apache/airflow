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

from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.triggers.glue import (
    GlueCatalogPartitionTrigger,
    GlueDataQualityRuleRecommendationRunCompleteTrigger,
    GlueDataQualityRuleSetEvaluationRunCompleteTrigger,
    GlueJobCompleteTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import TriggerEvent

from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.glue."


class TestGlueJobTrigger:
    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "get_waiter")
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_wait_job(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="JobRunId",
            verbose=False,
            aws_conn_id="aws_conn_id",
            waiter_max_attempts=3,
            waiter_delay=10,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert_expected_waiter_type(mock_get_waiter, "job_complete")
        mock_get_waiter().wait.assert_called_once()
        assert event.payload["status"] == "success"
        assert event.payload["run_id"] == "JobRunId"

    @pytest.mark.asyncio
    @mock.patch.object(GlueJobHook, "get_waiter")
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_wait_job_failed(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        from botocore.exceptions import WaiterError

        mock_get_waiter().wait = AsyncMock(
            side_effect=WaiterError(
                name="job_complete",
                reason="Waiter encountered a terminal failure state",
                last_response={"JobRun": {"JobRunState": "FAILED"}},
            )
        )

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="JobRunId",
            verbose=False,
            aws_conn_id="aws_conn_id",
            waiter_max_attempts=3,
            waiter_delay=10,
        )
        generator = trigger.run()

        with pytest.raises(AirflowException):
            await generator.asend(None)
        assert_expected_waiter_type(mock_get_waiter, "job_complete")

    def test_serialization(self):
        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="JobRunId",
            verbose=False,
            aws_conn_id="aws_conn_id",
            waiter_max_attempts=3,
            waiter_delay=10,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.glue.GlueJobCompleteTrigger"
        assert kwargs == {
            "job_name": "job_name",
            "run_id": "JobRunId",
            "verbose": False,
            "aws_conn_id": "aws_conn_id",
            "waiter_max_attempts": 3,
            "waiter_delay": 10,
        }


class TestGlueCatalogPartitionSensorTrigger:
    @pytest.mark.asyncio
    @mock.patch.object(GlueCatalogHook, "async_get_partitions")
    async def test_poke(self, mock_async_get_partitions):
        a_mock = mock.AsyncMock()
        a_mock.return_value = True
        mock_async_get_partitions.return_value = a_mock
        trigger = GlueCatalogPartitionTrigger(
            database_name="my_database",
            table_name="my_table",
            expression="my_expression",
            aws_conn_id="my_conn_id",
        )
        response = await trigger.poke(client=mock.MagicMock())

        assert response is True

    def test_serialization(self):
        trigger = GlueCatalogPartitionTrigger(
            database_name="test_database",
            table_name="test_table",
            expression="id=12",
            aws_conn_id="fake_conn_id",
            region_name="eu-west-2",
            verify=True,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.glue.GlueCatalogPartitionTrigger"
        assert kwargs == {
            "database_name": "test_database",
            "table_name": "test_table",
            "expression": "id=12",
            "waiter_delay": 60,
            "aws_conn_id": "fake_conn_id",
            "region_name": "eu-west-2",
            "verify": True,
            "botocore_config": None,
        }


class TestGlueDataQualityEvaluationRunCompletedTrigger:
    EXPECTED_WAITER_NAME = "data_quality_ruleset_evaluation_run_complete"
    RUN_ID = "1234567890abc"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = GlueDataQualityRuleSetEvaluationRunCompleteTrigger(evaluation_run_id=self.RUN_ID)
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "GlueDataQualityRuleSetEvaluationRunCompleteTrigger"
        assert kwargs.get("evaluation_run_id") == self.RUN_ID

    @pytest.mark.asyncio
    @mock.patch.object(GlueDataQualityHook, "get_waiter")
    @mock.patch.object(GlueDataQualityHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = GlueDataQualityRuleSetEvaluationRunCompleteTrigger(evaluation_run_id=self.RUN_ID)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "evaluation_run_id": self.RUN_ID})
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()


class TestGlueDataQualityRuleRecommendationRunCompleteTrigger:
    EXPECTED_WAITER_NAME = "data_quality_rule_recommendation_run_complete"
    RUN_ID = "1234567890abc"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = GlueDataQualityRuleRecommendationRunCompleteTrigger(recommendation_run_id=self.RUN_ID)
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "GlueDataQualityRuleRecommendationRunCompleteTrigger"
        assert kwargs.get("recommendation_run_id") == self.RUN_ID

    @pytest.mark.asyncio
    @mock.patch.object(GlueDataQualityHook, "get_waiter")
    @mock.patch.object(GlueDataQualityHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = GlueDataQualityRuleRecommendationRunCompleteTrigger(recommendation_run_id=self.RUN_ID)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "recommendation_run_id": self.RUN_ID})
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()
