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
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook, GlueJobHook
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.triggers.glue import (
    GlueCatalogPartitionTrigger,
    GlueDataQualityRuleRecommendationRunCompleteTrigger,
    GlueDataQualityRuleSetEvaluationRunCompleteTrigger,
    GlueJobCompleteTrigger,
)
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
        event = await generator.asend(None)

        assert_expected_waiter_type(mock_get_waiter, "job_complete")
        assert event.payload["status"] == "error"
        assert "message" in event.payload
        assert event.payload["run_id"] == "JobRunId"

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

    def test_serialization_verbose(self):
        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="JobRunId",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_max_attempts=3,
            waiter_delay=10,
        )
        classpath, kwargs = trigger.serialize()
        assert kwargs["verbose"] is True

    @pytest.mark.asyncio
    @mock.patch.object(AwsLogsHook, "get_async_conn")
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_verbose_run_success(self, mock_glue_conn, mock_logs_conn):
        """When verbose=True, the trigger polls job state and fetches CloudWatch logs."""
        glue_client = AsyncMock()
        glue_client.get_job_run = AsyncMock(
            side_effect=[
                # First call: log group metadata
                {"JobRun": {"JobRunState": "RUNNING", "LogGroupName": "/aws-glue/python-jobs"}},
                # Second call: state check at top of iteration 1 (RUNNING)
                {"JobRun": {"JobRunState": "RUNNING", "LogGroupName": "/aws-glue/python-jobs"}},
                # Third call: state check at top of iteration 2 (SUCCEEDED)
                {"JobRun": {"JobRunState": "SUCCEEDED", "LogGroupName": "/aws-glue/python-jobs"}},
            ]
        )
        mock_glue_conn.return_value.__aenter__ = AsyncMock(return_value=glue_client)
        mock_glue_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(
            return_value={
                "events": [{"timestamp": 1234, "message": "Processing step 1\n"}],
                "nextForwardToken": "token_1",
            }
        )
        mock_logs_conn.return_value.__aenter__ = AsyncMock(return_value=logs_client)
        mock_logs_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_delay=0,
            waiter_max_attempts=5,
        )
        generator = trigger.run()
        event = await generator.asend(None)

        assert event.payload["status"] == "success"
        assert event.payload["run_id"] == "jr_123"
        # Logs client was called for both output and error streams
        assert logs_client.get_log_events.call_count >= 2

    @pytest.mark.asyncio
    @mock.patch.object(AwsLogsHook, "get_async_conn")
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_verbose_run_job_failed(self, mock_glue_conn, mock_logs_conn):
        """When verbose=True and the job fails, the trigger yields an error event."""
        glue_client = AsyncMock()
        glue_client.get_job_run = AsyncMock(
            return_value={"JobRun": {"JobRunState": "FAILED", "LogGroupName": "/aws-glue/python-jobs"}}
        )
        mock_glue_conn.return_value.__aenter__ = AsyncMock(return_value=glue_client)
        mock_glue_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(return_value={"events": [], "nextForwardToken": "token_1"})
        mock_logs_conn.return_value.__aenter__ = AsyncMock(return_value=logs_client)
        mock_logs_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_delay=0,
            waiter_max_attempts=5,
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "error"
        assert "FAILED" in event.payload["message"]
        assert event.payload["run_id"] == "jr_123"

    @pytest.mark.asyncio
    @mock.patch.object(AwsLogsHook, "get_async_conn")
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_verbose_run_max_attempts(self, mock_glue_conn, mock_logs_conn):
        """When verbose=True and the job stays RUNNING past max attempts, yields an error event."""
        glue_client = AsyncMock()
        glue_client.get_job_run = AsyncMock(
            return_value={"JobRun": {"JobRunState": "RUNNING", "LogGroupName": "/aws-glue/python-jobs"}}
        )
        mock_glue_conn.return_value.__aenter__ = AsyncMock(return_value=glue_client)
        mock_glue_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(return_value={"events": [], "nextForwardToken": "token_1"})
        mock_logs_conn.return_value.__aenter__ = AsyncMock(return_value=logs_client)
        mock_logs_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_delay=0,
            waiter_max_attempts=2,
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "error"
        assert "max attempts" in event.payload["message"]
        assert event.payload["run_id"] == "jr_123"

    @pytest.mark.asyncio
    @mock.patch.object(AwsLogsHook, "get_async_conn")
    @mock.patch.object(GlueJobHook, "get_async_conn")
    async def test_verbose_run_cloudwatch_client_error(self, mock_glue_conn, mock_logs_conn):
        """When verbose=True and CloudWatch returns an unexpected ClientError, yields error event."""
        glue_client = AsyncMock()
        glue_client.get_job_run = AsyncMock(
            return_value={"JobRun": {"JobRunState": "RUNNING", "LogGroupName": "/aws-glue/python-jobs"}}
        )
        mock_glue_conn.return_value.__aenter__ = AsyncMock(return_value=glue_client)
        mock_glue_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(
            side_effect=ClientError(
                {"Error": {"Code": "AccessDeniedException", "Message": "not authorized"}},
                "GetLogEvents",
            )
        )
        mock_logs_conn.return_value.__aenter__ = AsyncMock(return_value=logs_client)
        mock_logs_conn.return_value.__aexit__ = AsyncMock(return_value=False)

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_delay=0,
            waiter_max_attempts=5,
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event.payload["status"] == "error"
        assert "Failed to fetch logs" in event.payload["message"]
        assert "AccessDeniedException" in event.payload["message"]
        assert event.payload["run_id"] == "jr_123"

    @pytest.mark.asyncio
    async def test_forward_logs_resource_not_found(self):
        """_forward_logs handles ResourceNotFoundException gracefully and uses resolved region in URL."""
        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(
            side_effect=ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
                "GetLogEvents",
            )
        )
        logs_client.meta.region_name = "eu-west-1"

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            region_name=None,
            waiter_delay=0,
            waiter_max_attempts=5,
        )
        with mock.patch.object(trigger.log, "warning") as mock_log_warning:
            result = await trigger._forward_logs(logs_client, "/aws-glue/python-jobs/output", "jr_123", None)
        assert result is None
        # Verify the URL uses the resolved region from the client, not self.region_name (which is None)
        url_arg = mock_log_warning.call_args[0][1]
        assert "eu-west-1" in url_arg

    @pytest.mark.asyncio
    async def test_forward_logs_pagination(self):
        """_forward_logs follows nextForwardToken and formats logs like the sync path."""
        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(
            side_effect=[
                {
                    "events": [{"timestamp": 1, "message": "line 1\n"}],
                    "nextForwardToken": "token_2",
                },
                {
                    "events": [{"timestamp": 2, "message": "line 2\n"}],
                    "nextForwardToken": "token_3",
                },
                {
                    "events": [],
                    "nextForwardToken": "token_3",
                },
            ]
        )

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_delay=0,
            waiter_max_attempts=5,
        )
        with mock.patch.object(trigger.log, "info") as mock_log_info:
            result = await trigger._forward_logs(logs_client, "/aws-glue/python-jobs/output", "jr_123", None)
        assert result == "token_3"
        assert logs_client.get_log_events.call_count == 3
        # Verify log format matches sync path: "Glue Job Run <log_group> Logs:" with tab-indented lines
        log_output = mock_log_info.call_args[0][0]
        assert "Glue Job Run /aws-glue/python-jobs/output Logs:" in log_output
        assert "\tline 1" in log_output
        assert "\tline 2" in log_output

    @pytest.mark.asyncio
    async def test_forward_logs_no_new_events(self):
        """_forward_logs logs 'No new log' when there are no events, matching sync path."""
        logs_client = AsyncMock()
        logs_client.get_log_events = AsyncMock(return_value={"events": [], "nextForwardToken": "token_1"})

        trigger = GlueJobCompleteTrigger(
            job_name="job_name",
            run_id="jr_123",
            verbose=True,
            aws_conn_id="aws_conn_id",
            waiter_delay=0,
            waiter_max_attempts=5,
        )
        with mock.patch.object(trigger.log, "info") as mock_log_info:
            result = await trigger._forward_logs(logs_client, "/aws-glue/python-jobs/output", "jr_123", None)
        assert result == "token_1"
        log_output = mock_log_info.call_args[0][0]
        assert "No new log from the Glue Job in /aws-glue/python-jobs/output" in log_output


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
