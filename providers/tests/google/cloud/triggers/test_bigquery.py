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

import asyncio
import logging
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from aiohttp import ClientResponseError, RequestInfo
from gcloud.aio.bigquery import Table
from multidict import CIMultiDict
from yarl import URL

from airflow.providers.google.cloud.hooks.bigquery import BigQueryTableAsyncHook
from airflow.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryTableExistenceTrigger,
    BigQueryTablePartitionExistenceTrigger,
    BigQueryValueCheckTrigger,
)
from airflow.triggers.base import TriggerEvent

pytestmark = pytest.mark.db_test


TEST_CONN_ID = "bq_default"
TEST_JOB_ID = "1234"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET_ID = "bq_dataset"
TEST_TABLE_ID = "bq_table"
TEST_LOCATION = "US"
POLLING_PERIOD_SECONDS = 4.0
TEST_SQL_QUERY = "SELECT count(*) from Any"
TEST_PASS_VALUE = 2
TEST_TOLERANCE = 1
TEST_FIRST_JOB_ID = "5678"
TEST_SECOND_JOB_ID = "6789"
TEST_METRIC_THRESHOLDS: dict[str, int] = {}
TEST_DATE_FILTER_COLUMN = "ds"
TEST_DAYS_BACK = -7
TEST_RATIO_FORMULA = "max_over_min"
TEST_IGNORE_ZERO = True
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_IMPERSONATION_CHAIN = "TEST_SERVICE_ACCOUNT"
TEST_HOOK_PARAMS: dict[str, Any] = {}
TEST_PARTITION_ID = "1234"
TEST_SELECTED_FIELDS = "f0_,f1_"


@pytest.fixture
def insert_job_trigger():
    return BigQueryInsertJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        location=TEST_LOCATION,
        poll_interval=POLLING_PERIOD_SECONDS,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def get_data_trigger():
    return BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        location=None,
        poll_interval=POLLING_PERIOD_SECONDS,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
        selected_fields=TEST_SELECTED_FIELDS,
    )


@pytest.fixture
def table_existence_trigger():
    return BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
        TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def interval_check_trigger():
    return BigQueryIntervalCheckTrigger(
        conn_id=TEST_CONN_ID,
        first_job_id=TEST_FIRST_JOB_ID,
        second_job_id=TEST_SECOND_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        table=TEST_TABLE_ID,
        metrics_thresholds=TEST_METRIC_THRESHOLDS,
        date_filter_column=TEST_DATE_FILTER_COLUMN,
        days_back=TEST_DAYS_BACK,
        ratio_formula=TEST_RATIO_FORMULA,
        ignore_zero=TEST_IGNORE_ZERO,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def check_trigger():
    return BigQueryCheckTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        location=None,
        poll_interval=POLLING_PERIOD_SECONDS,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def value_check_trigger():
    return BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        pass_value=TEST_PASS_VALUE,
        job_id=TEST_JOB_ID,
        dataset_id=TEST_DATASET_ID,
        project_id=TEST_GCP_PROJECT_ID,
        sql=TEST_SQL_QUERY,
        table_id=TEST_TABLE_ID,
        tolerance=TEST_TOLERANCE,
        poll_interval=POLLING_PERIOD_SECONDS,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


class TestBigQueryInsertJobTrigger:
    def test_serialization(self, insert_job_trigger):
        """
        Asserts that the BigQueryInsertJobTrigger correctly serializes its arguments and classpath.
        """
        classpath, kwargs = insert_job_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger"
        assert kwargs == {
            "cancel_on_kill": True,
            "conn_id": TEST_CONN_ID,
            "job_id": TEST_JOB_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "dataset_id": TEST_DATASET_ID,
            "table_id": TEST_TABLE_ID,
            "location": TEST_LOCATION,
            "poll_interval": POLLING_PERIOD_SECONDS,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_bigquery_insert_job_op_trigger_success(self, mock_job_status, insert_job_trigger):
        """
        Tests the BigQueryInsertJobTrigger only fires once the query execution reaches a successful state.
        """
        mock_job_status.return_value = {"status": "success", "message": "Job completed"}

        generator = insert_job_trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": "Job completed", "job_id": TEST_JOB_ID}) == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook._get_job")
    async def test_bigquery_insert_job_trigger_running(self, mock_get_job, caplog, insert_job_trigger):
        """Test that BigQuery Triggers do not fire while a query is still running."""

        mock_get_job.return_value = mock.MagicMock(state="RUNNING")
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(insert_job_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        assert "Bigquery job status is running. Sleeping for 4.0 seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_bigquery_op_trigger_terminated(self, mock_job_status, caplog, insert_job_trigger):
        """Test that BigQuery Triggers fire the correct event in case of an error."""
        mock_job_status.return_value = {
            "status": "error",
            "message": "The conn_id `bq_default` isn't defined",
        }

        generator = insert_job_trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "error", "message": "The conn_id `bq_default` isn't defined"}) == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_bigquery_op_trigger_exception(self, mock_job_status, caplog, insert_job_trigger):
        """Test that BigQuery Triggers fire the correct event in case of an error."""
        mock_job_status.side_effect = Exception("Test exception")

        generator = insert_job_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.cancel_job")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger.safe_to_cancel")
    async def test_bigquery_insert_job_trigger_cancellation(
        self, mock_get_task_instance, mock_get_job_status, mock_cancel_job, caplog, insert_job_trigger
    ):
        """
        Test that BigQueryInsertJobTrigger handles cancellation correctly, logs the appropriate message,
        and conditionally cancels the job based on the `cancel_on_kill` attribute.
        """
        mock_get_task_instance.return_value = True
        insert_job_trigger.cancel_on_kill = True
        insert_job_trigger.job_id = "1234"

        mock_get_job_status.side_effect = [
            {"status": "running", "message": "Job is still running"},
            asyncio.CancelledError(),
        ]

        mock_cancel_job.return_value = asyncio.Future()
        mock_cancel_job.return_value.set_result(None)

        caplog.set_level(logging.INFO)

        try:
            async for _ in insert_job_trigger.run():
                pass
        except asyncio.CancelledError:
            pass

        assert (
            "Task was killed" in caplog.text
            or "Bigquery job status is running. Sleeping for 4.0 seconds." in caplog.text
        ), "Expected messages about task status or cancellation not found in log."
        mock_cancel_job.assert_awaited_once()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.cancel_job")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger.safe_to_cancel")
    async def test_bigquery_insert_job_trigger_cancellation_unsafe_cancellation(
        self, mock_safe_to_cancel, mock_get_job_status, mock_cancel_job, caplog, insert_job_trigger
    ):
        """
        Test that BigQueryInsertJobTrigger logs the appropriate message and does not cancel the job
        if safe_to_cancel returns False even when the task is cancelled.
        """
        mock_safe_to_cancel.return_value = False
        insert_job_trigger.cancel_on_kill = True
        insert_job_trigger.job_id = "1234"

        # Simulate the initial job status as running
        mock_get_job_status.side_effect = [
            {"status": "running", "message": "Job is still running"},
            asyncio.CancelledError(),
            {"status": "running", "message": "Job is still running after cancellation"},
        ]

        caplog.set_level(logging.INFO)

        try:
            async for _ in insert_job_trigger.run():
                pass
        except asyncio.CancelledError:
            pass

        assert (
            "Skipping to cancel job" in caplog.text
        ), "Expected message about skipping cancellation not found in log."
        assert mock_get_job_status.call_count == 2, "Job status should be checked multiple times"


class TestBigQueryGetDataTrigger:
    def test_bigquery_get_data_trigger_serialization(self, get_data_trigger):
        """Asserts that the BigQueryGetDataTrigger correctly serializes its arguments and classpath."""

        classpath, kwargs = get_data_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger"
        assert kwargs == {
            "as_dict": False,
            "conn_id": TEST_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "job_id": TEST_JOB_ID,
            "dataset_id": TEST_DATASET_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "table_id": TEST_TABLE_ID,
            "location": None,
            "selected_fields": TEST_SELECTED_FIELDS,
            "poll_interval": POLLING_PERIOD_SECONDS,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook._get_job")
    async def test_bigquery_get_data_trigger_running(self, mock_get_job, caplog, get_data_trigger):
        """Test that BigQuery Triggers do not fire while a query is still running."""

        mock_get_job.return_value = mock.MagicMock(state="running")
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(get_data_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        assert "Bigquery job status is running. Sleeping for 4.0 seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_bigquery_get_data_trigger_terminated(self, mock_job_status, caplog, get_data_trigger):
        """Test that BigQuery Triggers fire the correct event in case of an error."""
        # Set the status to a value other than success or pending

        mock_job_status.return_value = {
            "status": "error",
            "message": "The conn_id `bq_default` isn't defined",
        }

        generator = get_data_trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "error", "message": "The conn_id `bq_default` isn't defined"}) == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_bigquery_get_data_trigger_exception(self, mock_job_status, caplog, get_data_trigger):
        """Test that BigQuery Triggers fire the correct event in case of an error."""
        mock_job_status.side_effect = Exception("Test exception")

        generator = get_data_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
    async def test_bigquery_get_data_trigger_success_with_data(
        self, mock_job_output, mock_job_status, get_data_trigger
    ):
        """
        Tests that BigQueryGetDataTrigger only fires once the query execution reaches a successful state.
        """
        mock_job_status.return_value = {"status": "success", "message": "Job completed"}
        mock_job_output.return_value = {
            "kind": "bigquery#tableDataList",
            "etag": "test_etag",
            "schema": {
                "fields": [
                    {"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "f1_", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
            "jobReference": {
                "projectId": "test-airflow-providers",
                "jobId": "test_jobid",
                "location": "US",
            },
            "totalRows": "10",
            "rows": [{"f": [{"v": "42"}, {"v": "monthy python"}]}, {"f": [{"v": "42"}, {"v": "fishy fish"}]}],
            "totalBytesProcessed": "0",
            "jobComplete": True,
            "cacheHit": False,
        }

        generator = get_data_trigger.run()
        actual = await generator.asend(None)
        # The extracted row will be parsed and formatted to retrieve the value from the
        # structure - 'rows":[{"f":[{"v":"42"},{"v":"monthy python"}]},{"f":[{"v":"42"},{"v":"fishy fish"}]}]

        assert (
            TriggerEvent(
                {
                    "status": "success",
                    "message": "Job completed",
                    "records": [[42, "monthy python"], [42, "fishy fish"]],
                }
            )
            == actual
        )
        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()


class TestBigQueryCheckTrigger:
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook._get_job")
    async def test_bigquery_check_trigger_running(self, mock_get_job, caplog, check_trigger):
        """Test that BigQuery Triggers do not fire while a query is still running."""

        mock_get_job.return_value = mock.MagicMock(state="running")

        task = asyncio.create_task(check_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        assert "Bigquery job status is running. Sleeping for 4.0 seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_bigquery_check_trigger_terminated(self, mock_job_status, caplog, check_trigger):
        """Test that BigQuery Triggers fire the correct event in case of an error."""
        # Set the status to a value other than success or pending

        mock_job_status.return_value = {
            "status": "error",
            "message": "The conn_id `bq_default` isn't defined",
        }

        generator = check_trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "error", "message": "The conn_id `bq_default` isn't defined"}) == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_check_trigger_exception(self, mock_job_status, caplog, check_trigger):
        """Test that BigQuery Triggers fire the correct event in case of an error."""
        mock_job_status.side_effect = Exception("Test exception")

        generator = check_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    def test_check_trigger_serialization(self, check_trigger):
        """Asserts that the BigQueryCheckTrigger correctly serializes its arguments and classpath."""

        classpath, kwargs = check_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryCheckTrigger"
        assert kwargs == {
            "conn_id": TEST_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "job_id": TEST_JOB_ID,
            "dataset_id": TEST_DATASET_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "table_id": TEST_TABLE_ID,
            "location": None,
            "poll_interval": POLLING_PERIOD_SECONDS,
            "cancel_on_kill": True,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
    async def test_check_trigger_success_with_data(self, mock_job_output, mock_job_status, check_trigger):
        """
        Test the BigQueryCheckTrigger only fires once the query execution reaches a successful state.
        """
        mock_job_status.return_value = {"status": "success", "message": "Job completed"}
        mock_job_output.return_value = {
            "kind": "bigquery#getQueryResultsResponse",
            "etag": "test_etag",
            "schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
            "jobReference": {
                "projectId": "test_airflow-providers",
                "jobId": "test_jobid",
                "location": "US",
            },
            "totalRows": "1",
            "rows": [{"f": [{"v": "22"}]}],
            "totalBytesProcessed": "0",
            "jobComplete": True,
            "cacheHit": False,
        }

        generator = check_trigger.run()
        actual = await generator.asend(None)

        assert TriggerEvent({"status": "success", "message": "Job completed", "records": [22]}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
    async def test_check_trigger_success_without_data(self, mock_job_output, mock_job_status, check_trigger):
        """
        Tests that BigQueryCheckTrigger sends TriggerEvent when no rows are available in the query result.
        """
        mock_job_status.return_value = {"status": "success", "message": "Job completed"}
        mock_job_output.return_value = {
            "kind": "bigquery#getQueryResultsResponse",
            "etag": "test_etag",
            "schema": {
                "fields": [
                    {"name": "value", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
                ]
            },
            "jobReference": {
                "projectId": "test_airflow-airflow-providers",
                "jobId": "test_jobid",
                "location": "US",
            },
            "totalRows": "0",
            "totalBytesProcessed": "0",
            "jobComplete": True,
            "cacheHit": False,
        }

        generator = check_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "Job completed", "records": None}) == actual


class TestBigQueryIntervalCheckTrigger:
    def test_interval_check_trigger_serialization(self, interval_check_trigger):
        """
        Asserts that the BigQueryIntervalCheckTrigger correctly serializes its arguments and classpath.
        """

        classpath, kwargs = interval_check_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger"
        assert kwargs == {
            "conn_id": TEST_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "first_job_id": TEST_FIRST_JOB_ID,
            "second_job_id": TEST_SECOND_JOB_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "table": TEST_TABLE_ID,
            "location": None,
            "metrics_thresholds": TEST_METRIC_THRESHOLDS,
            "date_filter_column": TEST_DATE_FILTER_COLUMN,
            "days_back": TEST_DAYS_BACK,
            "ratio_formula": TEST_RATIO_FORMULA,
            "ignore_zero": TEST_IGNORE_ZERO,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_records")
    async def test_interval_check_trigger_success(
        self, mock_get_records, mock_get_job_output, mock_job_status, interval_check_trigger
    ):
        """
        Tests the BigQueryIntervalCheckTrigger only fires once the query execution reaches a successful state.
        """
        mock_get_records.return_value = {}
        mock_job_status.return_value = {"status": "success", "message": "Job completed"}
        mock_get_job_output.return_value = ["0"]

        generator = interval_check_trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "The second SQL query returned None"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_interval_check_trigger_pending(self, mock_job_status, caplog, interval_check_trigger):
        """
        Tests that the BigQueryIntervalCheckTrigger do not fire while a query is still running.
        """
        mock_job_status.return_value = {"status": "pending", "message": "Job pending"}
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(interval_check_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        assert "Query is still running..." in caplog.text
        assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_interval_check_trigger_terminated(self, mock_job_status, interval_check_trigger):
        """Tests the BigQueryIntervalCheckTrigger fires the correct event in case of an error."""
        # Set the status to a value other than success or pending
        mock_job_status.return_value = {
            "status": "error",
            "message": "The conn_id `bq_default` isn't defined",
        }

        generator = interval_check_trigger.run()
        actual = await generator.asend(None)

        assert (
            TriggerEvent(
                {"status": "error", "message": "The conn_id `bq_default` isn't defined", "data": None}
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_interval_check_trigger_exception(self, mock_job_status, caplog, interval_check_trigger):
        """Tests that the BigQueryIntervalCheckTrigger fires the correct event in case of an error."""
        mock_job_status.side_effect = Exception("Test exception")
        caplog.set_level(logging.DEBUG)

        generator = interval_check_trigger.run()
        actual = await generator.asend(None)

        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual


class TestBigQueryValueCheckTrigger:
    def test_bigquery_value_check_op_trigger_serialization(self, value_check_trigger):
        """Asserts that the BigQueryValueCheckTrigger correctly serializes its arguments and classpath."""

        classpath, kwargs = value_check_trigger.serialize()

        assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger"
        assert kwargs == {
            "conn_id": TEST_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "pass_value": TEST_PASS_VALUE,
            "job_id": TEST_JOB_ID,
            "dataset_id": TEST_DATASET_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "location": None,
            "sql": TEST_SQL_QUERY,
            "table_id": TEST_TABLE_ID,
            "tolerance": TEST_TOLERANCE,
            "poll_interval": POLLING_PERIOD_SECONDS,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_records")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_value_check_op_trigger_success(
        self, mock_job_status, get_job_output, get_records, value_check_trigger
    ):
        """
        Tests BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
        """
        mock_job_status.return_value = {"status": "success", "message": "Job completed"}
        get_job_output.return_value = {}
        get_records.return_value = [[2], [4]]

        await value_check_trigger.run().__anext__()
        await asyncio.sleep(0.5)

        generator = value_check_trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "success", "message": "Job completed", "records": [4]})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_value_check_op_trigger_pending(self, mock_job_status, caplog, value_check_trigger):
        """
        Tests BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
        """
        mock_job_status.return_value = {"status": "pending", "message": "Job pending"}
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(value_check_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is False

        assert "Query is still running..." in caplog.text

        assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_value_check_op_trigger_fail(self, mock_job_status, value_check_trigger):
        """
        Tests BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
        """
        mock_job_status.return_value = {"status": "error", "message": "dummy"}

        generator = value_check_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "dummy", "records": None}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
    async def test_value_check_trigger_exception(self, mock_job_status):
        """Tests the BigQueryValueCheckTrigger does not fire if there is an exception."""
        mock_job_status.side_effect = Exception("Test exception")

        trigger = BigQueryValueCheckTrigger(
            conn_id=TEST_CONN_ID,
            sql=TEST_SQL_QUERY,
            pass_value=TEST_PASS_VALUE,
            tolerance=1,
            job_id=TEST_JOB_ID,
            project_id=TEST_GCP_PROJECT_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual


class TestBigQueryTableExistenceTrigger:
    def test_table_existence_trigger_serialization(self, table_existence_trigger):
        """
        Asserts that the BigQueryTableExistenceTrigger correctly serializes its arguments and classpath.
        """

        classpath, kwargs = table_existence_trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger"
        assert kwargs == {
            "dataset_id": TEST_DATASET_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "table_id": TEST_TABLE_ID,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "poll_interval": POLLING_PERIOD_SECONDS,
            "hook_params": TEST_HOOK_PARAMS,
        }

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists"
    )
    async def test_big_query_table_existence_trigger_success(
        self, mock_table_exists, table_existence_trigger
    ):
        """Tests success case BigQueryTableExistenceTrigger"""
        mock_table_exists.return_value = True

        generator = table_existence_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "success"}) == actual

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists"
    )
    async def test_table_existence_trigger_pending(self, mock_table_exists, table_existence_trigger):
        """Test that BigQueryTableExistenceTrigger is in loop till the table exist."""
        mock_table_exists.return_value = False

        task = asyncio.create_task(table_existence_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists"
    )
    async def test_table_existence_trigger_exception(self, mock_table_exists, table_existence_trigger):
        """Test BigQueryTableExistenceTrigger throws exception if any error."""
        mock_table_exists.side_effect = AsyncMock(side_effect=Exception("Test exception"))

        generator = table_existence_trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryTableAsyncHook.get_table_client")
    async def test_table_exists(self, mock_get_table_client, table_existence_trigger):
        """Test BigQueryTableExistenceTrigger._table_exists async function with mocked value
        and mocked return value"""
        hook = BigQueryTableAsyncHook()
        mock_get_table_client.return_value = AsyncMock(Table)

        res = await table_existence_trigger._table_exists(
            hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID
        )
        assert res is True

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryTableAsyncHook.get_table_client")
    async def test_table_exists_exception(self, mock_get_table_client, table_existence_trigger):
        """Test BigQueryTableExistenceTrigger._table_exists async function with exception and return False"""
        hook = BigQueryTableAsyncHook()
        mock_get_table_client.side_effect = ClientResponseError(
            history=(),
            request_info=RequestInfo(
                headers=CIMultiDict(),
                real_url=URL("https://example.com"),
                method="GET",
                url=URL("https://example.com"),
            ),
            status=404,
            message="Not Found",
        )

        res = await table_existence_trigger._table_exists(
            hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID
        )
        expected_response = False
        assert res == expected_response

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryTableAsyncHook.get_table_client")
    async def test_table_exists_raise_exception(self, mock_get_table_client, table_existence_trigger):
        """Test BigQueryTableExistenceTrigger._table_exists async function with raise exception"""
        hook = BigQueryTableAsyncHook()
        mock_get_table_client.side_effect = ClientResponseError(
            history=(),
            request_info=RequestInfo(
                headers=CIMultiDict(),
                real_url=URL("https://example.com"),
                method="GET",
                url=URL("https://example.com"),
            ),
            status=400,
            message="Not Found",
        )

        with pytest.raises(ClientResponseError):
            await table_existence_trigger._table_exists(
                hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID
            )


class TestBigQueryTablePartitionExistenceTrigger:
    def test_serialization_successfully(self):
        """
        Asserts that the BigQueryTablePartitionExistenceTrigger correctly serializes its arguments
        and classpath.
        """

        trigger = BigQueryTablePartitionExistenceTrigger(
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
            partition_id=TEST_PARTITION_ID,
            poll_interval=POLLING_PERIOD_SECONDS,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            hook_params={},
        )

        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.bigquery.BigQueryTablePartitionExistenceTrigger"
        )
        assert kwargs == {
            "dataset_id": TEST_DATASET_ID,
            "project_id": TEST_GCP_PROJECT_ID,
            "table_id": TEST_TABLE_ID,
            "partition_id": TEST_PARTITION_ID,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "poll_interval": POLLING_PERIOD_SECONDS,
            "hook_params": TEST_HOOK_PARAMS,
        }
