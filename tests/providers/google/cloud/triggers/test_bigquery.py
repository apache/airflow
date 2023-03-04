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
from tests.providers.google.cloud.utils.compat import AsyncMock, async_mock

TEST_CONN_ID = "bq_default"
TEST_JOB_ID = "1234"
RUN_ID = "1"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET_ID = "bq_dataset"
TEST_TABLE_ID = "bq_table"
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
TEST_HOOK_PARAMS: dict[str, Any] = {}
TEST_PARTITION_ID = "1234"


def test_bigquery_insert_job_op_trigger_serialization():
    """
    Asserts that the BigQueryInsertJobTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryInsertJobTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "job_id": TEST_JOB_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "dataset_id": TEST_DATASET_ID,
        "table_id": TEST_TABLE_ID,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_insert_job_op_trigger_success(mock_job_status):
    """
    Tests the BigQueryInsertJobTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"

    trigger = BigQueryInsertJobTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "message": "Job completed", "job_id": TEST_JOB_ID}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
async def test_bigquery_insert_job_trigger_running(mock_job_instance, caplog):
    """
    Test that BigQuery Triggers do not fire while a query is still running.
    """

    from gcloud.aio.bigquery import Job

    mock_job_client = AsyncMock(Job)
    mock_job_instance.return_value = mock_job_client
    mock_job_instance.return_value.result.side_effect = OSError
    caplog.set_level(logging.INFO)

    trigger = BigQueryInsertJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Query is still running..." in caplog.text
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
async def test_bigquery_get_data_trigger_running(mock_job_instance, caplog):
    """
    Test that BigQuery Triggers do not fire while a query is still running.
    """

    from gcloud.aio.bigquery import Job

    mock_job_client = AsyncMock(Job)
    mock_job_instance.return_value = mock_job_client
    mock_job_instance.return_value.result.side_effect = OSError
    caplog.set_level(logging.INFO)

    trigger = BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Query is still running..." in caplog.text
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
async def test_bigquery_check_trigger_running(mock_job_instance, caplog):
    """
    Test that BigQuery Triggers do not fire while a query is still running.
    """

    from gcloud.aio.bigquery import Job

    mock_job_client = AsyncMock(Job)
    mock_job_instance.return_value = mock_job_client
    mock_job_instance.return_value.result.side_effect = OSError
    caplog.set_level(logging.INFO)

    trigger = BigQueryCheckTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Query is still running..." in caplog.text
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_op_trigger_terminated(mock_job_status, caplog):
    """
    Test that BigQuery Triggers fire the correct event in case of an error.
    """
    # Set the status to a value other than success or pending

    mock_job_status.return_value = "error"

    trigger = BigQueryInsertJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "error"}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_check_trigger_terminated(mock_job_status, caplog):
    """
    Test that BigQuery Triggers fire the correct event in case of an error.
    """
    # Set the status to a value other than success or pending

    mock_job_status.return_value = "error"

    trigger = BigQueryCheckTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "error"}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_get_data_trigger_terminated(mock_job_status, caplog):
    """
    Test that BigQuery Triggers fire the correct event in case of an error.
    """
    # Set the status to a value other than success or pending

    mock_job_status.return_value = "error"

    trigger = BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "error"}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_op_trigger_exception(mock_job_status, caplog):
    """
    Test that BigQuery Triggers fire the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")

    trigger = BigQueryInsertJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_check_trigger_exception(mock_job_status, caplog):
    """
    Test that BigQuery Triggers fire the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")

    trigger = BigQueryCheckTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_get_data_trigger_exception(mock_job_status, caplog):
    """
    Test that BigQuery Triggers fire the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")

    trigger = BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "Test exception"}) == actual


def test_bigquery_check_op_trigger_serialization():
    """
    Asserts that the BigQueryCheckTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryCheckTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryCheckTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "job_id": TEST_JOB_ID,
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "table_id": TEST_TABLE_ID,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
async def test_bigquery_check_op_trigger_success_with_data(mock_job_output, mock_job_status):
    """
    Test the BigQueryCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
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

    trigger = BigQueryCheckTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)

    assert TriggerEvent({"status": "success", "records": [22]}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
async def test_bigquery_check_op_trigger_success_without_data(mock_job_output, mock_job_status):
    """
    Tests that BigQueryCheckTrigger sends TriggerEvent as  { "status": "success", "records": None}
    when no rows are available in the query result.
    """
    mock_job_status.return_value = "success"
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

    trigger = BigQueryCheckTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "records": None}) == actual


def test_bigquery_get_data_trigger_serialization():
    """
    Asserts that the BigQueryGetDataTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "job_id": TEST_JOB_ID,
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "table_id": TEST_TABLE_ID,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
async def test_bigquery_get_data_trigger_success_with_data(mock_job_output, mock_job_status):
    """
    Tests that BigQueryGetDataTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
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

    trigger = BigQueryGetDataTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    # # The extracted row will be parsed and formatted to retrieve the value from the
    # # structure - 'rows":[{"f":[{"v":"42"},{"v":"monthy python"}]},{"f":[{"v":"42"},{"v":"fishy fish"}]}]

    assert (
        TriggerEvent(
            {
                "status": "success",
                "message": "success",
                "records": [[42, "monthy python"], [42, "fishy fish"]],
            }
        )
        == actual
    )
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


def test_bigquery_interval_check_trigger_serialization():
    """
    Asserts that the BigQueryIntervalCheckTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryIntervalCheckTrigger(
        TEST_CONN_ID,
        TEST_FIRST_JOB_ID,
        TEST_SECOND_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_TABLE_ID,
        TEST_METRIC_THRESHOLDS,
        TEST_DATE_FILTER_COLUMN,
        TEST_DAYS_BACK,
        TEST_RATIO_FORMULA,
        TEST_IGNORE_ZERO,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "first_job_id": TEST_FIRST_JOB_ID,
        "second_job_id": TEST_SECOND_JOB_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "table": TEST_TABLE_ID,
        "metrics_thresholds": TEST_METRIC_THRESHOLDS,
        "date_filter_column": TEST_DATE_FILTER_COLUMN,
        "days_back": TEST_DAYS_BACK,
        "ratio_formula": TEST_RATIO_FORMULA,
        "ignore_zero": TEST_IGNORE_ZERO,
    }


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
async def test_bigquery_interval_check_trigger_success(mock_get_job_output, mock_job_status):
    """
    Tests the BigQueryIntervalCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
    mock_get_job_output.return_value = ["0"]

    trigger = BigQueryIntervalCheckTrigger(
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
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert actual == TriggerEvent({"status": "error", "message": "The second SQL query returned None"})


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_interval_check_trigger_pending(mock_job_status, caplog):
    """
    Tests that the BigQueryIntervalCheckTrigger do not fire while a query is still running.
    """
    mock_job_status.return_value = "pending"
    caplog.set_level(logging.INFO)

    trigger = BigQueryIntervalCheckTrigger(
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
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Query is still running..." in caplog.text
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_interval_check_trigger_terminated(mock_job_status):
    """
    Tests the BigQueryIntervalCheckTrigger fires the correct event in case of an error.
    """
    # Set the status to a value other than success or pending
    mock_job_status.return_value = "error"
    trigger = BigQueryIntervalCheckTrigger(
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
    )

    generator = trigger.run()
    actual = await generator.asend(None)

    assert TriggerEvent({"status": "error", "message": "error", "data": None}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_interval_check_trigger_exception(mock_job_status, caplog):
    """
    Tests that the BigQueryIntervalCheckTrigger fires the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")
    caplog.set_level(logging.DEBUG)

    trigger = BigQueryIntervalCheckTrigger(
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
    )

    # trigger event is yielded so it creates a generator object
    # so i have used async for to get all the values and added it to task
    task = [i async for i in trigger.run()]
    # since we use return as soon as we yield the trigger event
    # at any given point there should be one trigger event returned to the task
    # so we validate for length of task to be 1

    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


def test_bigquery_value_check_op_trigger_serialization():
    """
    Asserts that the BigQueryValueCheckTrigger correctly serializes its arguments
    and classpath.
    """

    trigger = BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        pass_value=TEST_PASS_VALUE,
        job_id=TEST_JOB_ID,
        dataset_id=TEST_DATASET_ID,
        project_id=TEST_GCP_PROJECT_ID,
        sql=TEST_SQL_QUERY,
        table_id=TEST_TABLE_ID,
        tolerance=TEST_TOLERANCE,
        poll_interval=POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()

    assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "pass_value": TEST_PASS_VALUE,
        "job_id": TEST_JOB_ID,
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "sql": TEST_SQL_QUERY,
        "table_id": TEST_TABLE_ID,
        "tolerance": TEST_TOLERANCE,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_records")
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_output")
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_value_check_op_trigger_success(mock_job_status, get_job_output, get_records):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
    get_job_output.return_value = {}
    get_records.return_value = [[2], [4]]

    trigger = BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        pass_value=TEST_PASS_VALUE,
        job_id=TEST_JOB_ID,
        dataset_id=TEST_DATASET_ID,
        project_id=TEST_GCP_PROJECT_ID,
        sql=TEST_SQL_QUERY,
        table_id=TEST_TABLE_ID,
        tolerance=TEST_TOLERANCE,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    generator = trigger.run()
    actual = await generator.asend(None)
    assert actual == TriggerEvent({"status": "success", "message": "Job completed", "records": [4]})


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_value_check_op_trigger_pending(mock_job_status, caplog):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "pending"
    caplog.set_level(logging.INFO)

    trigger = BigQueryValueCheckTrigger(
        TEST_CONN_ID,
        TEST_PASS_VALUE,
        TEST_JOB_ID,
        TEST_DATASET_ID,
        TEST_GCP_PROJECT_ID,
        TEST_SQL_QUERY,
        TEST_TABLE_ID,
        TEST_TOLERANCE,
        POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is False

    assert "Query is still running..." in caplog.text

    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_value_check_op_trigger_fail(mock_job_status):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "dummy"

    trigger = BigQueryValueCheckTrigger(
        TEST_CONN_ID,
        TEST_PASS_VALUE,
        TEST_JOB_ID,
        TEST_DATASET_ID,
        TEST_GCP_PROJECT_ID,
        TEST_SQL_QUERY,
        TEST_TABLE_ID,
        TEST_TOLERANCE,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "dummy", "records": None}) == actual


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_status")
async def test_bigquery_value_check_trigger_exception(mock_job_status):
    """
    Tests the BigQueryValueCheckTrigger does not fire if there is an exception.
    """
    mock_job_status.side_effect = Exception("Test exception")

    trigger = BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        sql=TEST_SQL_QUERY,
        pass_value=TEST_PASS_VALUE,
        tolerance=1,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
    )

    # trigger event is yielded so it creates a generator object
    # so i have used async for to get all the values and added it to task
    task = [i async for i in trigger.run()]
    # since we use return as soon as we yield the trigger event
    # at any given point there should be one trigger event returned to the task
    # so we validate for length of task to be 1

    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


def test_big_query_table_existence_trigger_serialization():
    """
    Asserts that the BigQueryTableExistenceTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger"
    assert kwargs == {
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "table_id": TEST_TABLE_ID,
        "gcp_conn_id": TEST_GCP_CONN_ID,
        "poll_interval": POLLING_PERIOD_SECONDS,
        "hook_params": TEST_HOOK_PARAMS,
    }


@pytest.mark.asyncio
@async_mock.patch(
    "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists"
)
async def test_big_query_table_existence_trigger_success(mock_table_exists):
    """
    Tests success case BigQueryTableExistenceTrigger
    """
    mock_table_exists.return_value = True

    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "message": "success"}) == actual


@pytest.mark.asyncio
@async_mock.patch(
    "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists"
)
async def test_big_query_table_existence_trigger_pending(mock_table_exists):
    """
    Test that BigQueryTableExistenceTrigger is in loop till the table exist.
    """
    mock_table_exists.return_value = False

    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@async_mock.patch(
    "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists"
)
async def test_big_query_table_existence_trigger_exception(mock_table_exists):
    """
    Test BigQueryTableExistenceTrigger throws exception if any error.
    """
    mock_table_exists.side_effect = AsyncMock(side_effect=Exception("Test exception"))

    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryTableAsyncHook.get_table_client")
async def test_table_exists(mock_get_table_client):
    """Test BigQueryTableExistenceTrigger._table_exists async function with mocked value
    and mocked return value"""
    hook = BigQueryTableAsyncHook()
    mock_get_table_client.return_value = AsyncMock(Table)
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    res = await trigger._table_exists(hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID)
    assert res is True


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryTableAsyncHook.get_table_client")
async def test_table_exists_exception(mock_get_table_client):
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
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    res = await trigger._table_exists(hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID)
    expected_response = False
    assert res == expected_response


@pytest.mark.asyncio
@async_mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryTableAsyncHook.get_table_client")
async def test_table_exists_raise_exception(mock_get_table_client):
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
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    with pytest.raises(ClientResponseError):
        await trigger._table_exists(hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID)


class TestBigQueryTablePartitionExistenceTrigger:
    def test_big_query_table_existence_partition_trigger_serialization_should_execute_successfully(self):
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
            "poll_interval": POLLING_PERIOD_SECONDS,
            "hook_params": TEST_HOOK_PARAMS,
        }
