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
from typing import Any, AsyncIterator, SupportsAbs

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientResponseError

from airflow.providers.google.cloud.hooks.bigquery import BigQueryAsyncHook, BigQueryTableAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BigQueryInsertJobTrigger(BaseTrigger):
    """
    BigQueryInsertJobTrigger run on the trigger worker to perform insert operation

    :param conn_id: Reference to google cloud connection id
    :param job_id:  The ID of the job. It will be suffixed with hash of job configuration
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        conn_id: str,
        job_id: str | None,
        project_id: str | None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        poll_interval: float = 4.0,
    ):
        super().__init__()
        self.log.info("Using the connection  %s .", conn_id)
        self.conn_id = conn_id
        self.job_id = job_id
        self._job_conn = None
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryInsertJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                self.log.debug("Response from hook: %s", response_from_hook)

                if response_from_hook == "success":
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                    return
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> BigQueryAsyncHook:
        return BigQueryAsyncHook(gcp_conn_id=self.conn_id)


class BigQueryCheckTrigger(BigQueryInsertJobTrigger):
    """BigQueryCheckTrigger run on the trigger worker"""

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryCheckTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryCheckTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)

                    records = hook.get_records(query_results)

                    # If empty list, then no records are available
                    if not records:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "records": None,
                            }
                        )
                    else:
                        # Extract only first record from the query results
                        first_record = records.pop(0)
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "records": first_record,
                            }
                        )
                    return

                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})


class BigQueryGetDataTrigger(BigQueryInsertJobTrigger):
    """BigQueryGetDataTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class"""

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryInsertJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent with response data"""
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    records = hook.get_records(query_results)
                    self.log.debug("Response from hook: %s", response_from_hook)
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": response_from_hook,
                            "records": records,
                        }
                    )
                    return
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return


class BigQueryIntervalCheckTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryIntervalCheckTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class

    :param conn_id: Reference to google cloud connection id
    :param first_job_id:  The ID of the job 1 performed
    :param second_job_id:  The ID of the job 2 performed
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table: table name
    :param metrics_thresholds: dictionary of ratios indexed by metrics
    :param date_filter_column: column name
    :param days_back: number of days between ds and the ds we want to check
        against
    :param ratio_formula: ration formula
    :param ignore_zero: boolean value to consider zero or not
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        conn_id: str,
        first_job_id: str,
        second_job_id: str,
        project_id: str | None,
        table: str,
        metrics_thresholds: dict[str, int],
        date_filter_column: str | None = "ds",
        days_back: SupportsAbs[int] = -7,
        ratio_formula: str = "max_over_min",
        ignore_zero: bool = True,
        dataset_id: str | None = None,
        table_id: str | None = None,
        poll_interval: float = 4.0,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=first_job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            poll_interval=poll_interval,
        )
        self.conn_id = conn_id
        self.first_job_id = first_job_id
        self.second_job_id = second_job_id
        self.project_id = project_id
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.date_filter_column = date_filter_column
        self.days_back = days_back
        self.ratio_formula = ratio_formula
        self.ignore_zero = ignore_zero

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryCheckTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger",
            {
                "conn_id": self.conn_id,
                "first_job_id": self.first_job_id,
                "second_job_id": self.second_job_id,
                "project_id": self.project_id,
                "table": self.table,
                "metrics_thresholds": self.metrics_thresholds,
                "date_filter_column": self.date_filter_column,
                "days_back": self.days_back,
                "ratio_formula": self.ratio_formula,
                "ignore_zero": self.ignore_zero,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                first_job_response_from_hook = await hook.get_job_status(
                    job_id=self.first_job_id, project_id=self.project_id
                )
                second_job_response_from_hook = await hook.get_job_status(
                    job_id=self.second_job_id, project_id=self.project_id
                )

                if first_job_response_from_hook == "success" and second_job_response_from_hook == "success":
                    first_query_results = await hook.get_job_output(
                        job_id=self.first_job_id, project_id=self.project_id
                    )

                    second_query_results = await hook.get_job_output(
                        job_id=self.second_job_id, project_id=self.project_id
                    )

                    first_records = hook.get_records(first_query_results)

                    second_records = hook.get_records(second_query_results)

                    # If empty list, then no records are available
                    if not first_records:
                        first_job_row: str | None = None
                    else:
                        # Extract only first record from the query results
                        first_job_row = first_records.pop(0)

                    # If empty list, then no records are available
                    if not second_records:
                        second_job_row: str | None = None
                    else:
                        # Extract only first record from the query results
                        second_job_row = second_records.pop(0)

                    hook.interval_check(
                        first_job_row,
                        second_job_row,
                        self.metrics_thresholds,
                        self.ignore_zero,
                        self.ratio_formula,
                    )

                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Job completed",
                            "first_row_data": first_job_row,
                            "second_row_data": second_job_row,
                        }
                    )
                    return
                elif first_job_response_from_hook == "pending" or second_job_response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent(
                        {"status": "error", "message": second_job_response_from_hook, "data": None}
                    )
                    return

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return


class BigQueryValueCheckTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryValueCheckTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class

    :param conn_id: Reference to google cloud connection id
    :param sql: the sql to be executed
    :param pass_value: pass value
    :param job_id:  The ID of the job
    :param project_id: Google Cloud Project where the job is running
    :param tolerance: certain metrics for tolerance
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        conn_id: str,
        sql: str,
        pass_value: int | float | str,
        job_id: str | None,
        project_id: str | None,
        tolerance: Any = None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        poll_interval: float = 4.0,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            poll_interval=poll_interval,
        )
        self.sql = sql
        self.pass_value = pass_value
        self.tolerance = tolerance

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryValueCheckTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger",
            {
                "conn_id": self.conn_id,
                "pass_value": self.pass_value,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "sql": self.sql,
                "table_id": self.table_id,
                "tolerance": self.tolerance,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    records = hook.get_records(query_results)
                    records = records.pop(0) if records else None
                    hook.value_check(self.sql, self.pass_value, records, self.tolerance)
                    yield TriggerEvent({"status": "success", "message": "Job completed", "records": records})
                    return
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook, "records": None})
                    return

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return


class BigQueryTableExistenceTrigger(BaseTrigger):
    """
    Initialize the BigQuery Table Existence Trigger with needed parameters

    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table.
    :param table_id: The table ID of the requested table.
    :param gcp_conn_id: Reference to google cloud connection id
    :param hook_params: params for hook
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str,
        hook_params: dict[str, Any],
        poll_interval: float = 4.0,
    ):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.gcp_conn_id: str = gcp_conn_id
        self.poll_interval = poll_interval
        self.hook_params = hook_params

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryTableExistenceTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger",
            {
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_interval": self.poll_interval,
                "hook_params": self.hook_params,
            },
        )

    def _get_async_hook(self) -> BigQueryTableAsyncHook:
        return BigQueryTableAsyncHook(gcp_conn_id=self.gcp_conn_id)

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Will run until the table exists in the Google Big Query."""
        while True:
            try:
                hook = self._get_async_hook()
                response = await self._table_exists(
                    hook=hook, dataset=self.dataset_id, table_id=self.table_id, project_id=self.project_id
                )
                if response:
                    yield TriggerEvent({"status": "success", "message": "success"})
                    return
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                self.log.exception("Exception occurred while checking for Table existence")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return

    async def _table_exists(
        self, hook: BigQueryTableAsyncHook, dataset: str, table_id: str, project_id: str
    ) -> bool:
        """
        Create client session and make call to BigQueryTableAsyncHook and check for the table in
        Google Big Query.

        :param hook: BigQueryTableAsyncHook Hook class
        :param dataset:  The name of the dataset in which to look for the table storage bucket.
        :param table_id: The name of the table to check the existence of.
        :param project_id: The Google cloud project in which to look for the table.
            The connection supplied to the hook must provide
            access to the specified project.
        """
        async with ClientSession() as session:
            try:
                client = await hook.get_table_client(
                    dataset=dataset, table_id=table_id, project_id=project_id, session=session
                )
                response = await client.get()
                return True if response else False
            except ClientResponseError as err:
                if err.status == 404:
                    return False
                raise err


class BigQueryTablePartitionExistenceTrigger(BigQueryTableExistenceTrigger):
    """
    Initialize the BigQuery Table Partition Existence Trigger with needed parameters
    :param partition_id: The name of the partition to check the existence of.
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table.
    :param table_id: The table ID of the requested table.
    :param gcp_conn_id: Reference to google cloud connection id
    :param hook_params: params for hook
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(self, partition_id: str, **kwargs):
        super().__init__(**kwargs)
        self.partition_id = partition_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryTablePartitionExistenceTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryTablePartitionExistenceTrigger",
            {
                "partition_id": self.partition_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_interval": self.poll_interval,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Will run until the table exists in the Google Big Query."""
        hook = BigQueryAsyncHook(gcp_conn_id=self.gcp_conn_id)
        job_id = None
        while True:
            if job_id is not None:
                status = await hook.get_job_status(job_id=job_id, project_id=self.project_id)
                if status == "success":
                    is_partition = await self._partition_exists(
                        hook=hook, job_id=job_id, project_id=self.project_id
                    )
                    if is_partition:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message": f"Partition: {self.partition_id} in table: {self.table_id}",
                            }
                        )
                    job_id = None
                elif status == "error":
                    yield TriggerEvent({"status": "error", "message": status})
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)

            else:
                job_id = await hook.create_job_for_partition_get(self.dataset_id, project_id=self.project_id)
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)

    async def _partition_exists(self, hook: BigQueryAsyncHook, job_id: str | None, project_id: str):
        query_results = await hook.get_job_output(job_id=job_id, project_id=project_id)
        records = hook.get_records(query_results)
        if records:
            records = [row[0] for row in records]
            return self.partition_id in records
