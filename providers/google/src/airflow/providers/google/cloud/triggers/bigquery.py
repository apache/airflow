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
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any, SupportsAbs

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientResponseError
from asgiref.sync import sync_to_async

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryAsyncHook, BigQueryTableAsyncHook
from airflow.providers.google.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

if not AIRFLOW_V_3_0_PLUS:
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import provide_session


class BigQueryInsertJobTrigger(BaseTrigger):
    """
    BigQueryInsertJobTrigger run on the trigger worker to perform insert operation.

    :param conn_id: Reference to google cloud connection id
    :param job_id:  The ID of the job. It will be suffixed with hash of job configuration
    :param project_id: Google Cloud Project where the job is running
    :param location: The dataset location.
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    """

    def __init__(
        self,
        conn_id: str,
        job_id: str | None,
        project_id: str,
        location: str | None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        poll_interval: float = 4.0,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_on_kill: bool = True,
    ):
        super().__init__()
        self.log.info("Using the connection  %s .", conn_id)
        self.conn_id = conn_id
        self.job_id = job_id
        self._job_conn = None
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.location = location
        self.table_id = table_id
        self.poll_interval = poll_interval
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BigQueryInsertJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "location": self.location,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
                "cancel_on_kill": self.cancel_on_kill,
            },
        )

    if not AIRFLOW_V_3_0_PLUS:

        @provide_session
        def get_task_instance(self, session: Session) -> TaskInstance:
            """
            Get the task instance for the current task.

            :param session: Sqlalchemy session
            """
            if not self.task_instance:
                raise RuntimeError(f"TaskInstance not set on {self.__class__.__name__}!")

            query = session.query(TaskInstance).filter(
                TaskInstance.dag_id == self.task_instance.dag_id,
                TaskInstance.task_id == self.task_instance.task_id,
                TaskInstance.run_id == self.task_instance.run_id,
                TaskInstance.map_index == self.task_instance.map_index,
            )
            task_instance = query.one_or_none()
            if task_instance is None:
                raise AirflowException(
                    "TaskInstance with dag_id: %s,task_id: %s, run_id: %s and map_index: %s is not found",
                    self.task_instance.dag_id,
                    self.task_instance.task_id,
                    self.task_instance.run_id,
                    self.task_instance.map_index,
                )
            return task_instance

    async def get_task_state(self):
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        if not self.task_instance:
            raise AirflowException(f"TaskInstance not set on {self.__class__.__name__}!")

        if not isinstance(self.task_instance, RuntimeTaskInstance):
            task_states_response = await sync_to_async(RuntimeTaskInstance.get_task_states)(
                dag_id=self.task_instance.dag_id,
                task_ids=[self.task_instance.task_id],
                run_ids=[self.task_instance.run_id],
                map_index=self.task_instance.map_index,
            )
            try:
                return task_states_response[self.task_instance.run_id][self.task_instance.task_id]
            except Exception:
                raise AirflowException(
                    "TaskInstance with dag_id: %s, task_id: %s, run_id: %s and map_index: %s is not found",
                    self.task_instance.dag_id,
                    self.task_instance.task_id,
                    self.task_instance.run_id,
                    self.task_instance.map_index,
                )
        return self.task_instance.state

    async def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to cancel the external job which is being executed by this trigger.

        This is to avoid the case that `asyncio.CancelledError` is called because the trigger itself is stopped.
        Because in those cases, we should NOT cancel the external job.
        """
        if AIRFLOW_V_3_0_PLUS:
            task_state = await self.get_task_state()
        else:
            # Database query is needed to get the latest state of the task instance.
            task_instance = self.get_task_instance()  # type: ignore[call-arg]
            task_state = task_instance.state
        return task_state != TaskInstanceState.DEFERRED

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current job execution status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            while True:
                job_status = await hook.get_job_status(
                    job_id=self.job_id, project_id=self.project_id, location=self.location
                )
                if job_status["status"] == "success":
                    self.log.info("BigQuery Job succeeded")
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "status": job_status["status"],
                            "message": job_status["message"],
                        }
                    )
                    return
                elif job_status["status"] == "error":
                    self.log.info("BigQuery Job failed: %s", job_status)
                    yield TriggerEvent(
                        {
                            "status": job_status["status"],
                            "message": job_status["message"],
                        }
                    )
                    return
                else:
                    self.log.info(
                        "Bigquery job status is %s. Sleeping for %s seconds.",
                        job_status["status"],
                        self.poll_interval,
                    )
                    await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            if self.job_id and self.cancel_on_kill and await self.safe_to_cancel():
                self.log.info(
                    "The job is safe to cancel the as airflow TaskInstance is not in deferred state."
                )
                self.log.info(
                    "Cancelling job. Project ID: %s, Location: %s, Job ID: %s",
                    self.project_id,
                    self.location,
                    self.job_id,
                )
                await hook.cancel_job(job_id=self.job_id, project_id=self.project_id, location=self.location)
            else:
                self.log.info(
                    "Trigger may have shutdown. Skipping to cancel job because the airflow "
                    "task is not cancelled yet: Project ID: %s, Location:%s, Job ID:%s",
                    self.project_id,
                    self.location,
                    self.job_id,
                )
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> BigQueryAsyncHook:
        return BigQueryAsyncHook(gcp_conn_id=self.conn_id, impersonation_chain=self.impersonation_chain)


class BigQueryCheckTrigger(BigQueryInsertJobTrigger):
    """BigQueryCheckTrigger run on the trigger worker."""

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BigQueryCheckTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryCheckTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "location": self.location,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
                "cancel_on_kill": self.cancel_on_kill,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current job execution status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            while True:
                # Poll for job execution status
                job_status = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if job_status["status"] == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    records = hook.get_records(query_results)

                    # If empty list, then no records are available
                    if not records:
                        yield TriggerEvent(
                            {
                                "status": job_status["status"],
                                "message": job_status["message"],
                                "records": None,
                            }
                        )
                    else:
                        # Extract only first record from the query results
                        first_record = records.pop(0)
                        yield TriggerEvent(
                            {
                                "status": job_status["status"],
                                "message": job_status["message"],
                                "records": first_record,
                            }
                        )
                    return
                elif job_status["status"] == "error":
                    yield TriggerEvent({"status": "error", "message": job_status["message"]})
                    return
                else:
                    self.log.info(
                        "Bigquery job status is %s. Sleeping for %s seconds.",
                        job_status["status"],
                        self.poll_interval,
                    )
                    await asyncio.sleep(self.poll_interval)
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})


class BigQueryGetDataTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryGetDataTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class.

    :param as_dict: if True returns the result as a list of dictionaries, otherwise as list of lists
        (default: False).
    """

    def __init__(self, as_dict: bool = False, selected_fields: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.as_dict = as_dict
        self.selected_fields = selected_fields

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BigQueryInsertJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "location": self.location,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
                "as_dict": self.as_dict,
                "selected_fields": self.selected_fields,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current job execution status and yields a TriggerEvent with response data."""
        hook = self._get_async_hook()
        try:
            while True:
                # Poll for job execution status
                job_status = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if job_status["status"] == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    records = hook.get_records(
                        query_results=query_results,
                        as_dict=self.as_dict,
                        selected_fields=self.selected_fields,
                    )
                    self.log.debug("Response from hook: %s", job_status["status"])
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": job_status["message"],
                            "records": records,
                        }
                    )
                    return
                elif job_status["status"] == "error":
                    yield TriggerEvent(
                        {
                            "status": job_status["status"],
                            "message": job_status["message"],
                        }
                    )
                    return
                else:
                    self.log.info(
                        "Bigquery job status is %s. Sleeping for %s seconds.",
                        job_status["status"],
                        self.poll_interval,
                    )
                    await asyncio.sleep(self.poll_interval)
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})


class BigQueryIntervalCheckTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryIntervalCheckTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class.

    :param conn_id: Reference to google cloud connection id
    :param first_job_id:  The ID of the job 1 performed
    :param second_job_id:  The ID of the job 2 performed
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table: table name
    :param metrics_thresholds: dictionary of ratios indexed by metrics
    :param location: The dataset location.
    :param date_filter_column: column name. (templated)
    :param days_back: number of days between ds and the ds we want to check against. (templated)
    :param ratio_formula: ration formula. (templated)
    :param ignore_zero: boolean value to consider zero or not. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    """

    def __init__(
        self,
        conn_id: str,
        first_job_id: str,
        second_job_id: str,
        project_id: str,
        table: str,
        metrics_thresholds: dict[str, int],
        location: str | None = None,
        date_filter_column: str | None = "ds",
        days_back: SupportsAbs[int] = -7,
        ratio_formula: str = "max_over_min",
        ignore_zero: bool = True,
        dataset_id: str | None = None,
        table_id: str | None = None,
        poll_interval: float = 4.0,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=first_job_id,
            project_id=project_id,
            location=location,
            dataset_id=dataset_id,
            table_id=table_id,
            poll_interval=poll_interval,
            impersonation_chain=impersonation_chain,
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
        """Serialize BigQueryCheckTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger",
            {
                "conn_id": self.conn_id,
                "first_job_id": self.first_job_id,
                "second_job_id": self.second_job_id,
                "project_id": self.project_id,
                "table": self.table,
                "metrics_thresholds": self.metrics_thresholds,
                "location": self.location,
                "date_filter_column": self.date_filter_column,
                "days_back": self.days_back,
                "ratio_formula": self.ratio_formula,
                "ignore_zero": self.ignore_zero,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current job execution status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            while True:
                first_job_response_from_hook = await hook.get_job_status(
                    job_id=self.first_job_id, project_id=self.project_id
                )
                second_job_response_from_hook = await hook.get_job_status(
                    job_id=self.second_job_id, project_id=self.project_id
                )

                if (
                    first_job_response_from_hook["status"] == "success"
                    and second_job_response_from_hook["status"] == "success"
                ):
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
                elif (
                    first_job_response_from_hook["status"] == "pending"
                    or second_job_response_from_hook["status"] == "pending"
                ):
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent(
                        {"status": "error", "message": second_job_response_from_hook["message"], "data": None}
                    )
                    return

        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})


class BigQueryValueCheckTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryValueCheckTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class.

    :param conn_id: Reference to google cloud connection id
    :param sql: the sql to be executed
    :param pass_value: pass value
    :param job_id: The ID of the job
    :param project_id: Google Cloud Project where the job is running
    :param tolerance: certain metrics for tolerance. (templated)
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param location: The dataset location
    :param poll_interval: polling period in seconds to check for the status. (templated)
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        conn_id: str,
        sql: str,
        pass_value: int | float | str,
        job_id: str | None,
        project_id: str,
        tolerance: Any = None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        location: str | None = None,
        poll_interval: float = 4.0,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            location=location,
            poll_interval=poll_interval,
            impersonation_chain=impersonation_chain,
        )
        self.sql = sql
        self.pass_value = pass_value
        self.tolerance = tolerance

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BigQueryValueCheckTrigger arguments and classpath."""
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
                "location": self.location,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current job execution status and yields a TriggerEvent."""
        hook = self._get_async_hook()
        try:
            while True:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook["status"] == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    records = hook.get_records(query_results)
                    _records = records.pop(0) if records else None
                    hook.value_check(self.sql, self.pass_value, _records, self.tolerance)
                    yield TriggerEvent({"status": "success", "message": "Job completed", "records": _records})
                    return
                elif response_from_hook["status"] == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent(
                        {"status": "error", "message": response_from_hook["message"], "records": None}
                    )
                    return
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})


class BigQueryTableExistenceTrigger(BaseTrigger):
    """
    Initialize the BigQuery Table Existence Trigger with needed parameters.

    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table.
    :param table_id: The table ID of the requested table.
    :param gcp_conn_id: Reference to google cloud connection id
    :param hook_params: params for hook
    :param poll_interval: polling period in seconds to check for the status
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str,
        hook_params: dict[str, Any],
        poll_interval: float = 4.0,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.gcp_conn_id: str = gcp_conn_id
        self.poll_interval = poll_interval
        self.hook_params = hook_params
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BigQueryTableExistenceTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger",
            {
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_interval": self.poll_interval,
                "hook_params": self.hook_params,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    def _get_async_hook(self) -> BigQueryTableAsyncHook:
        return BigQueryTableAsyncHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Will run until the table exists in the Google Big Query."""
        try:
            while True:
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

    async def _table_exists(
        self, hook: BigQueryTableAsyncHook, dataset: str, table_id: str, project_id: str
    ) -> bool:
        """
        Create session, make call to BigQueryTableAsyncHook, and check for the table in Google Big Query.

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
                return bool(response)
            except ClientResponseError as err:
                if err.status == 404:
                    return False
                raise err


class BigQueryTablePartitionExistenceTrigger(BigQueryTableExistenceTrigger):
    """
    Initialize the BigQuery Table Partition Existence Trigger with needed parameters.

    :param partition_id: The name of the partition to check the existence of.
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table.
    :param table_id: The table ID of the requested table.
    :param gcp_conn_id: Reference to google cloud connection id
    :param hook_params: params for hook
    :param poll_interval: polling period in seconds to check for the status
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account. (templated)
    """

    def __init__(self, partition_id: str, **kwargs):
        super().__init__(**kwargs)
        self.partition_id = partition_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize BigQueryTablePartitionExistenceTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery.BigQueryTablePartitionExistenceTrigger",
            {
                "partition_id": self.partition_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Will run until the table exists in the Google Big Query."""
        hook = BigQueryAsyncHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        job_id = None
        while True:
            if job_id is not None:
                job_status = await hook.get_job_status(job_id=job_id, project_id=self.project_id)
                if job_status["status"] == "success":
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
                        return
                    job_id = None
                elif job_status["status"] == "error":
                    yield TriggerEvent({"status": job_status["status"]})
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)

            else:
                job_id = await hook.create_job_for_partition_get(
                    self.dataset_id, table_id=self.table_id, project_id=self.project_id
                )
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)

    async def _partition_exists(self, hook: BigQueryAsyncHook, job_id: str | None, project_id: str):
        query_results = await hook.get_job_output(job_id=job_id, project_id=project_id)
        records = hook.get_records(query_results)
        if records:
            records = [row[0] for row in records]
            return self.partition_id in records
