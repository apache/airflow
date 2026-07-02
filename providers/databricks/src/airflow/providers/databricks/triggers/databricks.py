#
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
import time
from typing import Any

from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.utils.databricks import extract_failed_task_errors_async
from airflow.providers.databricks.utils.retry import validate_deferrable_databricks_retry_args
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DatabricksExecutionTrigger(BaseTrigger):
    """
    The trigger handles the logic of async communication with DataBricks API.

    :param run_id: id of the run
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param polling_period_seconds: Controls the rate of the poll for the result of this run.
        By default, the trigger will poll every 30 seconds.
    :param retry_limit: The number of times to retry the connection in case of service outages.
    :param retry_delay: The number of seconds to wait between retries.
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param run_page_url: The run page url.
    :param repair_run: Repair the databricks run in case of failure.
    :param caller: The name of the operator that is calling the hook.
    :param workflow_run_id: Parent workflow run ID for task-level monitoring.
    :param databricks_task_key: Task key to monitor within ``workflow_run_id``.
    """

    def __init__(
        self,
        run_id: int,
        databricks_conn_id: str,
        polling_period_seconds: int = 30,
        retry_limit: int = 3,
        retry_delay: int = 10,
        retry_args: dict[Any, Any] | None = None,
        run_page_url: str | None = None,
        repair_run: bool = False,
        caller: str = "DatabricksExecutionTrigger",
        workflow_run_id: int | None = None,
        databricks_task_key: str | None = None,
    ) -> None:
        super().__init__()
        # Trigger kwargs cross Airflow's serialization boundary, so fail before storing invalid
        # trigger state or surfacing a generic serializer error without Databricks-specific guidance.
        validate_deferrable_databricks_retry_args(retry_args, owner=caller)
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
        self.run_page_url = run_page_url
        self.repair_run = repair_run
        self.caller = caller
        self.workflow_run_id = workflow_run_id
        self.databricks_task_key = databricks_task_key
        self.hook = DatabricksHook(
            databricks_conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
            retry_args=retry_args,
            caller=caller,
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.databricks.DatabricksExecutionTrigger",
            {
                "run_id": self.run_id,
                "databricks_conn_id": self.databricks_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "retry_args": self.retry_args,
                "run_page_url": self.run_page_url,
                "repair_run": self.repair_run,
                "caller": self.caller,
                "workflow_run_id": self.workflow_run_id,
                "databricks_task_key": self.databricks_task_key,
            },
        )

    async def on_kill(self) -> None:
        """Cancel the Databricks run when the trigger is cancelled by a user action."""
        from asgiref.sync import sync_to_async

        run_id = self.run_id
        if self.workflow_run_id is not None and self.databricks_task_key is not None:
            # self.run_id may be an earlier, now-terminal attempt; cancel the task's latest attempt
            # so a retry/repair launched under the same task_key is not left running.
            tasks = await sync_to_async(self.hook.get_run_tasks)(self.workflow_run_id)
            attempt = {
                task["task_key"]: task for task in sorted(tasks, key=lambda task: task["start_time"])
            }.get(self.databricks_task_key)
            if attempt:
                run_id = attempt["run_id"]
        if run_id:
            self.log.info("Cancelling Databricks run %s.", run_id)
            await sync_to_async(self.hook.cancel_run)(run_id)

    def _monitors_workflow_task(self) -> bool:
        """Whether this trigger follows one task inside a shared workflow run (see ``workflow_run_id``)."""
        return bool(self.workflow_run_id and self.databricks_task_key)

    async def run(self):
        async with self.hook:
            if self._monitors_workflow_task():
                async for event in self._run_workflow_task():
                    yield event
                return
            while True:
                run_state = await self.hook.a_get_run_state(self.run_id)
                if not run_state.is_terminal:
                    self.log.info(
                        "run-id %s in run state %s. sleeping for %s seconds",
                        self.run_id,
                        run_state,
                        self.polling_period_seconds,
                    )
                    await asyncio.sleep(self.polling_period_seconds)
                    continue

                run_info = await self.hook.a_get_run(self.run_id)
                failed_tasks = await extract_failed_task_errors_async(self.hook, run_info, run_state)
                yield TriggerEvent(
                    {
                        "run_id": self.run_id,
                        "run_page_url": self.run_page_url,
                        "run_state": run_state.to_json(),
                        "repair_run": self.repair_run,
                        "errors": failed_tasks,
                    }
                )
                return

    async def _run_workflow_task(self):
        """
        Monitor a single task within a workflow run, tolerating in-flight retries/repairs.

        Tradeoff: a task whose attempt has failed (Databricks-native retries exhausted) is reported
        as failed only once the parent run is itself terminal. Until then the trigger keeps polling,
        because Databricks may still launch a retry/repair attempt under the same ``task_key`` and
        there is no per-task "retries exhausted" signal before the run terminates. Sibling tasks in
        the run continue independently in the meantime.
        """
        from asgiref.sync import sync_to_async

        while True:
            tasks = await sync_to_async(self.hook.get_run_tasks)(self.workflow_run_id)
            sorted_task_runs = sorted(tasks, key=lambda task: task["start_time"])
            attempt = {task["task_key"]: task for task in sorted_task_runs}.get(self.databricks_task_key)

            if attempt is not None:
                attempt_run_id = attempt["run_id"]
                attempt_state = await self.hook.a_get_run_state(attempt_run_id)

                if attempt_state.is_terminal:
                    if attempt_state.is_successful:
                        yield TriggerEvent(
                            {
                                "run_id": attempt_run_id,
                                "run_page_url": self.run_page_url,
                                "run_state": attempt_state.to_json(),
                                "repair_run": self.repair_run,
                                "errors": [],
                            }
                        )
                        return
                    # The attempt failed: only report failure once the parent run is also terminal,
                    # otherwise a retry/repair attempt may still be launched under the same task_key.
                    parent_state = await self.hook.a_get_run_state(self.workflow_run_id)
                    if parent_state.is_terminal:
                        run_info = await self.hook.a_get_run(attempt_run_id)
                        failed_tasks = await extract_failed_task_errors_async(
                            self.hook, run_info, attempt_state
                        )
                        yield TriggerEvent(
                            {
                                "run_id": attempt_run_id,
                                "run_page_url": self.run_page_url,
                                "run_state": attempt_state.to_json(),
                                "repair_run": self.repair_run,
                                "errors": failed_tasks,
                            }
                        )
                        return

            # attempt is None when the task has not yet surfaced in the run (e.g. just after launch);
            # keep polling rather than crashing on a missing task_key.
            self.log.info(
                "databricks task %s not yet conclusive in run %s. sleeping for %s seconds",
                self.databricks_task_key,
                self.workflow_run_id,
                self.polling_period_seconds,
            )
            await asyncio.sleep(self.polling_period_seconds)


class DatabricksSQLStatementExecutionTrigger(BaseTrigger):
    """
    The trigger handles the logic of async communication with DataBricks SQL Statements API.

    :param statement_id: ID of the SQL statement.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param end_time: The end time (set based on timeout supplied for the operator) for the SQL statement execution.
    :param polling_period_seconds: Controls the rate of the poll for the result of this run.
        By default, the trigger will poll every 30 seconds.
    :param retry_limit: The number of times to retry the connection in case of service outages.
    :param retry_delay: The number of seconds to wait between retries.
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param caller: The name of the operator that is calling the hook.
    """

    def __init__(
        self,
        statement_id: str,
        databricks_conn_id: str,
        end_time: float,
        polling_period_seconds: int = 30,
        retry_limit: int = 3,
        retry_delay: int = 10,
        retry_args: dict[Any, Any] | None = None,
        caller: str = "DatabricksSQLStatementExecutionTrigger",
    ) -> None:
        super().__init__()
        # Trigger kwargs cross Airflow's serialization boundary, so fail before storing invalid
        # trigger state or surfacing a generic serializer error without Databricks-specific guidance.
        validate_deferrable_databricks_retry_args(retry_args, owner=caller)
        self.statement_id = statement_id
        self.databricks_conn_id = databricks_conn_id
        self.end_time = end_time
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
        self.caller = caller
        self.hook = DatabricksHook(
            databricks_conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
            retry_args=retry_args,
            caller=caller,
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.databricks.triggers.databricks.DatabricksSQLStatementExecutionTrigger",
            {
                "statement_id": self.statement_id,
                "databricks_conn_id": self.databricks_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
                "end_time": self.end_time,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "retry_args": self.retry_args,
                "caller": self.caller,
            },
        )

    async def on_kill(self) -> None:
        """Cancel the Databricks SQL statement when the trigger is cancelled by a user action."""
        if self.statement_id:
            from asgiref.sync import sync_to_async

            self.log.info("Cancelling Databricks SQL statement %s.", self.statement_id)
            await sync_to_async(self.hook.cancel_sql_statement)(self.statement_id)

    async def run(self):
        async with self.hook:
            while self.end_time > time.time():
                statement_state = await self.hook.a_get_sql_statement_state(self.statement_id)
                if not statement_state.is_terminal:
                    self.log.info(
                        "Statement ID %s is in state %s. sleeping for %s seconds",
                        self.statement_id,
                        statement_state,
                        self.polling_period_seconds,
                    )
                    await asyncio.sleep(self.polling_period_seconds)
                    continue

                error = {}
                if statement_state.error_code:
                    error = {
                        "error_code": statement_state.error_code,
                        "error_message": statement_state.error_message,
                    }
                yield TriggerEvent(
                    {
                        "statement_id": self.statement_id,
                        "state": statement_state.to_json(),
                        "error": error,
                    }
                )
                return

            # If we reach here, it means the statement should be timed out as per the end_time.
            self.hook.cancel_sql_statement(self.statement_id)
            yield TriggerEvent(
                {
                    "statement_id": self.statement_id,
                    "state": statement_state.to_json(),
                    "error": {
                        "error_code": "TIMEOUT",
                        "error_message": f"Statement ID {self.statement_id} timed out after set end time {self.end_time}",
                    },
                }
            )
            return
