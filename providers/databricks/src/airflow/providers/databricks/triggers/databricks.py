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

from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunState
from airflow.providers.databricks.utils.databricks import (
    build_repair_run_json,
    extract_failed_task_errors_async,
    find_new_workflow_task_attempt,
)
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
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
        self.run_page_url = run_page_url
        self.repair_run = repair_run
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
            },
        )

    async def on_kill(self) -> None:
        """Cancel the Databricks run when the trigger is cancelled by a user action."""
        if self.run_id:
            from asgiref.sync import sync_to_async

            self.log.info("Cancelling Databricks run %s.", self.run_id)
            await sync_to_async(self.hook.cancel_run)(self.run_id)

    async def run(self):
        async with self.hook:
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
                        "run_start_time": run_info.get("start_time"),
                        "repair_run": self.repair_run,
                        "errors": failed_tasks,
                    }
                )
                return


class DatabricksWorkflowRepairCoordinatorTrigger(BaseTrigger):
    """
    Coordinate whole-run polling and ``rerun_all_failed_tasks`` repair for a Databricks Workflow run.

    Owned by the ``coordinator`` sibling task that
    :class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`
    injects when ``max_full_run_repairs > 0`` on Airflow 3+. Keeps a single Databricks job run alive across
    repair attempts so the same job cluster is reused. Each defer/resume cycle of the coordinator
    task corresponds to one iteration:

    1. Poll the run until it reaches a terminal state.
    2. On terminal success, yield ``status="completed"``.
    3. On terminal failure with repair budget remaining, call
       :meth:`~airflow.providers.databricks.hooks.databricks.DatabricksHook.repair_run` with
       ``rerun_all_failed_tasks=True`` and yield ``status="repaired"`` along with the new
       ``latest_repair_id`` and bumped ``repair_attempts``; the coordinator task then re-defers on
       a fresh trigger instance with the new state. Downstream task monitors observe the new sub-run
       attempt by polling the Databricks API directly (via
       :class:`DatabricksWorkflowRepairWaitTrigger`), not via any inter-task XCom.
    4. On terminal failure with the budget exhausted, yield ``status="failed"``.

    The Databricks ``run_id`` is stable across repair attempts; only ``latest_repair_id`` changes.

    :param run_id: The Databricks run id to coordinate.
    :param databricks_conn_id: Airflow connection id for the Databricks hook.
    :param max_full_run_repairs: Total repair attempts allowed for this run.
    :param repair_attempts: Repair attempts already performed (defaults to 0 on the first defer).
    :param latest_repair_id: Repair id of the most recent repair attempt, or ``None`` on the first
        defer. Forwarded to ``repair_run`` so Databricks knows which attempt is the latest.
    :param polling_period_seconds: How often to poll the run state.
    :param retry_limit: Hook retry limit for transient Databricks API failures.
    :param retry_delay: Hook retry delay (seconds).
    :param retry_args: Optional tenacity ``Retrying`` kwargs forwarded to the hook.
    :param run_page_url: The Databricks UI URL for this run, surfaced in events for logging.
    :param caller: Caller label forwarded to the hook for diagnostics.
    """

    def __init__(
        self,
        run_id: int,
        databricks_conn_id: str,
        max_full_run_repairs: int,
        repair_attempts: int = 0,
        latest_repair_id: int | None = None,
        polling_period_seconds: int = 30,
        retry_limit: int = 3,
        retry_delay: int = 10,
        retry_args: dict[Any, Any] | None = None,
        run_page_url: str | None = None,
        caller: str = "DatabricksWorkflowRepairCoordinatorTrigger",
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.max_full_run_repairs = max_full_run_repairs
        self.repair_attempts = repair_attempts
        self.latest_repair_id = latest_repair_id
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
        self.run_page_url = run_page_url
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
            "airflow.providers.databricks.triggers.databricks.DatabricksWorkflowRepairCoordinatorTrigger",
            {
                "run_id": self.run_id,
                "databricks_conn_id": self.databricks_conn_id,
                "max_full_run_repairs": self.max_full_run_repairs,
                "repair_attempts": self.repair_attempts,
                "latest_repair_id": self.latest_repair_id,
                "polling_period_seconds": self.polling_period_seconds,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "retry_args": self.retry_args,
                "run_page_url": self.run_page_url,
                "caller": self.caller,
            },
        )

    async def on_kill(self) -> None:
        """Cancel the Databricks run when the trigger is cancelled by a user action."""
        if self.run_id:
            from asgiref.sync import sync_to_async

            self.log.info("Cancelling Databricks run %s.", self.run_id)
            await sync_to_async(self.hook.cancel_run)(self.run_id)

    async def run(self):
        from asgiref.sync import sync_to_async

        async with self.hook:
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
                errors = await extract_failed_task_errors_async(self.hook, run_info, run_state)

                if run_state.is_successful:
                    self.log.info("Databricks run %s completed successfully.", self.run_id)
                    yield TriggerEvent(
                        {
                            "status": "completed",
                            "run_id": self.run_id,
                            "run_page_url": self.run_page_url,
                            "run_state": run_state.to_json(),
                            "repair_attempts": self.repair_attempts,
                            "latest_repair_id": self.latest_repair_id,
                            "errors": errors,
                        }
                    )
                    return

                if self.repair_attempts >= self.max_full_run_repairs:
                    self.log.info(
                        "Databricks run %s reached terminal failure state %s and repair budget "
                        "is exhausted (max_full_run_repairs=%s).",
                        self.run_id,
                        run_state.result_state,
                        self.max_full_run_repairs,
                    )
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "run_id": self.run_id,
                            "run_page_url": self.run_page_url,
                            "run_state": run_state.to_json(),
                            "repair_attempts": self.repair_attempts,
                            "latest_repair_id": self.latest_repair_id,
                            "errors": errors,
                        }
                    )
                    return

                self.log.info(
                    "Databricks run %s reached terminal failure state %s. Repairing all failed "
                    "tasks (attempt %s of %s, latest_repair_id=%s).",
                    self.run_id,
                    run_state.result_state,
                    self.repair_attempts + 1,
                    self.max_full_run_repairs,
                    self.latest_repair_id,
                )

                repair_json = build_repair_run_json(
                    run_id=self.run_id,
                    latest_repair_id=self.latest_repair_id,
                    overriding_parameters=run_info.get("overriding_parameters"),
                )

                new_repair_id = await sync_to_async(self.hook.repair_run)(repair_json)
                self.log.info(
                    "Databricks repair_run accepted for run %s; new repair_id=%s.",
                    self.run_id,
                    new_repair_id,
                )

                yield TriggerEvent(
                    {
                        "status": "repaired",
                        "run_id": self.run_id,
                        "run_page_url": self.run_page_url,
                        "run_state": run_state.to_json(),
                        "repair_attempts": self.repair_attempts + 1,
                        "latest_repair_id": new_repair_id,
                        "errors": errors,
                    }
                )
                return


class DatabricksWorkflowRepairWaitTrigger(BaseTrigger):
    """
    Wait for the next attempt of a single Databricks Workflow task after its sub-run failed.

    Used by Databricks task monitors inside a
    :class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`
    when ``max_full_run_repairs > 0`` on Airflow 3+. A monitor whose sub-run reaches terminal failure defers
    on this trigger; the trigger polls the parent run's task list and yields when a new attempt of
    the same ``task_key`` appears (issued by the sibling ``coordinator`` task via
    ``rerun_all_failed_tasks``), so the monitor can then defer on a fresh
    :class:`DatabricksExecutionTrigger` watching the new sub-run id.

    Each poll cycle:

    1. If a Databricks task with our ``databricks_task_key`` exists whose ``run_id`` differs from
       ``original_sub_run_id`` and whose ``start_time`` is newer, yield ``status="new_attempt"``
       with the new sub-run id.
    2. Otherwise, if the parent run is in a terminal failure state, count one "grace" observation.
       After ``terminal_grace_polls`` consecutive terminal observations without a new attempt,
       yield ``status="parent_failed"``. This avoids racing the coordinator: the parent run is
       briefly terminal between sub-run failure and the coordinator issuing ``repair_run``.
    3. Otherwise (parent still running, or terminal but inside the grace window), sleep and poll
       again.

    :param run_id: Parent workflow run id (stable across repairs).
    :param databricks_conn_id: Airflow connection id for the Databricks hook.
    :param databricks_task_key: The ``task_key`` of the Databricks task to watch for a new attempt.
    :param original_sub_run_id: The sub-run id of the attempt that just failed; the trigger only
        yields ``new_attempt`` for a sub-run id different from this one.
    :param polling_period_seconds: How often to poll the parent run.
    :param terminal_grace_polls: Number of consecutive terminal-with-no-new-attempt observations
        required before yielding ``status="parent_failed"``. Bounds how long we wait for the
        coordinator to issue a repair after observing terminal failure.
    :param retry_limit: Hook retry limit for transient Databricks API failures.
    :param retry_delay: Hook retry delay (seconds).
    :param retry_args: Optional tenacity ``Retrying`` kwargs forwarded to the hook.
    :param run_page_url: The Databricks UI URL for the parent run, surfaced in events for logging.
    :param caller: Caller label forwarded to the hook for diagnostics.
    """

    def __init__(
        self,
        run_id: int,
        databricks_conn_id: str,
        databricks_task_key: str,
        original_sub_run_id: int,
        original_start_time: int | None = None,
        polling_period_seconds: int = 30,
        terminal_grace_polls: int = 3,
        retry_limit: int = 3,
        retry_delay: int = 10,
        retry_args: dict[Any, Any] | None = None,
        run_page_url: str | None = None,
        caller: str = "DatabricksWorkflowRepairWaitTrigger",
    ) -> None:
        super().__init__()
        if terminal_grace_polls < 1:
            raise ValueError(f"terminal_grace_polls must be >= 1, got {terminal_grace_polls}")
        self.run_id = run_id
        self.databricks_conn_id = databricks_conn_id
        self.databricks_task_key = databricks_task_key
        self.original_sub_run_id = original_sub_run_id
        self.original_start_time = original_start_time
        self.polling_period_seconds = polling_period_seconds
        self.terminal_grace_polls = terminal_grace_polls
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
        self.run_page_url = run_page_url
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
            "airflow.providers.databricks.triggers.databricks.DatabricksWorkflowRepairWaitTrigger",
            {
                "run_id": self.run_id,
                "databricks_conn_id": self.databricks_conn_id,
                "databricks_task_key": self.databricks_task_key,
                "original_sub_run_id": self.original_sub_run_id,
                "original_start_time": self.original_start_time,
                "polling_period_seconds": self.polling_period_seconds,
                "terminal_grace_polls": self.terminal_grace_polls,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "retry_args": self.retry_args,
                "run_page_url": self.run_page_url,
                "caller": self.caller,
            },
        )

    def _find_new_attempt(self, tasks: list[dict[str, Any]]) -> dict[str, Any] | None:
        return find_new_workflow_task_attempt(
            tasks=tasks,
            task_key=self.databricks_task_key,
            original_sub_run_id=self.original_sub_run_id,
            original_start_time=self.original_start_time,
        )

    async def run(self):
        terminal_observations = 0
        async with self.hook:
            while True:
                run_info = await self.hook.a_get_run(self.run_id)
                run_state = RunState(**run_info["state"])
                tasks = run_info.get("tasks", [])

                new_attempt = self._find_new_attempt(tasks)
                if new_attempt is not None:
                    self.log.info(
                        "Databricks workflow run %s produced a new attempt for task_key %s "
                        "(new sub-run id %s).",
                        self.run_id,
                        self.databricks_task_key,
                        new_attempt["run_id"],
                    )
                    yield TriggerEvent(
                        {
                            "status": "new_attempt",
                            "parent_run_id": self.run_id,
                            "databricks_task_key": self.databricks_task_key,
                            "new_sub_run_id": new_attempt["run_id"],
                            "run_page_url": self.run_page_url,
                        }
                    )
                    return

                if run_state.is_terminal and not run_state.is_successful:
                    terminal_observations += 1
                    self.log.info(
                        "Databricks workflow run %s is in terminal failure state %s with no new "
                        "attempt for task_key %s (grace %s of %s).",
                        self.run_id,
                        run_state.result_state,
                        self.databricks_task_key,
                        terminal_observations,
                        self.terminal_grace_polls,
                    )
                    if terminal_observations >= self.terminal_grace_polls:
                        yield TriggerEvent(
                            {
                                "status": "parent_failed",
                                "parent_run_id": self.run_id,
                                "databricks_task_key": self.databricks_task_key,
                                "parent_run_state": run_state.to_json(),
                                "run_page_url": self.run_page_url,
                            }
                        )
                        return
                else:
                    terminal_observations = 0

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
        self.statement_id = statement_id
        self.databricks_conn_id = databricks_conn_id
        self.end_time = end_time
        self.polling_period_seconds = polling_period_seconds
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.retry_args = retry_args
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
