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

import time
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import RunState, SQLStatementState
from airflow.providers.databricks.triggers.databricks import (
    DatabricksExecutionTrigger,
    DatabricksSQLStatementExecutionTrigger,
    DatabricksWorkflowRepairCoordinatorTrigger,
    DatabricksWorkflowRepairWaitTrigger,
)
from airflow.triggers.base import TriggerEvent

pytestmark = pytest.mark.db_test


DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
LOGIN = "login"
PASSWORD = "password"
POLLING_INTERVAL_SECONDS = 30
RETRY_DELAY = 10
RETRY_LIMIT = 3
RUN_ID = 1
STATEMENT_ID = "statement_id"
TASK_RUN_ID1 = 11
TASK_RUN_ID1_KEY = "first_task"
TASK_RUN_ID2 = 22
TASK_RUN_ID2_KEY = "second_task"
TASK_RUN_ID3 = 33
TASK_RUN_ID3_KEY = "third_task"
JOB_ID = 42
RUN_PAGE_URL = "https://XX.cloud.databricks.com/#jobs/1/runs/1"
ERROR_MESSAGE = "error message from databricks API"
GET_RUN_OUTPUT_RESPONSE = {"metadata": {}, "error": ERROR_MESSAGE, "notebook_output": {}}

RUN_LIFE_CYCLE_STATES = ["PENDING", "RUNNING", "TERMINATING", "TERMINATED", "SKIPPED", "INTERNAL_ERROR"]

LIFE_CYCLE_STATE_PENDING = "PENDING"
LIFE_CYCLE_STATE_TERMINATED = "TERMINATED"
LIFE_CYCLE_STATE_INTERNAL_ERROR = "INTERNAL_ERROR"

STATE_MESSAGE = "Waiting for cluster"

GET_RUN_RESPONSE_PENDING = {
    "job_id": JOB_ID,
    "run_page_url": RUN_PAGE_URL,
    "state": {
        "life_cycle_state": LIFE_CYCLE_STATE_PENDING,
        "state_message": STATE_MESSAGE,
        "result_state": None,
    },
}
GET_RUN_RESPONSE_TERMINATED = {
    "job_id": JOB_ID,
    "run_page_url": RUN_PAGE_URL,
    "state": {
        "life_cycle_state": LIFE_CYCLE_STATE_TERMINATED,
        "state_message": None,
        "result_state": "SUCCESS",
    },
}
GET_RUN_RESPONSE_TERMINATED_WITH_FAILED = {
    "job_id": JOB_ID,
    "run_page_url": RUN_PAGE_URL,
    "state": {
        "life_cycle_state": LIFE_CYCLE_STATE_INTERNAL_ERROR,
        "state_message": None,
        "result_state": "FAILED",
    },
    "tasks": [
        {
            "run_id": TASK_RUN_ID1,
            "task_key": TASK_RUN_ID1_KEY,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Workload failed, see run output for details",
            },
        },
        {
            "run_id": TASK_RUN_ID2,
            "task_key": TASK_RUN_ID2_KEY,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": None,
            },
        },
        {
            "run_id": TASK_RUN_ID3,
            "task_key": TASK_RUN_ID3_KEY,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Workload failed, see run output for details",
            },
        },
    ],
}


class TestDatabricksExecutionTrigger:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=DEFAULT_CONN_ID,
                conn_type="databricks",
                host=HOST,
                login=LOGIN,
                password=PASSWORD,
                extra=None,
            )
        )

        self.trigger = DatabricksExecutionTrigger(
            run_id=RUN_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            polling_period_seconds=POLLING_INTERVAL_SECONDS,
            run_page_url=RUN_PAGE_URL,
        )

    def test_serialize(self):
        assert self.trigger.serialize() == (
            "airflow.providers.databricks.triggers.databricks.DatabricksExecutionTrigger",
            {
                "run_id": RUN_ID,
                "databricks_conn_id": DEFAULT_CONN_ID,
                "polling_period_seconds": POLLING_INTERVAL_SECONDS,
                "retry_delay": 10,
                "retry_limit": 3,
                "retry_args": None,
                "run_page_url": RUN_PAGE_URL,
                "repair_run": False,
            },
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_page_url")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_run_return_success(
        self, mock_get_run_state, mock_get_run_page_url, mock_get_run, mock_get_run_output
    ):
        mock_get_run_page_url.return_value = RUN_PAGE_URL
        mock_get_run_state.return_value = RunState(
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            state_message="",
            result_state="SUCCESS",
        )
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE

        trigger_event = self.trigger.run()
        async for event in trigger_event:
            assert event == TriggerEvent(
                {
                    "run_id": RUN_ID,
                    "run_state": RunState(
                        life_cycle_state=LIFE_CYCLE_STATE_TERMINATED, state_message="", result_state="SUCCESS"
                    ).to_json(),
                    "run_page_url": RUN_PAGE_URL,
                    "run_start_time": None,
                    "repair_run": False,
                    "errors": [],
                }
            )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_page_url")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_run_return_failure(
        self, mock_get_run_state, mock_get_run_page_url, mock_get_run, mock_get_run_output
    ):
        mock_get_run_page_url.return_value = RUN_PAGE_URL
        mock_get_run_state.return_value = RunState(
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            state_message="",
            result_state="FAILED",
        )
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED_WITH_FAILED

        trigger_event = self.trigger.run()
        async for event in trigger_event:
            assert event == TriggerEvent(
                {
                    "run_id": RUN_ID,
                    "run_state": RunState(
                        life_cycle_state=LIFE_CYCLE_STATE_TERMINATED, state_message="", result_state="FAILED"
                    ).to_json(),
                    "run_page_url": RUN_PAGE_URL,
                    "run_start_time": None,
                    "repair_run": False,
                    "errors": [
                        {"task_key": TASK_RUN_ID1_KEY, "run_id": TASK_RUN_ID1, "error": ERROR_MESSAGE},
                        {"task_key": TASK_RUN_ID3_KEY, "run_id": TASK_RUN_ID3, "error": ERROR_MESSAGE},
                    ],
                }
            )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.triggers.databricks.asyncio.sleep")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_sleep_between_retries(
        self, mock_get_run_state, mock_sleep, mock_get_run, mock_get_run_output
    ):
        mock_get_run_state.side_effect = [
            RunState(
                life_cycle_state=LIFE_CYCLE_STATE_PENDING,
                state_message="",
                result_state="",
            ),
            RunState(
                life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
                state_message="",
                result_state="SUCCESS",
            ),
        ]
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE

        trigger_event = self.trigger.run()
        async for event in trigger_event:
            assert event == TriggerEvent(
                {
                    "run_id": RUN_ID,
                    "run_state": RunState(
                        life_cycle_state=LIFE_CYCLE_STATE_TERMINATED, state_message="", result_state="SUCCESS"
                    ).to_json(),
                    "run_page_url": RUN_PAGE_URL,
                    "run_start_time": None,
                    "repair_run": False,
                    "errors": [],
                }
            )
        mock_sleep.assert_called_once()
        mock_sleep.assert_called_with(POLLING_INTERVAL_SECONDS)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.cancel_run")
    async def test_on_kill_cancels_run(self, mock_cancel_run):
        await self.trigger.on_kill()
        mock_cancel_run.assert_called_once_with(RUN_ID)


class TestDatabricksSQLStatementExecutionTrigger:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        self.end_time = time.time() + 60
        create_connection_without_db(
            Connection(
                conn_id=DEFAULT_CONN_ID,
                conn_type="databricks",
                host=HOST,
                login=LOGIN,
                password=PASSWORD,
                extra=None,
            )
        )

        self.trigger = DatabricksSQLStatementExecutionTrigger(
            statement_id=STATEMENT_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            polling_period_seconds=POLLING_INTERVAL_SECONDS,
            end_time=self.end_time,
        )

    def test_serialize(self):
        assert self.trigger.serialize() == (
            "airflow.providers.databricks.triggers.databricks.DatabricksSQLStatementExecutionTrigger",
            {
                "statement_id": STATEMENT_ID,
                "databricks_conn_id": DEFAULT_CONN_ID,
                "end_time": self.end_time,
                "polling_period_seconds": POLLING_INTERVAL_SECONDS,
                "retry_delay": 10,
                "retry_limit": 3,
                "retry_args": None,
            },
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_sql_statement_state")
    async def test_run_return_success(self, mock_a_get_sql_statement_state):
        mock_a_get_sql_statement_state.return_value = SQLStatementState(state="SUCCEEDED")

        trigger_event = self.trigger.run()
        async for event in trigger_event:
            assert event == TriggerEvent(
                {
                    "statement_id": STATEMENT_ID,
                    "state": SQLStatementState(state="SUCCEEDED").to_json(),
                    "error": {},
                }
            )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_sql_statement_state")
    async def test_run_return_failure(self, mock_a_get_sql_statement_state):
        mock_a_get_sql_statement_state.return_value = SQLStatementState(
            state="FAILED",
            error_code="500",
            error_message="Something went wrong",
        )

        trigger_event = self.trigger.run()
        async for event in trigger_event:
            assert event == TriggerEvent(
                {
                    "statement_id": STATEMENT_ID,
                    "state": SQLStatementState(
                        state="FAILED",
                        error_code="500",
                        error_message="Something went wrong",
                    ).to_json(),
                    "error": {
                        "error_code": "500",
                        "error_message": "Something went wrong",
                    },
                }
            )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.triggers.databricks.asyncio.sleep")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_sql_statement_state")
    async def test_sleep_between_retries(self, mock_a_get_sql_statement_state, mock_sleep):
        mock_a_get_sql_statement_state.side_effect = [
            SQLStatementState(
                state="PENDING",
            ),
            SQLStatementState(
                state="SUCCEEDED",
            ),
        ]

        trigger_event = self.trigger.run()
        async for event in trigger_event:
            assert event == TriggerEvent(
                {
                    "statement_id": STATEMENT_ID,
                    "state": SQLStatementState(state="SUCCEEDED").to_json(),
                    "error": {},
                }
            )
        mock_sleep.assert_called_once()
        mock_sleep.assert_called_with(POLLING_INTERVAL_SECONDS)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.cancel_sql_statement")
    async def test_on_kill_cancels_statement(self, mock_cancel_sql_statement):
        await self.trigger.on_kill()
        mock_cancel_sql_statement.assert_called_once_with(STATEMENT_ID)


class TestDatabricksWorkflowRepairCoordinatorTrigger:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=DEFAULT_CONN_ID,
                conn_type="databricks",
                host=HOST,
                login=LOGIN,
                password=PASSWORD,
                extra=None,
            )
        )

    def _make_trigger(
        self,
        max_full_run_repairs: int = 2,
        repair_attempts: int = 0,
        latest_repair_id: int | None = None,
    ) -> DatabricksWorkflowRepairCoordinatorTrigger:
        return DatabricksWorkflowRepairCoordinatorTrigger(
            run_id=RUN_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            max_full_run_repairs=max_full_run_repairs,
            repair_attempts=repair_attempts,
            latest_repair_id=latest_repair_id,
            polling_period_seconds=POLLING_INTERVAL_SECONDS,
            run_page_url=RUN_PAGE_URL,
        )

    def test_serialize_round_trips_state(self):
        trigger = self._make_trigger(max_full_run_repairs=3, repair_attempts=1, latest_repair_id=42)
        path, kwargs = trigger.serialize()

        assert (
            path
            == "airflow.providers.databricks.triggers.databricks.DatabricksWorkflowRepairCoordinatorTrigger"
        )
        assert kwargs == {
            "run_id": RUN_ID,
            "databricks_conn_id": DEFAULT_CONN_ID,
            "max_full_run_repairs": 3,
            "repair_attempts": 1,
            "latest_repair_id": 42,
            "polling_period_seconds": POLLING_INTERVAL_SECONDS,
            "retry_limit": RETRY_LIMIT,
            "retry_delay": RETRY_DELAY,
            "retry_args": None,
            "run_page_url": RUN_PAGE_URL,
            "caller": "DatabricksWorkflowRepairCoordinatorTrigger",
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_emits_completed_when_run_succeeds(self, mock_get_run_state, mock_get_run):
        mock_get_run_state.return_value = RunState(
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            state_message="",
            result_state="SUCCESS",
        )
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED

        trigger = self._make_trigger(max_full_run_repairs=2, repair_attempts=0, latest_repair_id=None)
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "completed"
        assert events[0].payload["run_id"] == RUN_ID
        assert events[0].payload["repair_attempts"] == 0
        assert events[0].payload["latest_repair_id"] is None
        assert events[0].payload["errors"] == []

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.repair_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_first_failure_within_budget_calls_repair_and_emits_repaired(
        self, mock_get_run_state, mock_get_run, mock_get_run_output, mock_repair_run
    ):
        mock_get_run_state.return_value = RunState(
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            state_message="",
            result_state="FAILED",
        )
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED_WITH_FAILED
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE
        mock_repair_run.return_value = 101

        trigger = self._make_trigger(max_full_run_repairs=2, repair_attempts=0, latest_repair_id=None)
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "repaired"
        assert events[0].payload["run_id"] == RUN_ID
        assert events[0].payload["repair_attempts"] == 1
        assert events[0].payload["latest_repair_id"] == 101
        assert events[0].payload["errors"] == [
            {"task_key": TASK_RUN_ID1_KEY, "run_id": TASK_RUN_ID1, "error": ERROR_MESSAGE},
            {"task_key": TASK_RUN_ID3_KEY, "run_id": TASK_RUN_ID3, "error": ERROR_MESSAGE},
        ]

        mock_repair_run.assert_called_once()
        repair_json = mock_repair_run.call_args.args[0]
        assert repair_json["run_id"] == RUN_ID
        assert repair_json["rerun_all_failed_tasks"] is True
        # First repair: latest_repair_id was None, so the field must be omitted from the payload
        assert "latest_repair_id" not in repair_json

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.repair_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_emits_failed_when_budget_exhausted(
        self, mock_get_run_state, mock_get_run, mock_get_run_output, mock_repair_run
    ):
        mock_get_run_state.return_value = RunState(
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            state_message="",
            result_state="FAILED",
        )
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED_WITH_FAILED
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE

        trigger = self._make_trigger(max_full_run_repairs=2, repair_attempts=2, latest_repair_id=202)
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "failed"
        assert events[0].payload["repair_attempts"] == 2
        assert events[0].payload["latest_repair_id"] == 202
        assert events[0].payload["errors"] == [
            {"task_key": TASK_RUN_ID1_KEY, "run_id": TASK_RUN_ID1, "error": ERROR_MESSAGE},
            {"task_key": TASK_RUN_ID3_KEY, "run_id": TASK_RUN_ID3, "error": ERROR_MESSAGE},
        ]
        mock_repair_run.assert_not_called()


class TestDatabricksWorkflowRepairWaitTrigger:
    PARENT_RUN_ID = 100
    TASK_KEY = "monitored_task"
    ORIGINAL_SUB_RUN_ID = 500
    NEW_SUB_RUN_ID = 700

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=DEFAULT_CONN_ID,
                conn_type="databricks",
                host=HOST,
                login=LOGIN,
                password=PASSWORD,
                extra=None,
            )
        )

    def _make_trigger(
        self,
        terminal_grace_polls: int = 3,
    ) -> DatabricksWorkflowRepairWaitTrigger:
        return DatabricksWorkflowRepairWaitTrigger(
            run_id=self.PARENT_RUN_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            databricks_task_key=self.TASK_KEY,
            original_sub_run_id=self.ORIGINAL_SUB_RUN_ID,
            polling_period_seconds=POLLING_INTERVAL_SECONDS,
            terminal_grace_polls=terminal_grace_polls,
            run_page_url=RUN_PAGE_URL,
        )

    def _run_payload(
        self,
        result_state: str | None,
        life_cycle_state: str = LIFE_CYCLE_STATE_TERMINATED,
        tasks: list[dict] | None = None,
    ) -> dict:
        return {
            "run_page_url": RUN_PAGE_URL,
            "state": {
                "life_cycle_state": life_cycle_state,
                "state_message": None,
                "result_state": result_state,
            },
            "tasks": tasks or [],
        }

    def test_serialize_round_trips_state(self):
        trigger = self._make_trigger(terminal_grace_polls=5)
        path, kwargs = trigger.serialize()

        assert path == "airflow.providers.databricks.triggers.databricks.DatabricksWorkflowRepairWaitTrigger"
        assert kwargs == {
            "run_id": self.PARENT_RUN_ID,
            "databricks_conn_id": DEFAULT_CONN_ID,
            "databricks_task_key": self.TASK_KEY,
            "original_sub_run_id": self.ORIGINAL_SUB_RUN_ID,
            "original_start_time": None,
            "polling_period_seconds": POLLING_INTERVAL_SECONDS,
            "terminal_grace_polls": 5,
            "retry_limit": RETRY_LIMIT,
            "retry_delay": RETRY_DELAY,
            "retry_args": None,
            "run_page_url": RUN_PAGE_URL,
            "caller": "DatabricksWorkflowRepairWaitTrigger",
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    async def test_emits_new_attempt_when_new_sub_run_appears(self, mock_get_run):
        mock_get_run.return_value = self._run_payload(
            result_state=None,
            life_cycle_state="RUNNING",
            tasks=[
                {
                    "run_id": self.ORIGINAL_SUB_RUN_ID,
                    "task_key": self.TASK_KEY,
                    "start_time": 1000,
                },
                {
                    "run_id": self.NEW_SUB_RUN_ID,
                    "task_key": self.TASK_KEY,
                    "start_time": 2000,
                },
            ],
        )

        trigger = self._make_trigger()
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload == {
            "status": "new_attempt",
            "parent_run_id": self.PARENT_RUN_ID,
            "databricks_task_key": self.TASK_KEY,
            "new_sub_run_id": self.NEW_SUB_RUN_ID,
            "run_page_url": RUN_PAGE_URL,
        }

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    async def test_emits_parent_failed_after_grace_polls(self, mock_get_run, mock_sleep):
        terminal_payload = self._run_payload(
            result_state="FAILED",
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            tasks=[
                {
                    "run_id": self.ORIGINAL_SUB_RUN_ID,
                    "task_key": self.TASK_KEY,
                    "start_time": 1000,
                },
            ],
        )
        mock_get_run.return_value = terminal_payload

        trigger = self._make_trigger(terminal_grace_polls=3)
        events = [event async for event in trigger.run()]

        assert mock_get_run.call_count == 3
        assert mock_sleep.call_count == 2
        assert len(events) == 1
        payload = events[0].payload
        assert payload["status"] == "parent_failed"
        assert payload["parent_run_id"] == self.PARENT_RUN_ID
        assert payload["databricks_task_key"] == self.TASK_KEY
        assert RunState.from_json(payload["parent_run_state"]).result_state == "FAILED"
