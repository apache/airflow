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
import time_machine
from tenacity import stop_after_attempt, wait_incrementing

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
STATEMENT_END_TIME = 9999999999.0
TASK_RUN_ID1 = 11
TASK_RUN_ID1_KEY = "first_task"
TASK_RUN_ID2 = 22
TASK_RUN_ID2_KEY = "second_task"
TASK_RUN_ID3 = 33
TASK_RUN_ID3_KEY = "third_task"
JOB_ID = 42
RUN_PAGE_URL = "https://XX.cloud.databricks.com/#jobs/1/runs/1"
CALLER = "DatabricksSubmitRunOperator"
ERROR_MESSAGE = "error message from databricks API"
GET_RUN_OUTPUT_RESPONSE = {"metadata": {}, "error": ERROR_MESSAGE, "notebook_output": {}}
INVALID_RETRY_ARGS_PATTERN = (
    "does not support non-serializable retry_args/databricks_retry_args when deferrable=True"
)
UNSUPPORTED_RETRY_ARGS = [
    pytest.param({"wait": wait_incrementing(start=1, increment=1, max=3)}, id="wait_incrementing"),
    pytest.param({"stop": stop_after_attempt(3)}, id="stop_after_attempt"),
]

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

TRIGGER_INIT_CASES = [
    pytest.param(
        DatabricksExecutionTrigger,
        {
            "run_id": RUN_ID,
            "databricks_conn_id": DEFAULT_CONN_ID,
        },
        id="execution_trigger",
    ),
    pytest.param(
        DatabricksSQLStatementExecutionTrigger,
        {
            "statement_id": STATEMENT_ID,
            "databricks_conn_id": DEFAULT_CONN_ID,
            "end_time": 1234567890.0,
        },
        id="sql_statement_trigger",
    ),
]


@pytest.mark.parametrize("retry_args", UNSUPPORTED_RETRY_ARGS)
@pytest.mark.parametrize(("trigger_cls", "trigger_kwargs"), TRIGGER_INIT_CASES)
def test_trigger_init_rejects_non_serializable_retry_args(trigger_cls, trigger_kwargs, retry_args):
    with pytest.raises(ValueError, match=INVALID_RETRY_ARGS_PATTERN):
        trigger_cls(**trigger_kwargs, retry_args=retry_args)


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
                "caller": "DatabricksExecutionTrigger",
            },
        )

    def test_serialize_round_trip_caller(self):
        trigger = DatabricksExecutionTrigger(
            run_id=RUN_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            caller=CALLER,
        )
        _, kwargs = trigger.serialize()
        restored = DatabricksExecutionTrigger(**kwargs)
        assert restored.caller == CALLER

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
        self.end_time = STATEMENT_END_TIME
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
                "caller": "DatabricksSQLStatementExecutionTrigger",
            },
        )

    def test_serialize_round_trip_caller(self):
        trigger = DatabricksSQLStatementExecutionTrigger(
            statement_id=STATEMENT_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            end_time=self.end_time,
            caller=CALLER,
        )
        _, kwargs = trigger.serialize()
        restored = DatabricksSQLStatementExecutionTrigger(**kwargs)
        assert restored.caller == CALLER

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
        workflow_repair_attempts: int = 2,
        repair_attempts: int = 0,
        latest_repair_id: int | None = None,
        workflow_repair_timeout: int = 300,
    ) -> DatabricksWorkflowRepairCoordinatorTrigger:
        return DatabricksWorkflowRepairCoordinatorTrigger(
            run_id=RUN_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            workflow_repair_attempts=workflow_repair_attempts,
            repair_attempts=repair_attempts,
            latest_repair_id=latest_repair_id,
            polling_period_seconds=POLLING_INTERVAL_SECONDS,
            workflow_repair_timeout=workflow_repair_timeout,
            run_page_url=RUN_PAGE_URL,
        )

    def test_serialize_round_trips_state(self):
        trigger = self._make_trigger(workflow_repair_attempts=3, repair_attempts=1, latest_repair_id=42)

        path, kwargs = trigger.serialize()
        restored = DatabricksWorkflowRepairCoordinatorTrigger(**kwargs)

        assert (
            path
            == "airflow.providers.databricks.triggers.databricks.DatabricksWorkflowRepairCoordinatorTrigger"
        )
        assert restored.serialize() == (path, kwargs)

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

        trigger = self._make_trigger(workflow_repair_attempts=2, repair_attempts=0, latest_repair_id=None)
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "completed"
        assert events[0].payload["run_id"] == RUN_ID
        assert events[0].payload["repair_attempts"] == 0
        assert events[0].payload["latest_repair_id"] is None
        assert events[0].payload["errors"] == []

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.triggers.databricks.asyncio.sleep")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.repair_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_first_failure_within_budget_calls_repair_and_emits_repaired(
        self, mock_get_run_state, mock_get_run, mock_get_run_output, mock_repair_run, mock_sleep
    ):
        # Outer loop: terminal+failed → trigger issues repair.
        # Reflection poll: run is still terminal, but repair_id is in repair_history → reflected →
        # reflection loop breaks. This is the fast-repair case that a terminal-state-only check
        # would miss.
        mock_get_run_state.side_effect = [
            RunState(
                life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
                state_message="",
                result_state="FAILED",
            ),
        ]
        mock_get_run.side_effect = [
            GET_RUN_RESPONSE_TERMINATED_WITH_FAILED,
            {**GET_RUN_RESPONSE_TERMINATED_WITH_FAILED, "repair_history": [{"id": 101}]},
        ]
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE
        mock_repair_run.return_value = 101

        trigger = self._make_trigger(workflow_repair_attempts=2, repair_attempts=0, latest_repair_id=None)
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
        # Grace loop slept once before observing non-terminal state.
        assert mock_sleep.call_count == 1

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.triggers.databricks.asyncio.sleep")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.repair_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run_state")
    async def test_emits_repair_not_reflected_when_reflection_timeout_elapses(
        self,
        mock_get_run_state,
        mock_get_run,
        mock_get_run_output,
        mock_repair_run,
        mock_sleep,
    ):
        # terminal+failed → repair; reflection poll still terminal with no repair_history entry →
        # deadline trips → repair_not_reflected.
        # timeout=0 makes the deadline the run's end_time, already past, so the loop bails out.
        mock_get_run_state.side_effect = [
            RunState(
                life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
                state_message="",
                result_state="FAILED",
            ),
        ]
        mock_get_run.return_value = GET_RUN_RESPONSE_TERMINATED_WITH_FAILED
        mock_get_run_output.return_value = GET_RUN_OUTPUT_RESPONSE
        mock_repair_run.return_value = 101

        trigger = self._make_trigger(
            workflow_repair_attempts=2,
            repair_attempts=0,
            latest_repair_id=None,
            workflow_repair_timeout=0,
        )
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "repair_not_reflected"
        assert events[0].payload["run_id"] == RUN_ID
        assert events[0].payload["repair_attempts"] == 1
        assert events[0].payload["latest_repair_id"] == 101
        # Only the original repair_run — yielding must prevent a duplicate next cycle.
        mock_repair_run.assert_called_once()
        # One reflection-loop sleep fired before the deadline check tripped.
        assert mock_sleep.call_count == 1

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

        trigger = self._make_trigger(workflow_repair_attempts=2, repair_attempts=2, latest_repair_id=202)
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
    # Parent run terminal end_time (epoch ms) the waiter anchors its give-up deadline to.
    PARENT_END_TIME_MS = 1_700_000_000_000
    ANCHOR_S = PARENT_END_TIME_MS / 1000

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

    def _make_trigger(self, workflow_repair_timeout: int = 180) -> DatabricksWorkflowRepairWaitTrigger:
        return DatabricksWorkflowRepairWaitTrigger(
            run_id=self.PARENT_RUN_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            databricks_task_key=self.TASK_KEY,
            original_sub_run_id=self.ORIGINAL_SUB_RUN_ID,
            polling_period_seconds=POLLING_INTERVAL_SECONDS,
            workflow_repair_timeout=workflow_repair_timeout,
            run_page_url=RUN_PAGE_URL,
        )

    def _run_payload(
        self,
        result_state: str | None,
        life_cycle_state: str = LIFE_CYCLE_STATE_TERMINATED,
        tasks: list[dict] | None = None,
        end_time: int | None = None,
    ) -> dict:
        payload = {
            "run_page_url": RUN_PAGE_URL,
            "state": {
                "life_cycle_state": life_cycle_state,
                "state_message": None,
                "result_state": result_state,
            },
            "tasks": tasks or [],
        }
        if end_time is not None:
            payload["end_time"] = end_time
        return payload

    def test_serialize_round_trips_state(self):
        trigger = self._make_trigger()

        path, kwargs = trigger.serialize()
        restored = DatabricksWorkflowRepairWaitTrigger(**kwargs)

        assert path == "airflow.providers.databricks.triggers.databricks.DatabricksWorkflowRepairWaitTrigger"
        assert restored.serialize() == (path, kwargs)

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
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    async def test_emits_parent_failed_after_repair_timeout(self, mock_get_run):
        # Gives up only once the deadline (end_time + timeout) passes, not after a fixed poll
        # count. 180s timeout, 30s polling = 6 sleeps.
        mock_get_run.return_value = self._run_payload(
            result_state="FAILED",
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            tasks=[{"run_id": self.ORIGINAL_SUB_RUN_ID, "task_key": self.TASK_KEY, "start_time": 1000}],
            end_time=self.PARENT_END_TIME_MS,
        )

        with time_machine.travel(self.ANCHOR_S, tick=False) as traveller:

            def advance(_seconds):
                traveller.move_to(time.time() + POLLING_INTERVAL_SECONDS)

            with mock.patch("asyncio.sleep", side_effect=advance) as mock_sleep:
                trigger = self._make_trigger(workflow_repair_timeout=180)
                events = [event async for event in trigger.run()]

        # 6 sleeps advance the clock to the deadline; parent_failed fires on the 7th poll.
        assert mock_get_run.call_count == 7
        assert mock_sleep.call_count == 6
        assert len(events) == 1
        payload = events[0].payload
        assert payload["status"] == "parent_failed"
        assert payload["parent_run_id"] == self.PARENT_RUN_ID
        assert payload["databricks_task_key"] == self.TASK_KEY
        assert RunState.from_json(payload["parent_run_state"]).result_state == "FAILED"

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    async def test_parent_failed_fires_immediately_when_deadline_already_past(self, mock_get_run, mock_sleep):
        # Deadline is anchored to end_time, not to when polling started: a run that went terminal
        # long ago is already past its deadline, so the first poll fails without waiting.
        mock_get_run.return_value = self._run_payload(
            result_state="FAILED",
            life_cycle_state=LIFE_CYCLE_STATE_TERMINATED,
            tasks=[{"run_id": self.ORIGINAL_SUB_RUN_ID, "task_key": self.TASK_KEY, "start_time": 1000}],
            end_time=self.PARENT_END_TIME_MS,
        )

        with time_machine.travel(self.ANCHOR_S + 1000, tick=False):
            trigger = self._make_trigger(workflow_repair_timeout=180)
            events = [event async for event in trigger.run()]

        assert mock_get_run.call_count == 1
        assert mock_sleep.call_count == 0
        assert events[0].payload["status"] == "parent_failed"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.a_get_run")
    async def test_deadline_resets_when_run_leaves_terminal(self, mock_get_run):
        # When the repair takes effect the run leaves terminal, resetting the deadline, and the
        # waiter picks up the repaired attempt instead of failing.
        original_task = {"run_id": self.ORIGINAL_SUB_RUN_ID, "task_key": self.TASK_KEY, "start_time": 1000}
        new_task = {"run_id": self.NEW_SUB_RUN_ID, "task_key": self.TASK_KEY, "start_time": 2000}
        mock_get_run.side_effect = [
            self._run_payload(result_state="FAILED", tasks=[original_task], end_time=self.PARENT_END_TIME_MS),
            self._run_payload(result_state="FAILED", tasks=[original_task], end_time=self.PARENT_END_TIME_MS),
            # Repair lands: the run is RUNNING again → deadline resets.
            self._run_payload(result_state=None, life_cycle_state="RUNNING", tasks=[original_task]),
            # The repaired attempt for the watched task_key appears.
            self._run_payload(result_state=None, life_cycle_state="RUNNING", tasks=[original_task, new_task]),
        ]

        with time_machine.travel(self.ANCHOR_S, tick=False) as traveller:

            def advance(_seconds):
                traveller.move_to(time.time() + POLLING_INTERVAL_SECONDS)

            with mock.patch("asyncio.sleep", side_effect=advance):
                trigger = self._make_trigger(workflow_repair_timeout=180)
                events = [event async for event in trigger.run()]

        assert mock_get_run.call_count == 4
        assert len(events) == 1
        assert events[0].payload["status"] == "new_attempt"
        assert events[0].payload["new_sub_run_id"] == self.NEW_SUB_RUN_ID
