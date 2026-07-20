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

from unittest import mock

import pytest
from tenacity import stop_after_attempt, wait_incrementing

from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import RunState, SQLStatementState
from airflow.providers.databricks.triggers.databricks import (
    DatabricksExecutionTrigger,
    DatabricksSQLStatementExecutionTrigger,
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
