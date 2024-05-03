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

from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.triggers.databricks import DatabricksExecutionTrigger
from airflow.triggers.base import TriggerEvent
from airflow.utils.session import provide_session

pytestmark = pytest.mark.db_test


DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
LOGIN = "login"
PASSWORD = "password"
POLLING_INTERVAL_SECONDS = 30
RETRY_DELAY = 10
RETRY_LIMIT = 3
RUN_ID = 1
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
    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.login = LOGIN
        conn.password = PASSWORD
        conn.extra = None
        session.commit()

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
