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
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.hooks.databricks import RunState
from airflow.providers.databricks.utils.databricks import (
    build_repair_run_json,
    extract_failed_task_errors,
    extract_failed_task_errors_async,
    find_new_workflow_task_attempt,
    normalise_json_content,
    validate_trigger_event,
)

RUN_ID = 1
RUN_PAGE_URL = "run-page-url"
ERROR_MESSAGE = "Exception: Something went wrong"
TASK_RUN_ID_1 = 101
TASK_RUN_ID_2 = 102
TASK_KEY_1 = "first_task"
TASK_KEY_2 = "second_task"


def mock_dict(d: dict):
    """Helper function to create a MagicMock that returns a dict"""
    m = MagicMock()
    m.return_value = d
    return m


class TestDatabricksOperatorSharedFunctions:
    def test_normalise_json_content(self):
        test_json = {
            "test_bool": True,
            "test_int": 1,
            "test_float": 1.0,
            "test_dict": {"key": "value"},
            "test_list": [1, 1.0, "a", "b"],
            "test_tuple": (1, 1.0, "a", "b"),
        }

        expected = {
            "test_bool": True,
            "test_int": "1",
            "test_float": "1.0",
            "test_dict": {"key": "value"},
            "test_list": ["1", "1.0", "a", "b"],
            "test_tuple": ["1", "1.0", "a", "b"],
        }
        assert normalise_json_content(test_json) == expected

    def test_normalise_json_content_with_xcom_arg(self):
        """XComArg values should be passed through since they resolve at runtime."""
        from airflow.providers.common.compat.sdk import DAG, BaseOperator

        with DAG(dag_id="test_dag"):
            op = BaseOperator(task_id="test_task")
            xcom_arg = op.output

        test_json = {"job_id": xcom_arg, "name": "test"}
        result = normalise_json_content(test_json)
        assert result["job_id"] is xcom_arg
        assert result["name"] == "test"

    def test_validate_trigger_event_success(self):
        event = {
            "run_id": RUN_ID,
            "run_page_url": RUN_PAGE_URL,
            "run_state": RunState("TERMINATED", "SUCCESS", "").to_json(),
            "errors": [],
        }
        assert validate_trigger_event(event) is None

    def test_validate_trigger_event_failure(self):
        event = {}
        with pytest.raises(AirflowException):
            validate_trigger_event(event)

    def test_find_new_workflow_task_attempt_picks_newest_matching_attempt(self):
        tasks = [
            {"run_id": TASK_RUN_ID_1, "task_key": TASK_KEY_1, "start_time": 1000},
            {"run_id": 201, "task_key": TASK_KEY_1, "start_time": 1500},
            {"run_id": 202, "task_key": TASK_KEY_1, "start_time": 2500},
            {"run_id": 203, "task_key": TASK_KEY_2, "start_time": 3000},
        ]

        result = find_new_workflow_task_attempt(
            tasks=tasks,
            task_key=TASK_KEY_1,
            original_sub_run_id=TASK_RUN_ID_1,
            original_start_time=1000,
        )

        assert result == {"run_id": 202, "task_key": TASK_KEY_1, "start_time": 2500}

    def test_build_repair_run_json_includes_optional_fields_only_when_present(self):
        assert build_repair_run_json(run_id=RUN_ID, latest_repair_id=None) == {
            "run_id": RUN_ID,
            "rerun_all_failed_tasks": True,
            "rerun_dependent_tasks": True,
        }

        assert build_repair_run_json(
            run_id=RUN_ID,
            latest_repair_id=42,
            overriding_parameters={"notebook_params": {"date": "2024-01-01"}},
        ) == {
            "run_id": RUN_ID,
            "rerun_all_failed_tasks": True,
            "rerun_dependent_tasks": True,
            "latest_repair_id": 42,
            "overriding_parameters": {"notebook_params": {"date": "2024-01-01"}},
        }


class TestExtractFailedTaskErrors:
    """Test cases for the extract_failed_task_errors utility function (synchronous version)"""

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    def test_extract_failed_task_errors_success_run(self, mock_hook_class):
        """Test that no errors are extracted for successful runs"""
        hook = mock_hook_class.return_value
        run_state = RunState("TERMINATED", "SUCCESS", "")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
            },
            "tasks": [],
        }

        result = extract_failed_task_errors(hook, run_info, run_state)

        assert result == []
        hook.get_run_output.assert_not_called()

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    def test_extract_failed_task_errors_failed_run_no_tasks(self, mock_hook_class):
        """Test that no errors are extracted for failed runs with no tasks"""
        hook = mock_hook_class.return_value
        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [],
        }

        result = extract_failed_task_errors(hook, run_info, run_state)

        assert result == []
        hook.get_run_output.assert_not_called()

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    def test_extract_failed_task_errors_single_failed_task_with_error_output(self, mock_hook_class):
        """Test extracting errors from a single failed task with error in run output"""
        hook = mock_hook_class.return_value
        hook.get_run_output = mock_dict({"error": ERROR_MESSAGE})

        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Task failed",
                    },
                }
            ],
        }

        result = extract_failed_task_errors(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_1,
                "run_id": TASK_RUN_ID_1,
                "error": ERROR_MESSAGE,
            }
        ]
        assert result == expected
        hook.get_run_output.assert_called_once_with(TASK_RUN_ID_1)

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    def test_extract_failed_task_errors_single_failed_task_without_error_output(self, mock_hook_class):
        """Test extracting errors from a single failed task without error in run output"""
        hook = mock_hook_class.return_value
        hook.get_run_output = mock_dict({})  # No error in output

        run_state = RunState("TERMINATED", "FAILED", "Job failed with general message")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed with general message",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Task failed",
                    },
                }
            ],
        }

        result = extract_failed_task_errors(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_1,
                "run_id": TASK_RUN_ID_1,
                "error": "Job failed with general message",  # Falls back to run state message
            }
        ]
        assert result == expected
        hook.get_run_output.assert_called_once_with(TASK_RUN_ID_1)

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    def test_extract_failed_task_errors_multiple_failed_tasks(self, mock_hook_class):
        """Test extracting errors from multiple failed tasks"""
        hook = mock_hook_class.return_value
        hook.get_run_output = mock_dict({"error": ERROR_MESSAGE})

        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "First task failed",
                    },
                },
                {
                    "run_id": TASK_RUN_ID_2,
                    "task_key": TASK_KEY_2,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Second task failed",
                    },
                },
            ],
        }

        result = extract_failed_task_errors(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_1,
                "run_id": TASK_RUN_ID_1,
                "error": ERROR_MESSAGE,
            },
            {
                "task_key": TASK_KEY_2,
                "run_id": TASK_RUN_ID_2,
                "error": ERROR_MESSAGE,
            },
        ]
        assert result == expected
        assert hook.get_run_output.call_count == 2

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    def test_extract_failed_task_errors_mixed_task_states(self, mock_hook_class):
        """Test extracting errors when some tasks succeed and some fail"""
        hook = mock_hook_class.return_value
        hook.get_run_output = mock_dict({"error": ERROR_MESSAGE})

        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",  # This task succeeded
                        "state_message": "Task completed",
                    },
                },
                {
                    "run_id": TASK_RUN_ID_2,
                    "task_key": TASK_KEY_2,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",  # This task failed
                        "state_message": "Task failed",
                    },
                },
            ],
        }

        result = extract_failed_task_errors(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_2,
                "run_id": TASK_RUN_ID_2,
                "error": ERROR_MESSAGE,
            }
        ]
        assert result == expected
        hook.get_run_output.assert_called_once_with(TASK_RUN_ID_2)


class TestExtractFailedTaskErrorsAsync:
    """Test cases for the extract_failed_task_errors_async utility function (asynchronous version)"""

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    @pytest.mark.asyncio
    async def test_extract_failed_task_errors_async_success_run(self, mock_hook_class):
        """Test that no errors are extracted for successful runs (async)"""
        hook = mock_hook_class.return_value
        hook.a_get_run_output = mock.AsyncMock()

        run_state = RunState("TERMINATED", "SUCCESS", "")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
            },
            "tasks": [],
        }

        result = await extract_failed_task_errors_async(hook, run_info, run_state)

        assert result == []
        hook.a_get_run_output.assert_not_called()

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    @pytest.mark.asyncio
    async def test_extract_failed_task_errors_async_single_failed_task(self, mock_hook_class):
        """Test extracting errors from a single failed task (async)"""
        hook = mock_hook_class.return_value
        hook.a_get_run_output = mock.AsyncMock(return_value={"error": ERROR_MESSAGE})

        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Task failed",
                    },
                }
            ],
        }

        result = await extract_failed_task_errors_async(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_1,
                "run_id": TASK_RUN_ID_1,
                "error": ERROR_MESSAGE,
            }
        ]
        assert result == expected
        hook.a_get_run_output.assert_called_once_with(TASK_RUN_ID_1)

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    @pytest.mark.asyncio
    async def test_extract_failed_task_errors_async_multiple_failed_tasks(self, mock_hook_class):
        """Test extracting errors from multiple failed tasks (async)"""
        hook = mock_hook_class.return_value
        hook.a_get_run_output = mock.AsyncMock(return_value={"error": ERROR_MESSAGE})

        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "First task failed",
                    },
                },
                {
                    "run_id": TASK_RUN_ID_2,
                    "task_key": TASK_KEY_2,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Second task failed",
                    },
                },
            ],
        }

        result = await extract_failed_task_errors_async(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_1,
                "run_id": TASK_RUN_ID_1,
                "error": ERROR_MESSAGE,
            },
            {
                "task_key": TASK_KEY_2,
                "run_id": TASK_RUN_ID_2,
                "error": ERROR_MESSAGE,
            },
        ]
        assert result == expected
        assert hook.a_get_run_output.call_count == 2

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    @pytest.mark.asyncio
    async def test_extract_failed_task_errors_async_fallback_to_state_message(self, mock_hook_class):
        """Test async function falls back to state message when no error in output"""
        hook = mock_hook_class.return_value
        hook.a_get_run_output = mock.AsyncMock(return_value={})  # No error in output

        run_state = RunState("TERMINATED", "FAILED", "General failure message")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "General failure message",
            },
            "tasks": [
                {
                    "run_id": TASK_RUN_ID_1,
                    "task_key": TASK_KEY_1,
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "Task failed",
                    },
                }
            ],
        }

        result = await extract_failed_task_errors_async(hook, run_info, run_state)

        expected = [
            {
                "task_key": TASK_KEY_1,
                "run_id": TASK_RUN_ID_1,
                "error": "General failure message",  # Falls back to run state message
            }
        ]
        assert result == expected
        hook.a_get_run_output.assert_called_once_with(TASK_RUN_ID_1)

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook")
    @pytest.mark.asyncio
    async def test_extract_failed_task_errors_async_edge_case_empty_tasks(self, mock_hook_class):
        """Test async function with failed run but empty tasks list"""
        hook = mock_hook_class.return_value
        hook.a_get_run_output = mock.AsyncMock()

        run_state = RunState("TERMINATED", "FAILED", "Job failed")
        run_info = {
            "run_id": RUN_ID,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "Job failed",
            },
            "tasks": [],  # Empty tasks list
        }

        result = await extract_failed_task_errors_async(hook, run_info, run_state)

        assert result == []
        hook.a_get_run_output.assert_not_called()
