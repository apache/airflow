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
import sys
import types
from unittest import mock

import pytest
from google.api_core.exceptions import ServiceUnavailable
from google.cloud.dataflow_v1beta3 import Job, JobState, JobType


# While the apache-beam provider is suspended (#61926),
# `airflow.providers.google.cloud.hooks.dataflow` cannot be imported because it
# does `from airflow.providers.apache.beam.hooks.beam import ...` at module
# scope. Probe-import the real module first (which also triggers Airflow's
# provider-entry-point discovery while the real `airflow.providers.apache`
# namespace package is still intact) and only fall back to stubs when the
# probe fails. The stubs deliberately cover only the missing beam-specific
# subtree — we never overwrite `airflow.providers.apache` itself, otherwise
# sibling providers such as `airflow.providers.apache.pig` would be unreachable
# via the namespace package's `__path__`.
def _beam_module_stubs() -> dict[str, types.ModuleType]:
    beam_package = types.ModuleType("airflow.providers.apache.beam")
    beam_package.__path__ = []
    hooks_package = types.ModuleType("airflow.providers.apache.beam.hooks")
    hooks_package.__path__ = []

    class BeamRunnerType:
        DataflowRunner = "DataflowRunner"

    class BeamModule(types.ModuleType):
        BeamHook: object
        BeamRunnerType: object
        beam_options_to_args: object

    beam_module = BeamModule("airflow.providers.apache.beam.hooks.beam")
    beam_module.BeamHook = mock.MagicMock()
    beam_module.BeamRunnerType = BeamRunnerType
    beam_module.beam_options_to_args = mock.MagicMock(return_value=[])

    return {
        "airflow.providers.apache.beam": beam_package,
        "airflow.providers.apache.beam.hooks": hooks_package,
        "airflow.providers.apache.beam.hooks.beam": beam_module,
    }


try:
    import airflow.providers.apache.beam.hooks.beam  # noqa: F401
except ImportError:
    sys.modules.update(_beam_module_stubs())

from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus  # noqa: E402
from airflow.providers.google.cloud.triggers.dataflow import (  # noqa: E402
    DataflowJobAutoScalingEventTrigger,
    DataflowJobMessagesTrigger,
    DataflowJobMetricsTrigger,
    DataflowJobStateCompleteTrigger,
    DataflowJobStatusTrigger,
    DataflowStartYamlJobTrigger,
    TemplateJobStartTrigger,
)
from airflow.triggers.base import TriggerEvent  # noqa: E402

PROJECT_ID = "test-project-id"
JOB_ID = "test_job_id_2012-12-23-10:00"
LOCATION = "us-central1"
GCP_CONN_ID = "test_gcp_conn_id"
POLL_SLEEP = 20
IMPERSONATION_CHAIN = ["impersonate", "this"]
CANCEL_TIMEOUT = 10 * 420
WAIT_UNTIL_FINISHED = None


@pytest.fixture
def template_job_start_trigger():
    return TemplateJobStartTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
    )


@pytest.fixture
def dataflow_job_autoscaling_event_trigger():
    return DataflowJobAutoScalingEventTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        fail_on_terminal_state=False,
    )


@pytest.fixture
def dataflow_job_messages_trigger():
    return DataflowJobMessagesTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        fail_on_terminal_state=False,
    )


@pytest.fixture
def dataflow_job_metrics_trigger():
    return DataflowJobMetricsTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        fail_on_terminal_state=False,
    )


@pytest.fixture
def dataflow_job_status_trigger():
    return DataflowJobStatusTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        expected_statuses={JobState.JOB_STATE_DONE, JobState.JOB_STATE_FAILED},
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
    )


@pytest.fixture
def dataflow_start_yaml_job_trigger():
    return DataflowStartYamlJobTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        cancel_timeout=CANCEL_TIMEOUT,
    )


@pytest.fixture
def dataflow_job_state_complete_trigger():
    return DataflowJobStateCompleteTrigger(
        project_id=PROJECT_ID,
        job_id=JOB_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        poll_sleep=POLL_SLEEP,
        impersonation_chain=IMPERSONATION_CHAIN,
        wait_until_finished=WAIT_UNTIL_FINISHED,
    )


@pytest.fixture
def test_dataflow_batch_job():
    return Job(id=JOB_ID, current_state=JobState.JOB_STATE_DONE, type_=JobType.JOB_TYPE_BATCH)


class TestTemplateJobStartTrigger:
    def test_serialize(self, template_job_start_trigger):
        actual_data = template_job_start_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.TemplateJobStartTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "cancel_timeout": CANCEL_TIMEOUT,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
            ("cancel_timeout", CANCEL_TIMEOUT),
        ],
    )
    def test_get_async_hook(self, template_job_start_trigger, attr, expected):
        hook = template_job_start_trigger._get_async_hook()
        actual = hook._hook_kwargs.get(attr)
        assert actual is not None
        assert actual == expected

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_success_event(self, mock_job_status, template_job_start_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_DONE

        expected_event = TriggerEvent(
            {
                "job_id": JOB_ID,
                "status": "success",
                "message": "Job completed",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_failed_event(self, mock_job_status, template_job_start_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_FAILED

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Dataflow job with id {JOB_ID} has failed its execution",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_return_stopped_event(self, mock_job_status, template_job_start_trigger):
        mock_job_status.return_value = JobState.JOB_STATE_STOPPED
        expected_event = TriggerEvent(
            {
                "status": "stopped",
                "message": f"Dataflow job with id {JOB_ID} was stopped",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_is_still_running(self, mock_job_status, template_job_start_trigger, caplog):
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(template_job_start_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert "Current job status is: JOB_STATE_RUNNING" in caplog.text
        assert f"Sleeping for {POLL_SLEEP} seconds." in caplog.text
        # cancel the task to suppress test warnings
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_continues_polling_after_retryable_service_unavailable(
        self, mock_job_status, mock_sleep, template_job_start_trigger
    ):
        mock_job_status.side_effect = [
            ServiceUnavailable(
                "Visibility check was unavailable. Please retry the request and contact support if the problem persists"
            ),
            JobState.JOB_STATE_DONE,
        ]
        expected_event = TriggerEvent(
            {
                "job_id": JOB_ID,
                "status": "success",
                "message": "Job completed",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event
        assert mock_job_status.await_count == 2
        mock_sleep.assert_awaited_once_with(POLL_SLEEP)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_returns_error_event_after_unexpected_status_polling_exception(
        self, mock_job_status, template_job_start_trigger
    ):
        mock_job_status.side_effect = Exception("Test exception")

        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await template_job_start_trigger.run().asend(None)

        assert actual_event == expected_event
        mock_job_status.assert_awaited_once_with(project_id=PROJECT_ID, job_id=JOB_ID, location=LOCATION)


class TestDataflowJobAutoScalingEventTrigger:
    def test_serialize(self, dataflow_job_autoscaling_event_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "fail_on_terminal_state": False,
            },
        )
        actual_data = dataflow_job_autoscaling_event_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
        ],
    )
    def test_async_hook(self, dataflow_job_autoscaling_event_trigger, attr, expected):
        hook = dataflow_job_autoscaling_event_trigger.async_hook
        actual = hook._hook_kwargs.get(attr)
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_UPDATED,
            JobState.JOB_STATE_DRAINED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch(
        "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger.list_job_autoscaling_events"
    )
    async def test_run_yields_terminal_state_event_if_fail_on_terminal_state(
        self,
        mock_list_job_autoscaling_events,
        mock_job_status,
        job_status_value,
        dataflow_job_autoscaling_event_trigger,
    ):
        dataflow_job_autoscaling_event_trigger.fail_on_terminal_state = True
        mock_list_job_autoscaling_events.return_value = []
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is already in terminal state: {job_status_value.name}",
                "result": None,
            }
        )
        actual_event = await dataflow_job_autoscaling_event_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch(
        "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger.list_job_autoscaling_events"
    )
    async def test_run_loop_is_still_running_if_fail_on_terminal_state(
        self,
        mock_list_job_autoscaling_events,
        mock_job_status,
        dataflow_job_autoscaling_event_trigger,
        caplog,
    ):
        """Test that DataflowJobAutoScalingEventTrigger is still in loop if the job status is RUNNING."""
        dataflow_job_autoscaling_event_trigger.fail_on_terminal_state = True
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        mock_list_job_autoscaling_events.return_value = []
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(dataflow_job_autoscaling_event_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        # cancel the task to suppress test warnings
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch(
        "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger.list_job_autoscaling_events"
    )
    async def test_run_yields_autoscaling_events(
        self, mock_list_job_autoscaling_events, mock_job_status, dataflow_job_autoscaling_event_trigger
    ):
        mock_job_status.return_value = JobState.JOB_STATE_DONE
        test_autoscaling_events = [
            {
                "event_type": 2,
                "description": {},
                "time": "2024-02-05T13:43:31.066611771Z",
                "worker_pool": "Regular",
                "current_num_workers": "0",
                "target_num_workers": "0",
            },
            {
                "target_num_workers": "1",
                "event_type": 1,
                "description": {},
                "time": "2024-02-05T13:43:31.066611771Z",
                "worker_pool": "Regular",
                "current_num_workers": "0",
            },
        ]
        mock_list_job_autoscaling_events.return_value = test_autoscaling_events
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Detected 2 autoscaling events for job '{JOB_ID}'",
                "result": test_autoscaling_events,
            }
        )
        actual_event = await dataflow_job_autoscaling_event_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_raises_exception(self, mock_job_status, dataflow_job_autoscaling_event_trigger):
        """
        Tests the DataflowJobAutoScalingEventTrigger does fire if there is an exception.
        """
        mock_job_status.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent({"status": "error", "message": "Test exception", "result": None})
        actual_event = await dataflow_job_autoscaling_event_trigger.run().asend(None)
        assert expected_event == actual_event


class TestDataflowJobMessagesTrigger:
    """Test case for DataflowJobMessagesTrigger"""

    def test_serialize(self, dataflow_job_messages_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "fail_on_terminal_state": False,
            },
        )
        actual_data = dataflow_job_messages_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
        ],
    )
    def test_async_hook(self, dataflow_job_messages_trigger, attr, expected):
        hook = dataflow_job_messages_trigger.async_hook
        actual = hook._hook_kwargs.get(attr)
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_UPDATED,
            JobState.JOB_STATE_DRAINED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch(
        "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger.list_job_messages"
    )
    async def test_run_yields_terminal_state_event_if_fail_on_terminal_state(
        self,
        mock_list_job_messages,
        mock_job_status,
        job_status_value,
        dataflow_job_messages_trigger,
    ):
        dataflow_job_messages_trigger.fail_on_terminal_state = True
        mock_list_job_messages.return_value = []
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is already in terminal state: {job_status_value.name}",
                "result": None,
            }
        )
        actual_event = await dataflow_job_messages_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch(
        "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger.list_job_messages"
    )
    async def test_run_loop_is_still_running_if_fail_on_terminal_state(
        self,
        mock_list_job_messages,
        mock_job_status,
        dataflow_job_messages_trigger,
        caplog,
    ):
        """Test that DataflowJobMessagesTrigger is still in loop if the job status is RUNNING."""
        dataflow_job_messages_trigger.fail_on_terminal_state = True
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        mock_list_job_messages.return_value = []
        caplog.set_level(logging.INFO)
        task = asyncio.create_task(dataflow_job_messages_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        # cancel the task to suppress test warnings
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch(
        "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger.list_job_messages"
    )
    async def test_run_yields_job_messages(
        self, mock_list_job_messages, mock_job_status, dataflow_job_messages_trigger
    ):
        mock_job_status.return_value = JobState.JOB_STATE_DONE
        test_job_messages = [
            {
                "id": "1707695235850",
                "time": "2024-02-06T23:47:15.850Z",
                "message_text": "msg.",
                "message_importance": 5,
            },
            {
                "id": "1707695635401",
                "time": "2024-02-06T23:53:55.401Z",
                "message_text": "msg.",
                "message_importance": 5,
            },
        ]
        mock_list_job_messages.return_value = test_job_messages
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Detected 2 job messages for job '{JOB_ID}'",
                "result": test_job_messages,
            }
        )
        actual_event = await dataflow_job_messages_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_raises_exception(self, mock_job_status, dataflow_job_messages_trigger):
        """
        Tests the DataflowJobMessagesTrigger does fire if there is an exception.
        """
        mock_job_status.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent({"status": "error", "message": "Test exception", "result": None})
        actual_event = await dataflow_job_messages_trigger.run().asend(None)
        assert expected_event == actual_event


class TestDataflowJobMetricsTrigger:
    """Test case for DataflowJobMetricsTrigger"""

    def test_serialize(self, dataflow_job_metrics_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMetricsTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "fail_on_terminal_state": False,
            },
        )
        actual_data = dataflow_job_metrics_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
        ],
    )
    def test_async_hook(self, dataflow_job_metrics_trigger, attr, expected):
        hook = dataflow_job_metrics_trigger.async_hook
        actual = hook._hook_kwargs.get(attr)
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_UPDATED,
            JobState.JOB_STATE_DRAINED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobMetricsTrigger.get_job_metrics")
    async def test_run_yields_terminal_state_event_if_fail_on_terminal_state(
        self,
        mock_get_job_metrics,
        mock_job_status,
        job_status_value,
        dataflow_job_metrics_trigger,
    ):
        dataflow_job_metrics_trigger.fail_on_terminal_state = True
        mock_get_job_metrics.return_value = []
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is already in terminal state: {job_status_value.name}",
                "result": None,
            }
        )
        actual_event = await dataflow_job_metrics_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobMetricsTrigger.get_job_metrics")
    async def test_run_loop_is_still_running_if_fail_on_terminal_state(
        self,
        mock_get_job_metrics,
        mock_job_status,
        dataflow_job_metrics_trigger,
    ):
        """Test that DataflowJobMetricsTrigger is still in loop if the job status is RUNNING."""
        dataflow_job_metrics_trigger.fail_on_terminal_state = True
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        mock_get_job_metrics.return_value = []
        task = asyncio.create_task(dataflow_job_metrics_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        # cancel the task to suppress test warnings
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    @mock.patch("airflow.providers.google.cloud.triggers.dataflow.DataflowJobMetricsTrigger.get_job_metrics")
    async def test_run_yields_job_messages(
        self, mock_get_job_metrics, mock_job_status, dataflow_job_metrics_trigger
    ):
        mock_job_status.return_value = JobState.JOB_STATE_DONE
        test_job_metrics = [
            {
                "name": {"origin": "", "name": "", "context": {}},
                "scalar": 0.0,
                "update_time": "2024-03-20T12:36:05.229Z",
                "kind": "",
                "cumulative": False,
            },
            {
                "name": {"origin": "", "name": "", "context": {}},
                "scalar": 0.0,
                "update_time": "2024-03-20T12:36:05.229Z",
                "kind": "",
                "cumulative": False,
            },
        ]
        mock_get_job_metrics.return_value = test_job_metrics
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Detected 2 metrics for job '{JOB_ID}'",
                "result": test_job_metrics,
            }
        )
        actual_event = await dataflow_job_metrics_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_raises_exception(self, mock_job_status, dataflow_job_metrics_trigger):
        """
        Tests the DataflowJobMetrcisTrigger does fire if there is an exception.
        """
        mock_job_status.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent({"status": "error", "message": "Test exception", "result": None})
        actual_event = await dataflow_job_metrics_trigger.run().asend(None)
        assert expected_event == actual_event


class TestDataflowJobStatusTrigger:
    """Test case for DataflowJobStatusTrigger"""

    def test_serialize(self, dataflow_job_status_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobStatusTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "expected_statuses": {JobState.JOB_STATE_DONE, JobState.JOB_STATE_FAILED},
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
            },
        )
        actual_data = dataflow_job_status_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
        ],
    )
    def test_async_hook(self, dataflow_job_status_trigger, attr, expected):
        hook = dataflow_job_status_trigger.async_hook
        actual = hook._hook_kwargs.get(attr)
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_UPDATED,
            JobState.JOB_STATE_DRAINED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_yields_terminal_state_event(
        self,
        mock_job_status,
        job_status_value,
        dataflow_job_status_trigger,
    ):
        dataflow_job_status_trigger.expected_statuses = {DataflowJobStatus.JOB_STATE_CANCELLING}
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is already in terminal state: {job_status_value.name}",
            }
        )
        actual_event = await dataflow_job_status_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_DONE,
            JobState.JOB_STATE_RUNNING,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_yields_success_event_if_expected_job_status(
        self,
        mock_job_status,
        job_status_value,
        dataflow_job_status_trigger,
    ):
        dataflow_job_status_trigger.expected_statuses = {
            DataflowJobStatus.JOB_STATE_DONE,
            DataflowJobStatus.JOB_STATE_RUNNING,
        }
        mock_job_status.return_value = job_status_value
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Job with id '{JOB_ID}' has reached an expected state: {job_status_value.name}",
            }
        )
        actual_event = await dataflow_job_status_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_loop_is_still_running_if_state_is_not_terminal_or_expected(
        self,
        mock_job_status,
        dataflow_job_status_trigger,
    ):
        """Test that DataflowJobStatusTrigger is still in loop if the job status neither terminal nor expected."""
        dataflow_job_status_trigger.expected_statuses = {DataflowJobStatus.JOB_STATE_DONE}
        mock_job_status.return_value = JobState.JOB_STATE_RUNNING
        task = asyncio.create_task(dataflow_job_status_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job_status")
    async def test_run_raises_exception(self, mock_job_status, dataflow_job_status_trigger):
        """
        Tests the DataflowJobStatusTrigger does fire if there is an exception.
        """
        mock_job_status.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await dataflow_job_status_trigger.run().asend(None)
        assert expected_event == actual_event


class TestDataflowStartYamlJobTrigger:
    def test_serialize(self, dataflow_start_yaml_job_trigger):
        actual_data = dataflow_start_yaml_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowStartYamlJobTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "expected_terminal_state": None,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "cancel_timeout": CANCEL_TIMEOUT,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
            ("cancel_timeout", CANCEL_TIMEOUT),
        ],
    )
    def test_get_async_hook(self, dataflow_start_yaml_job_trigger, attr, expected):
        hook = dataflow_start_yaml_job_trigger._get_async_hook()
        actual = hook._hook_kwargs.get(attr)
        assert actual is not None
        assert actual == expected

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_loop_return_success_event(
        self, mock_get_job, dataflow_start_yaml_job_trigger, test_dataflow_batch_job
    ):
        mock_get_job.return_value = test_dataflow_batch_job
        expected_event = TriggerEvent(
            {
                "job": Job.to_dict(test_dataflow_batch_job),
                "status": "success",
                "message": "Batch job completed.",
            }
        )
        actual_event = await dataflow_start_yaml_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_loop_return_failed_event(
        self, mock_get_job, dataflow_start_yaml_job_trigger, test_dataflow_batch_job
    ):
        test_dataflow_batch_job.current_state = JobState.JOB_STATE_FAILED
        mock_get_job.return_value = test_dataflow_batch_job
        expected_event = TriggerEvent(
            {
                "job": Job.to_dict(test_dataflow_batch_job),
                "status": "error",
                "message": "Job failed.",
            }
        )
        actual_event = await dataflow_start_yaml_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_loop_return_stopped_event(
        self, mock_get_job, dataflow_start_yaml_job_trigger, test_dataflow_batch_job
    ):
        test_dataflow_batch_job.current_state = JobState.JOB_STATE_STOPPED
        mock_get_job.return_value = test_dataflow_batch_job
        expected_event = TriggerEvent(
            {
                "job": Job.to_dict(test_dataflow_batch_job),
                "status": "stopped",
                "message": "Job was stopped.",
            }
        )
        actual_event = await dataflow_start_yaml_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_loop_return_expected_state_event(
        self, mock_get_job, dataflow_start_yaml_job_trigger, test_dataflow_batch_job
    ):
        dataflow_start_yaml_job_trigger.expected_terminal_state = DataflowJobStatus.JOB_STATE_RUNNING
        test_dataflow_batch_job.current_state = JobState.JOB_STATE_RUNNING
        mock_get_job.return_value = test_dataflow_batch_job
        expected_event = TriggerEvent(
            {
                "job": Job.to_dict(test_dataflow_batch_job),
                "status": "success",
                "message": f"Job reached the expected terminal state: {DataflowJobStatus.JOB_STATE_RUNNING}.",
            }
        )
        actual_event = await dataflow_start_yaml_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_loop_is_still_running(
        self, mock_get_job, dataflow_start_yaml_job_trigger, test_dataflow_batch_job
    ):
        """Test that DataflowStartYamlJobTrigger is still in loop if the job status neither terminal nor expected."""
        dataflow_start_yaml_job_trigger.expected_terminal_state = DataflowJobStatus.JOB_STATE_STOPPED
        test_dataflow_batch_job.current_state = JobState.JOB_STATE_RUNNING
        mock_get_job.return_value = test_dataflow_batch_job
        task = asyncio.create_task(dataflow_start_yaml_job_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        task.cancel()


class TestDataflowJobStateCompleteTrigger:
    """Test case for DataflowJobStatusTrigger"""

    def test_serialize(self, dataflow_job_state_complete_trigger):
        expected_data = (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobStateCompleteTrigger",
            {
                "project_id": PROJECT_ID,
                "job_id": JOB_ID,
                "location": LOCATION,
                "wait_until_finished": WAIT_UNTIL_FINISHED,
                "gcp_conn_id": GCP_CONN_ID,
                "poll_sleep": POLL_SLEEP,
                "impersonation_chain": IMPERSONATION_CHAIN,
            },
        )
        actual_data = dataflow_job_state_complete_trigger.serialize()
        assert actual_data == expected_data

    @pytest.mark.parametrize(
        ("attr", "expected"),
        [
            ("gcp_conn_id", GCP_CONN_ID),
            ("poll_sleep", POLL_SLEEP),
            ("impersonation_chain", IMPERSONATION_CHAIN),
        ],
    )
    def test_async_hook(self, dataflow_job_state_complete_trigger, attr, expected):
        hook = dataflow_job_state_complete_trigger.async_hook
        actual = hook._hook_kwargs.get(attr)
        assert actual == expected

    @pytest.mark.parametrize(
        "job_status_value",
        [
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_DRAINED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_yields_error_on_failed_state(
        self,
        mock_job,
        job_status_value,
        dataflow_job_state_complete_trigger,
    ):
        mock_job.return_value = Job(
            id=JOB_ID, current_state=job_status_value, type_=JobType.JOB_TYPE_STREAMING
        )
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": f"Job with id '{JOB_ID}' is in failed terminal state: {job_status_value.name}",
            }
        )
        actual_event = await dataflow_job_state_complete_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "job_item",
        [
            Job(id=JOB_ID, current_state=JobState.JOB_STATE_DONE, type_=JobType.JOB_TYPE_BATCH),
            Job(id=JOB_ID, current_state=JobState.JOB_STATE_RUNNING, type_=JobType.JOB_TYPE_STREAMING),
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_yields_success_event_if_expected_job_status(
        self,
        mock_job,
        job_item,
        dataflow_job_state_complete_trigger,
    ):
        dataflow_job_state_complete_trigger.expected_statuses = {
            DataflowJobStatus.JOB_STATE_DONE,
            DataflowJobStatus.JOB_STATE_RUNNING,
        }
        mock_job.return_value = mock_job.return_value = job_item

        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": f"Job with id '{JOB_ID}' has reached successful final state:"
                f" {job_item.current_state.name}",
            }
        )
        actual_event = await dataflow_job_state_complete_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_loop_is_still_running_if_state_is_not_terminal_or_expected(
        self,
        mock_job,
        dataflow_job_state_complete_trigger,
    ):
        """
        Test that DataflowJobStateCompleteTrigger is still in loop if the job status neither
        terminal nor expected.
        """
        mock_job.return_value = Job(
            id=JOB_ID,
            current_state=JobState.JOB_STATE_PENDING,
            type_=JobType.JOB_TYPE_STREAMING,
        )
        task = asyncio.create_task(dataflow_job_state_complete_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook.get_job")
    async def test_run_raises_exception(self, mock_job, dataflow_job_state_complete_trigger):
        """
        Tests the DataflowJobStateCompleteTrigger does trigger error if there is an exception.
        """
        mock_job.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await dataflow_job_state_complete_trigger.run().asend(None)
        assert expected_event == actual_event
