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

from airflow.providers.common.compat.sdk import AirflowException, BaseHook, TaskDeferred, timezone
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator
from airflow.providers.microsoft.azure.triggers.powerbi import PowerBITrigger

from tests_common.test_utils.operators.run_deferrable import run_trigger
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS
from unit.microsoft.azure.test_utils import get_airflow_connection

DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
TASK_ID = "run_powerbi_operator"
GROUP_ID = "group_id"
DATASET_ID = "dataset_id"
REQUEST_BODY = {
    "type": "full",
    "commitMode": "transactional",
    "objects": [{"table": "Customer", "partition": "Robert"}],
    "applyRefreshPolicy": "false",
    "timeout": "05:00:00",
}
CONFIG = {
    "task_id": TASK_ID,
    "conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
    "group_id": GROUP_ID,
    "dataset_id": DATASET_ID,
    "check_interval": 1,
    "timeout": 3,
    "request_body": REQUEST_BODY,
}
NEW_REFRESH_REQUEST_ID = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"
PREVIOUS_REFRESH_REQUEST_ID = "0b4c1d2e-3f40-4a5b-8c6d-7e8f9a0b1c2d"
REFRESH_ID_KEY = "powerbi_dataset_refresh_id"

SUCCESS_TRIGGER_EVENT = {
    "status": "success",
    "dataset_refresh_status": None,
    "message": "success",
    "dataset_refresh_id": NEW_REFRESH_REQUEST_ID,
}

SUCCESS_REFRESH_EVENT = {
    "status": "success",
    "dataset_refresh_status": PowerBIDatasetRefreshStatus.COMPLETED,
    "message": "success",
    "dataset_refresh_id": NEW_REFRESH_REQUEST_ID,
}

DEFAULT_DATE = timezone.datetime(2021, 1, 1)


# Sample responses from PowerBI API
COMPLETED_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: NEW_REFRESH_REQUEST_ID,
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
}

FAILED_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: NEW_REFRESH_REQUEST_ID,
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.FAILED,
    PowerBIDatasetRefreshFields.ERROR.value: '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
}

IN_PROGRESS_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: NEW_REFRESH_REQUEST_ID,
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,  # endtime is not available.
}


def previous_refresh(status: str) -> dict[str, str]:
    return {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: PREVIOUS_REFRESH_REQUEST_ID,
        PowerBIDatasetRefreshFields.STATUS.value: status,
        PowerBIDatasetRefreshFields.ERROR.value: "None",
    }


class TaskStateStore:
    """In-memory stand-in for ``context["task_state_store"]``."""

    def __init__(self, **values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value, *, retention=None):
        self.values[key] = value


class TestPowerBIDatasetRefreshOperator:
    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_handle_refresh_wait_for_completion(self, connection):
        """Test that handle_refresh defers to execute_complete when wait_for_completion=True."""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
            wait_for_completion=True,
        )
        context = {"ti": MagicMock()}

        with pytest.raises(TaskDeferred) as exc:
            operator.handle_refresh(
                context=context,
                event=SUCCESS_TRIGGER_EVENT,
            )

        assert context["ti"].xcom_push.call_count == 1
        assert isinstance(exc.value.trigger, PowerBITrigger)
        assert exc.value.trigger.dataset_refresh_id == NEW_REFRESH_REQUEST_ID
        assert exc.value.trigger.wait_for_termination is True
        assert exc.value.method_name == "execute_complete"

    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_handle_refresh_fire_and_forget(self, connection):
        """Test that handle_refresh finishes immediately for fire-and-forget mode."""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
            wait_for_completion=False,
        )
        context = {"ti": MagicMock()}

        operator.handle_refresh(
            context=context,
            event=SUCCESS_TRIGGER_EVENT,
        )

        assert context["ti"].xcom_push.call_count == 1

    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_handle_refresh_failure(self, connection):
        """Test that handle_refresh raises exception on trigger error."""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
            wait_for_completion=False,
        )
        context = {"ti": MagicMock()}

        with pytest.raises(AirflowException, match="Failed to trigger the dataset refresh."):
            operator.handle_refresh(
                context=context,
                event={
                    "status": "error",
                    "dataset_refresh_status": None,
                    "message": "Failed to trigger the dataset refresh.",
                    "dataset_refresh_id": None,
                },
            )

        assert context["ti"].xcom_push.call_count == 0

    def test_powerbi_operator_async_execute_complete_success(self):
        """Assert that execute_complete processes success event correctly"""
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        context = {"ti": MagicMock()}

        operator.execute_complete(
            context=context,
            event=SUCCESS_REFRESH_EVENT,
        )

        # Should push both refresh_id and status
        assert context["ti"].xcom_push.call_count == 2

        # Verify the XCom keys and values
        calls = context["ti"].xcom_push.call_args_list
        xcom_data = {call[1]["key"]: call[1]["value"] for call in calls}

        assert f"{TASK_ID}.powerbi_dataset_refresh_id" in xcom_data
        assert xcom_data[f"{TASK_ID}.powerbi_dataset_refresh_id"] == NEW_REFRESH_REQUEST_ID

        assert f"{TASK_ID}.powerbi_dataset_refresh_status" in xcom_data
        assert xcom_data[f"{TASK_ID}.powerbi_dataset_refresh_status"] == PowerBIDatasetRefreshStatus.COMPLETED

    def test_powerbi_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException) as exc:
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "dataset_refresh_status": None,
                    "message": "error",
                    "dataset_refresh_id": "1234",
                },
            )
        assert context["ti"].xcom_push.call_count == 1
        assert str(exc.value) == "error"

    def test_powerbi_operator_refresh_fail(self):
        """Assert that execute_complete raise exception on refresh fail"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException) as exc:
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "dataset_refresh_status": PowerBIDatasetRefreshStatus.FAILED,
                    "message": "error message",
                    "dataset_refresh_id": "1234",
                },
            )
        assert context["ti"].xcom_push.call_count == 2
        assert str(exc.value) == "error message"

    def test_execute_complete_no_event(self):
        """Test execute_complete when event is None or empty."""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=None,
        )
        assert context["ti"].xcom_push.call_count == 0

    @pytest.mark.db_test
    def test_powerbi_link(self, dag_maker, create_task_instance_of_operator):
        """Assert Power BI Extra link matches the expected URL."""
        ti = create_task_instance_of_operator(
            PowerBIDatasetRefreshOperator,
            dag_id="test_powerbi_refresh_op_link",
            task_id=TASK_ID,
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            group_id=GROUP_ID,
            dataset_id=DATASET_ID,
            check_interval=1,
            timeout=3,
        )

        ti.xcom_push(key="powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID)
        task = dag_maker.dag.get_task(ti.task_id)
        url = task.operator_extra_links[0].get_link(operator=task, ti_key=ti.key)
        EXPECTED_ITEM_RUN_OP_EXTRA_LINK = (
            f"https://app.powerbi.com/groups/{GROUP_ID}/datasets/{DATASET_ID}/details?experience=power-bi"
        )

        assert url == EXPECTED_ITEM_RUN_OP_EXTRA_LINK

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The worker starts the refresh only from Airflow 3.3")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=NEW_REFRESH_REQUEST_ID)
    def test_execute_starts_refresh_on_worker_and_defers_to_poll(self, mock_trigger_dataset_refresh):
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        task_state_store = TaskStateStore()
        context = {"ti": MagicMock(), "task_state_store": task_state_store}

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        mock_trigger_dataset_refresh.assert_called_once_with(
            dataset_id=DATASET_ID, group_id=GROUP_ID, request_body=REQUEST_BODY
        )
        assert isinstance(exc.value.trigger, PowerBITrigger)
        assert exc.value.trigger.dataset_refresh_id == NEW_REFRESH_REQUEST_ID
        assert exc.value.trigger.wait_for_termination is True
        assert exc.value.method_name == "execute_complete"
        context["ti"].xcom_push.assert_called_once_with(
            key=f"{TASK_ID}.powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID
        )
        assert task_state_store.values == {REFRESH_ID_KEY: NEW_REFRESH_REQUEST_ID}
        assert exc.value.trigger.cancel_on_error is False

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The worker starts the refresh only from Airflow 3.3")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=NEW_REFRESH_REQUEST_ID)
    def test_execute_fire_and_forget_mode(self, mock_trigger_dataset_refresh):
        """Fire-and-forget starts the refresh and returns without deferring."""
        operator = PowerBIDatasetRefreshOperator(**CONFIG, wait_for_completion=False)
        context = {"ti": MagicMock(), "task_state_store": TaskStateStore()}

        assert operator.execute(context) is None

        mock_trigger_dataset_refresh.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(
            key=f"{TASK_ID}.powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID
        )

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The task state store arrived in Airflow 3.3")
    @pytest.mark.parametrize(
        "status", [PowerBIDatasetRefreshStatus.IN_PROGRESS, PowerBIDatasetRefreshStatus.COMPLETED]
    )
    @mock.patch.object(PowerBIHook, "get_refresh_history")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    def test_execute_fire_and_forget_does_not_refresh_again_once_started(
        self, mock_trigger_dataset_refresh, mock_get_refresh_history, status
    ):
        mock_get_refresh_history.return_value = [previous_refresh(status)]
        operator = PowerBIDatasetRefreshOperator(**CONFIG, wait_for_completion=False)
        context = {
            "ti": MagicMock(),
            "task_state_store": TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID}),
        }

        assert operator.execute(context) is None

        mock_trigger_dataset_refresh.assert_not_called()
        context["ti"].xcom_push.assert_called_once_with(
            key=f"{TASK_ID}.powerbi_dataset_refresh_id", value=PREVIOUS_REFRESH_REQUEST_ID
        )

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The worker starts the refresh only from Airflow 3.3")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=None)
    def test_execute_raises_when_refresh_has_no_id(self, mock_trigger_dataset_refresh):
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        task_state_store = TaskStateStore()
        context = {"ti": MagicMock(), "task_state_store": task_state_store}

        with pytest.raises(PowerBIDatasetRefreshException, match="Failed to trigger the dataset refresh."):
            operator.execute(context)

        assert task_state_store.values == {}

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The task state store arrived in Airflow 3.3")
    @mock.patch.object(PowerBIHook, "get_refresh_history")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    def test_execute_reconnects_to_refresh_in_progress(
        self, mock_trigger_dataset_refresh, mock_get_refresh_history
    ):
        mock_get_refresh_history.return_value = [previous_refresh(PowerBIDatasetRefreshStatus.IN_PROGRESS)]
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        context = {
            "ti": MagicMock(),
            "task_state_store": TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID}),
        }

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        mock_trigger_dataset_refresh.assert_not_called()
        assert exc.value.trigger.dataset_refresh_id == PREVIOUS_REFRESH_REQUEST_ID
        assert exc.value.method_name == "execute_complete"

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The task state store arrived in Airflow 3.3")
    @mock.patch.object(PowerBIHook, "get_refresh_history")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    def test_execute_does_not_refresh_again_once_completed(
        self, mock_trigger_dataset_refresh, mock_get_refresh_history
    ):
        mock_get_refresh_history.return_value = [previous_refresh(PowerBIDatasetRefreshStatus.COMPLETED)]
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        context = {
            "ti": MagicMock(),
            "task_state_store": TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID}),
        }

        assert operator.execute(context) is None

        mock_trigger_dataset_refresh.assert_not_called()
        xcom_data = {
            call.kwargs["key"]: call.kwargs["value"] for call in context["ti"].xcom_push.call_args_list
        }
        assert xcom_data == {
            f"{TASK_ID}.powerbi_dataset_refresh_id": PREVIOUS_REFRESH_REQUEST_ID,
            f"{TASK_ID}.powerbi_dataset_refresh_status": PowerBIDatasetRefreshStatus.COMPLETED,
        }

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The task state store arrived in Airflow 3.3")
    @pytest.mark.parametrize(
        "refresh_history",
        [
            pytest.param([previous_refresh(PowerBIDatasetRefreshStatus.FAILED)], id="failed"),
            pytest.param([previous_refresh(PowerBIDatasetRefreshStatus.DISABLED)], id="disabled"),
            pytest.param([], id="no-longer-in-history"),
        ],
    )
    @mock.patch.object(PowerBIHook, "get_refresh_history")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=NEW_REFRESH_REQUEST_ID)
    def test_execute_starts_new_refresh_when_previous_cannot_be_resumed(
        self, mock_trigger_dataset_refresh, mock_get_refresh_history, refresh_history
    ):
        mock_get_refresh_history.return_value = refresh_history
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        task_state_store = TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID})
        context = {"ti": MagicMock(), "task_state_store": task_state_store}

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        mock_trigger_dataset_refresh.assert_called_once()
        assert exc.value.trigger.dataset_refresh_id == NEW_REFRESH_REQUEST_ID
        assert task_state_store.values == {REFRESH_ID_KEY: NEW_REFRESH_REQUEST_ID}

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The task state store arrived in Airflow 3.3")
    @pytest.mark.parametrize(
        "execution_status",
        ["Failed", "Completed", "InProgress", "NotStarted"],
    )
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    @mock.patch.object(
        PowerBIHook, "trigger_dataset_refresh", autospec=True, return_value=NEW_REFRESH_REQUEST_ID
    )
    def test_execute_uses_enhanced_refresh_status(
        self, mock_trigger_dataset_refresh, mock_run, execution_status
    ):
        history = {
            "value": [
                {
                    "requestId": PREVIOUS_REFRESH_REQUEST_ID,
                    "status": "Unknown",
                    "refreshType": PowerBIHook.VIA_ENHANCED_API,
                }
            ]
        }
        mock_run.side_effect = [history, history, {"status": "Unknown", "extendedStatus": execution_status}]
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        task_state_store = TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID})
        context = {
            "ti": MagicMock(spec=["xcom_push", "stats_tags"], stats_tags={}),
            "task_state_store": task_state_store,
        }

        if execution_status == "Completed":
            assert operator.execute(context) is None
            context["ti"].xcom_push.assert_any_call(
                key=f"{TASK_ID}.powerbi_dataset_refresh_status", value=PowerBIDatasetRefreshStatus.COMPLETED
            )
        else:
            with pytest.raises(TaskDeferred) as exc:
                operator.execute(context)
            assert exc.value.trigger.dataset_refresh_id == (
                NEW_REFRESH_REQUEST_ID if execution_status == "Failed" else PREVIOUS_REFRESH_REQUEST_ID
            )
            assert exc.value.trigger.cancel_on_error is False

        if execution_status == "Failed":
            mock_trigger_dataset_refresh.assert_awaited_once_with(
                operator.hook, dataset_id=DATASET_ID, group_id=GROUP_ID, request_body=REQUEST_BODY
            )
            assert task_state_store.values == {REFRESH_ID_KEY: NEW_REFRESH_REQUEST_ID}
        else:
            mock_trigger_dataset_refresh.assert_not_awaited()
            assert task_state_store.values == {REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID}
        assert mock_run.await_count == 3
        mock_run.assert_awaited_with(
            operator.hook,
            url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes/{refresh_id}",
            path_parameters={
                "group_id": GROUP_ID,
                "dataset_id": DATASET_ID,
                "refresh_id": PREVIOUS_REFRESH_REQUEST_ID,
            },
        )

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The task state store arrived in Airflow 3.3")
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", autospec=True)
    @mock.patch.object(PowerBIHook, "cancel_dataset_refresh", autospec=True)
    def test_execute_preserves_refresh_when_execution_details_fail(
        self, mock_cancel_dataset_refresh, mock_trigger_dataset_refresh, mock_run
    ):
        history = {
            "value": [
                {
                    "requestId": PREVIOUS_REFRESH_REQUEST_ID,
                    "status": "Unknown",
                    "refreshType": PowerBIHook.VIA_ENHANCED_API,
                }
            ]
        }
        mock_run.side_effect = [history, history, AirflowException("Execution details unavailable")]
        operator = PowerBIDatasetRefreshOperator(**CONFIG)
        task_state_store = TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID})
        context = {
            "ti": MagicMock(spec=["xcom_push", "stats_tags"], stats_tags={}),
            "task_state_store": task_state_store,
        }

        with pytest.raises(
            PowerBIDatasetRefreshException, match="Failed to retrieve refresh execution details"
        ):
            operator.execute(context)

        mock_trigger_dataset_refresh.assert_not_awaited()
        mock_cancel_dataset_refresh.assert_not_awaited()
        assert task_state_store.values == {REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID}

    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=NEW_REFRESH_REQUEST_ID)
    def test_execute_not_durable_defers_to_the_trigger_that_starts_the_refresh(
        self, mock_trigger_dataset_refresh
    ):
        """Without durable execution the worker never talks to Power BI, as before the operator was durable."""
        operator = PowerBIDatasetRefreshOperator(**CONFIG, durable=False)
        task_state_store = TaskStateStore(**{REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID})
        context = {"ti": MagicMock(), "task_state_store": task_state_store}

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        mock_trigger_dataset_refresh.assert_not_called()
        context["ti"].xcom_push.assert_not_called()
        assert isinstance(exc.value.trigger, PowerBITrigger)
        assert exc.value.trigger.dataset_refresh_id is None
        assert exc.value.trigger.request_body == REQUEST_BODY
        assert exc.value.trigger.wait_for_termination is False
        assert exc.value.trigger.cancel_on_error is True
        assert exc.value.method_name == "handle_refresh"
        assert task_state_store.values == {REFRESH_ID_KEY: PREVIOUS_REFRESH_REQUEST_ID}

    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=NEW_REFRESH_REQUEST_ID)
    def test_execute_not_durable_full_lifecycle(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id
    ):
        """Assert the full lifecycle: start in the trigger, defer again to poll, complete on the worker."""
        mock_get_refresh_details_by_refresh_id.return_value = {
            PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
            PowerBIDatasetRefreshFields.ERROR.value: None,
        }
        operator = PowerBIDatasetRefreshOperator(**CONFIG, durable=False)
        context = {"ti": MagicMock(), "task_state_store": TaskStateStore()}

        with pytest.raises(TaskDeferred) as started:
            operator.execute(context)
        [started_event] = run_trigger(started.value.trigger)
        with pytest.raises(TaskDeferred) as polled:
            getattr(operator, started.value.method_name)(context=context, event=started_event.payload)
        [completed_event] = run_trigger(polled.value.trigger)
        assert (
            getattr(operator, polled.value.method_name)(context=context, event=completed_event.payload)
            is None
        )

        mock_trigger_dataset_refresh.assert_called_once_with(
            dataset_id=DATASET_ID, group_id=GROUP_ID, request_body=REQUEST_BODY
        )
        assert started_event.payload["dataset_refresh_id"] == NEW_REFRESH_REQUEST_ID
        assert polled.value.trigger.dataset_refresh_id == NEW_REFRESH_REQUEST_ID
        assert completed_event.payload["dataset_refresh_status"] == PowerBIDatasetRefreshStatus.COMPLETED
        assert context["ti"].xcom_push.call_args_list == [
            mock.call(key=f"{TASK_ID}.powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID),
            mock.call(key=f"{TASK_ID}.powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID),
            mock.call(
                key=f"{TASK_ID}.powerbi_dataset_refresh_status", value=PowerBIDatasetRefreshStatus.COMPLETED
            ),
        ]
        assert context["task_state_store"].values == {}

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The worker starts the refresh only from Airflow 3.3")
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh", return_value=NEW_REFRESH_REQUEST_ID)
    def test_execute_operator_wait_for_completion_full_lifecycle(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id
    ):
        """Assert the full lifecycle: start on the worker, poll in the trigger, complete on the worker."""
        mock_get_refresh_details_by_refresh_id.side_effect = [
            {
                PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,
                PowerBIDatasetRefreshFields.ERROR.value: None,
            },
            {
                PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
                PowerBIDatasetRefreshFields.ERROR.value: None,
            },
        ]
        operator = PowerBIDatasetRefreshOperator(**CONFIG)

        context = {"ti": MagicMock(), "task_state_store": TaskStateStore()}

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)
        events = run_trigger(exc.value.trigger)

        assert getattr(operator, exc.value.method_name)(context=context, event=events[0].payload) is None
        mock_trigger_dataset_refresh.assert_called_once_with(
            dataset_id=DATASET_ID, group_id=GROUP_ID, request_body=REQUEST_BODY
        )
        assert mock_get_refresh_details_by_refresh_id.call_count == 2
        assert [event.payload for event in events] == [
            {
                "status": "success",
                "dataset_refresh_status": PowerBIDatasetRefreshStatus.COMPLETED,
                "message": f"The dataset refresh {NEW_REFRESH_REQUEST_ID} has {PowerBIDatasetRefreshStatus.COMPLETED}.",
                "dataset_refresh_id": NEW_REFRESH_REQUEST_ID,
            }
        ]
