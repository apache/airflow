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

from airflow.providers.common.compat.sdk import AirflowException, BaseHook, TaskDeferred
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
)
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator
from airflow.providers.microsoft.azure.triggers.powerbi import PowerBITrigger

from tests_common.test_utils.mock_context import mock_context
from unit.microsoft.azure.test_utils import get_airflow_connection

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[no-redef]


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


class TestPowerBIDatasetRefreshOperator:
    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_execute_wait_for_completion_with_deferrable(self, connection):
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = mock_context(task=operator)

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        assert isinstance(exc.value.trigger, PowerBITrigger)
        assert exc.value.trigger.dataset_refresh_id is None

    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_powerbi_operator_async_get_refresh_status_success(self, connection):
        """Test that execute defers once when wait_for_completion=True"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
            wait_for_completion=True,  # Explicitly set to True
        )
        context = mock_context(task=operator)

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        # Verify trigger is correct type
        assert isinstance(exc.value.trigger, PowerBITrigger)

        # Verify trigger has correct parameters
        assert exc.value.trigger.dataset_id == DATASET_ID
        assert exc.value.trigger.group_id == GROUP_ID
        assert exc.value.trigger.wait_for_termination is True

        # Verify callback method name
        assert exc.value.method_name == "execute_complete"

        # Verify dataset_refresh_id is None (trigger will create it)
        assert exc.value.trigger.dataset_refresh_id is None

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

    @mock.patch("airflow.providers.microsoft.azure.operators.powerbi.PowerBIHook")
    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_execute_fire_and_forget_mode(self, mock_connection, mock_hook_class):
        """Test fire-and-forget mode (wait_for_completion=False)"""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.trigger_dataset_refresh.return_value = NEW_REFRESH_REQUEST_ID

        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
            wait_for_completion=False,
        )
        context = {"ti": MagicMock()}
        context["ti"].task_id = TASK_ID

        # Should not raise TaskDeferred
        result = operator.execute(context)

        # Verify hook was called correctly
        mock_hook_instance.trigger_dataset_refresh.assert_called_once_with(
            dataset_id=DATASET_ID,
            group_id=GROUP_ID,
            request_body=REQUEST_BODY,
        )

        # Verify XCom push
        assert context["ti"].xcom_push.call_count == 1
        call_args = context["ti"].xcom_push.call_args
        assert call_args[1]["key"] == f"{TASK_ID}.powerbi_dataset_refresh_id"
        assert call_args[1]["value"] == NEW_REFRESH_REQUEST_ID

        # Should return None (completes synchronously)
        assert result is None

    @mock.patch("airflow.providers.microsoft.azure.operators.powerbi.PowerBIHook")
    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_execute_fire_and_forget_mode_failure(self, mock_connection, mock_hook_class):
        """Test fire-and-forget mode raises exception when trigger fails"""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.trigger_dataset_refresh.return_value = None

        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
            wait_for_completion=False,
        )
        context = {"ti": MagicMock()}
        context["ti"].task_id = TASK_ID

        # Should raise AirflowException
        with pytest.raises(AirflowException, match="Failed to trigger dataset refresh"):
            operator.execute(context)

        # Should not push to XCom on failure
        assert context["ti"].xcom_push.call_count == 0

    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_execute_default_behavior_waits_for_completion(self, mock_connection):
        """Test that default behavior (wait_for_completion=True) defers and waits"""
        config_without_wait = {
            "task_id": TASK_ID,
            "conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
            "group_id": GROUP_ID,
            "dataset_id": DATASET_ID,
            "request_body": REQUEST_BODY,
            "check_interval": 1,
            "timeout": 3,
            # Deliberately exclude wait_for_completion - should default to True
        }

        operator = PowerBIDatasetRefreshOperator(**config_without_wait)
        context = mock_context(task=operator)

        # Should defer (because default is wait_for_completion=True)
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        # Verify it deferred with correct trigger
        assert isinstance(exc.value.trigger, PowerBITrigger)
        assert exc.value.trigger.wait_for_termination is True
        assert exc.value.method_name == "execute_complete"
