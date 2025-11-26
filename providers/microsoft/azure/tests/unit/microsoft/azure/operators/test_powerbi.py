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

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.common.compat.sdk import BaseHook
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
    def test_execute_wait_for_termination_with_deferrable(self, connection):
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
        """Assert that get_refresh_status log success message"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        context["ti"].task_id = TASK_ID

        with pytest.raises(TaskDeferred) as exc:
            operator.get_refresh_status(
                context=context,
                event=SUCCESS_TRIGGER_EVENT,
            )

        assert isinstance(exc.value.trigger, PowerBITrigger)
        assert exc.value.trigger.dataset_refresh_id is NEW_REFRESH_REQUEST_ID
        assert context["ti"].xcom_push.call_count == 1

    def test_powerbi_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=SUCCESS_REFRESH_EVENT,
        )
        assert context["ti"].xcom_push.call_count == 1

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
        assert context["ti"].xcom_push.call_count == 1
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
    def test_powerbi_link(self, create_task_instance_of_operator):
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
        url = ti.task.operator_extra_links[0].get_link(operator=ti.task, ti_key=ti.key)
        EXPECTED_ITEM_RUN_OP_EXTRA_LINK = (
            f"https://app.powerbi.com/groups/{GROUP_ID}/datasets/{DATASET_ID}/details?experience=power-bi"
        )

        assert url == EXPECTED_ITEM_RUN_OP_EXTRA_LINK
