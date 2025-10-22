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
from airflow.providers.microsoft.azure.operators.powerbi import (
    PowerBIDatasetListOperator,
    PowerBIWorkspaceListOperator,
)
from airflow.providers.microsoft.azure.triggers.powerbi import (
    PowerBIDatasetListTrigger,
    PowerBIWorkspaceListTrigger,
)

from unit.microsoft.azure.test_utils import get_airflow_connection

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[no-redef]


DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
TASK_ID = "run_powerbi_operators"
GROUP_ID = "group_id"

DATASET_LIST_ID = ["5e2d9921-e91b-491f-b7e1-e7d8db49194c"]
WORKSPACE_LIST_ID = ["5e2d9921-e91b-491f-b7e1-e7d8db49194c"]

CONFIG_DATASETS = {
    "task_id": TASK_ID,
    "conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
    "group_id": GROUP_ID,
}
CONFIG_WORKSPACES = {
    "task_id": TASK_ID,
    "conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
}

SUCCESS_LIST_EVENT_DATASETS = {
    "status": "success",
    "message": "success",
    "dataset_ids": DATASET_LIST_ID,
}

SUCCESS_LIST_EVENT_WORKSPACES = {
    "status": "success",
    "message": "success",
    "workspace_ids": WORKSPACE_LIST_ID,
}

DEFAULT_DATE = timezone.datetime(2021, 1, 1)


class TestPowerBIDatasetListOperator:
    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_powerbi_operator_async_get_dataset_list_success(self, connection):
        """Assert that get_dataset_list log success message"""
        operator = PowerBIDatasetListOperator(
            **CONFIG_DATASETS,
        )
        context = {"ti": MagicMock()}
        context["ti"].task_id = TASK_ID

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(
                context=context,
            )

        assert isinstance(exc.value.trigger, PowerBIDatasetListTrigger)
        assert exc.value.trigger.dataset_ids is None
        assert str(exc.value.trigger.group_id) == GROUP_ID

    def test_powerbi_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""
        operator = PowerBIDatasetListOperator(
            **CONFIG_DATASETS,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=SUCCESS_LIST_EVENT_DATASETS,
        )
        assert context["ti"].xcom_push.call_count == 1

    def test_powerbi_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""
        operator = PowerBIDatasetListOperator(
            **CONFIG_DATASETS,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException) as exc:
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "message": "error",
                    "dataset_ids": None,
                },
            )
        assert context["ti"].xcom_push.call_count == 1
        assert str(exc.value) == "error"

    def test_powerbi_operator_dataset_list_fail(self):
        """Assert that execute_complete raise exception on dataset list fail"""
        operator = PowerBIDatasetListOperator(
            **CONFIG_DATASETS,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException) as exc:
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "message": "error message",
                    "dataset_ids": None,
                },
            )
        assert context["ti"].xcom_push.call_count == 1
        assert str(exc.value) == "error message"

    def test_execute_complete_no_event(self):
        """Test execute_complete when event is None or empty."""
        operator = PowerBIDatasetListOperator(
            **CONFIG_DATASETS,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=None,
        )
        assert context["ti"].xcom_push.call_count == 0


class TestPowerBIWorkspaceListOperator:
    @mock.patch.object(BaseHook, "get_connection", side_effect=get_airflow_connection)
    def test_powerbi_operator_async_get_workspace_list_success(self, connection):
        """Assert that get_workspace_list log success message"""
        operator = PowerBIWorkspaceListOperator(
            **CONFIG_WORKSPACES,
        )
        context = {"ti": MagicMock()}
        context["ti"].task_id = TASK_ID

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(
                context=context,
            )

        assert isinstance(exc.value.trigger, PowerBIWorkspaceListTrigger)
        assert exc.value.trigger.workspace_ids is None

    def test_powerbi_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""
        operator = PowerBIWorkspaceListOperator(
            **CONFIG_WORKSPACES,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=SUCCESS_LIST_EVENT_WORKSPACES,
        )
        assert context["ti"].xcom_push.call_count == 1

    def test_powerbi_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""
        operator = PowerBIWorkspaceListOperator(
            **CONFIG_WORKSPACES,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException) as exc:
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "message": "error",
                    "workspace_ids": None,
                },
            )
        assert context["ti"].xcom_push.call_count == 1
        assert str(exc.value) == "error"

    def test_powerbi_operator_workspace_list_fail(self):
        """Assert that execute_complete raise exception on workspace list fail"""
        operator = PowerBIWorkspaceListOperator(
            **CONFIG_WORKSPACES,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException) as exc:
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "message": "error message",
                    "workspace_ids": None,
                },
            )
        assert context["ti"].xcom_push.call_count == 1
        assert str(exc.value) == "error message"

    def test_execute_complete_no_event(self):
        """Test execute_complete when event is None or empty."""
        operator = PowerBIWorkspaceListOperator(
            **CONFIG_WORKSPACES,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=None,
        )
        assert context["ti"].xcom_push.call_count == 0
