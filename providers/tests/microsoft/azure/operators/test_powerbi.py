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
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
)
from airflow.providers.microsoft.azure.operators.powerbi import (
    PowerBIDatasetRefreshOperator,
)
from airflow.providers.microsoft.azure.triggers.powerbi import PowerBITrigger
from airflow.utils import timezone

from providers.tests.microsoft.azure.base import Base
from providers.tests.microsoft.conftest import get_airflow_connection, mock_context

DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
TASK_ID = "run_powerbi_operator"
GROUP_ID = "group_id"
DATASET_ID = "dataset_id"
CONFIG = {
    "task_id": TASK_ID,
    "conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
    "group_id": GROUP_ID,
    "dataset_id": DATASET_ID,
    "check_interval": 1,
    "timeout": 3,
}
NEW_REFRESH_REQUEST_ID = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"

SUCCESS_TRIGGER_EVENT = {
    "status": "success",
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


class TestPowerBIDatasetRefreshOperator(Base):
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection
    )
    def test_execute_wait_for_termination_with_deferrable(self, connection):
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = mock_context(task=operator)

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        assert isinstance(exc.value.trigger, PowerBITrigger)

    def test_powerbi_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        operator.execute_complete(
            context=context,
            event=SUCCESS_TRIGGER_EVENT,
        )
        assert context["ti"].xcom_push.call_count == 2

    def test_powerbi_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""
        operator = PowerBIDatasetRefreshOperator(
            **CONFIG,
        )
        context = {"ti": MagicMock()}
        with pytest.raises(AirflowException):
            operator.execute_complete(
                context=context,
                event={
                    "status": "error",
                    "message": "error",
                    "dataset_refresh_id": "1234",
                },
            )
        assert context["ti"].xcom_push.call_count == 0

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
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            group_id=GROUP_ID,
            dataset_id=DATASET_ID,
            check_interval=1,
            timeout=3,
        )

        ti.xcom_push(key="powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID)
        url = ti.task.get_extra_links(ti, "Monitor PowerBI Dataset")
        EXPECTED_ITEM_RUN_OP_EXTRA_LINK = (
            "https://app.powerbi.com"  # type: ignore[attr-defined]
            f"/groups/{GROUP_ID}/datasets/{DATASET_ID}"  # type: ignore[attr-defined]
            "/details?experience=power-bi"
        )

        assert url == EXPECTED_ITEM_RUN_OP_EXTRA_LINK
