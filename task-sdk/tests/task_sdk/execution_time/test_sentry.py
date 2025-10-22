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

import datetime
import importlib
from unittest import mock

import pytest
import time_machine
import uuid6
from sentry_sdk import configure_scope
from sentry_sdk.transport import Transport

from airflow._shared.timezones import timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType
from airflow.sdk.execution_time.comms import GetTaskBreadcrumbs, TaskBreadcrumbsResult
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.utils.module_loading import import_string
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars

LOGICAL_DATE = timezone.utcnow()
SCHEDULE_INTERVAL = datetime.timedelta(days=1)
DATA_INTERVAL = (LOGICAL_DATE, LOGICAL_DATE + SCHEDULE_INTERVAL)
DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "test_run"
OPERATOR = "PythonOperator"
TRY_NUMBER = 0
STATE = State.SUCCESS
TEST_SCOPE = {
    "dag_id": DAG_ID,
    "task_id": TASK_ID,
    "data_interval_start": DATA_INTERVAL[0],
    "data_interval_end": DATA_INTERVAL[1],
    "logical_date": LOGICAL_DATE,
    "operator": OPERATOR,
    "try_number": TRY_NUMBER,
}
TASK_DATA = {
    "task_id": TASK_ID,
    "state": STATE,
    "operator": OPERATOR,
    "duration": None,
}

CRUMB_DATE = datetime.datetime(2019, 5, 15, tzinfo=datetime.timezone.utc)
CRUMB = {
    "timestamp": CRUMB_DATE,
    "type": "default",
    "category": "completed_tasks",
    "data": TASK_DATA,
    "level": "info",
}


def before_send(_):
    pass


class CustomTransport(Transport):
    pass


class TestSentryHook:
    @pytest.fixture
    def dag_run(self):
        return DagRun.model_construct(
            dag_id=DAG_ID,
            run_id=RUN_ID,
            logical_date=LOGICAL_DATE,
            data_interval_start=DATA_INTERVAL[0],
            data_interval_end=DATA_INTERVAL[1],
            run_after=max(DATA_INTERVAL),
            start_date=max(DATA_INTERVAL),
            run_type=DagRunType.MANUAL,
            state=DagRunState.RUNNING,
            consumed_asset_events=[],
        )

    @pytest.fixture
    def task_instance(self, dag_run):
        ti_date = timezone.utcnow()
        return RuntimeTaskInstance.model_construct(
            id=uuid6.uuid7(),
            task_id=TASK_ID,
            dag_id=dag_run.dag_id,
            run_id=dag_run.run_id,
            try_number=TRY_NUMBER,
            dag_version_id=uuid6.uuid7(),
            task=PythonOperator(task_id=TASK_ID, python_callable=bool),
            bundle_instance=mock.Mock(),
            start_date=ti_date,
            end_date=ti_date,
            state=STATE,
        )

    @pytest.fixture
    def sentry_sdk(self):
        with mock.patch("sentry_sdk.init") as sentry_sdk:
            yield sentry_sdk

    @pytest.fixture
    def sentry(self):
        with conf_vars(
            {
                ("sentry", "sentry_on"): "True",
                ("sentry", "default_integrations"): "False",
                ("sentry", "before_send"): "task_sdk.execution_time.test_sentry.before_send",
            },
        ):
            from airflow.sdk.execution_time import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    @pytest.fixture
    def sentry_custom_transport(self):
        with conf_vars(
            {
                ("sentry", "sentry_on"): "True",
                ("sentry", "default_integrations"): "False",
                ("sentry", "transport"): "task_sdk.execution_time.test_sentry.CustomTransport",
            },
        ):
            from airflow.sdk.execution_time import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    @pytest.fixture
    def sentry_minimum(self):
        """
        Minimum sentry config
        """
        with conf_vars({("sentry", "sentry_on"): "True"}):
            from airflow.sdk.execution_time import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    def test_add_tagging(self, sentry, dag_run, task_instance):
        """
        Test adding tags.
        """
        sentry.add_tagging(dag_run=dag_run, task_instance=task_instance)
        with configure_scope() as scope:
            assert scope._tags == TEST_SCOPE

    @time_machine.travel(CRUMB_DATE)
    def test_add_breadcrumbs(self, mock_supervisor_comms, sentry, dag_run, task_instance):
        """
        Test adding breadcrumbs.
        """
        mock_supervisor_comms.send.return_value = TaskBreadcrumbsResult.model_construct(
            breadcrumbs=[TASK_DATA],
        )

        sentry.add_breadcrumbs(task_instance=task_instance)
        with configure_scope() as scope:
            collected_crumb = scope._breadcrumbs.pop()
        assert collected_crumb == CRUMB

        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(GetTaskBreadcrumbs(dag_id=DAG_ID, run_id=RUN_ID)),
        ]

    def test_before_send(self, sentry_sdk, sentry):
        """
        Test before send callable gets passed to the sentry SDK.
        """
        assert sentry
        called = sentry_sdk.call_args.kwargs["before_send"]
        expected = import_string("task_sdk.execution_time.test_sentry.before_send")
        assert called == expected

    def test_custom_transport(self, sentry_sdk, sentry_custom_transport):
        """
        Test transport gets passed to the sentry SDK
        """
        assert sentry_custom_transport
        called = sentry_sdk.call_args.kwargs["transport"]
        expected = import_string("task_sdk.execution_time.test_sentry.CustomTransport")
        assert called == expected

    def test_minimum_config(self, sentry_sdk, sentry_minimum):
        """
        Test before_send doesn't raise an exception when not set
        """
        assert sentry_minimum
        sentry_sdk.assert_called_once()
