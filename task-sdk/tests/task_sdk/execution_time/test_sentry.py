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
import sys
import types
from unittest import mock

import pytest
import uuid6

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk._shared.timezones import timezone
from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType, TaskInstanceState
from airflow.sdk.execution_time.comms import GetTaskBreadcrumbs, TaskBreadcrumbsResult
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.sdk.module_loading import import_string

from tests_common.test_utils.config import conf_vars

LOGICAL_DATE = timezone.utcnow()
SCHEDULE_INTERVAL = datetime.timedelta(days=1)
DATA_INTERVAL = (LOGICAL_DATE, LOGICAL_DATE + SCHEDULE_INTERVAL)
DAG_ID = "test_dag"
TASK_ID = "test_task"
RUN_ID = "test_run"
OPERATOR = "PythonOperator"
TRY_NUMBER = 0
STATE = TaskInstanceState.SUCCESS
TASK_DATA = {
    "task_id": TASK_ID,
    "state": STATE,
    "operator": OPERATOR,
    "duration": None,
}


def before_send(_):
    pass


class CustomIntegration:
    """
    Integration object to use in tests.

    All instances of this class are equal to each other.
    """

    def __hash__(self):  # Implemented to satisfy Ruff.
        return 0

    def __eq__(self, other):
        return type(self) is type(other)


class CustomTransport:
    pass


def is_configured(obj):
    from airflow.sdk.execution_time.sentry.configured import ConfiguredSentry

    return isinstance(obj, ConfiguredSentry)


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

    @pytest.fixture(scope="class", autouse=True)
    def mock_sentry_sdk(self):
        sentry_sdk_integrations_logging = types.ModuleType("sentry_sdk.integrations.logging")
        sentry_sdk_integrations_logging.ignore_logger = mock.MagicMock()

        sentry_sdk = types.ModuleType("sentry_sdk")
        sentry_sdk.init = mock.MagicMock()
        sentry_sdk.integrations = mock.Mock(logging=sentry_sdk_integrations_logging)
        sentry_sdk.configure_scope = mock.MagicMock()
        sentry_sdk.add_breadcrumb = mock.MagicMock()

        sys.modules["sentry_sdk"] = sentry_sdk
        sys.modules["sentry_sdk.integrations.logging"] = sentry_sdk_integrations_logging
        yield sentry_sdk
        del sys.modules["sentry_sdk"]
        del sys.modules["sentry_sdk.integrations.logging"]

    @pytest.fixture(autouse=True)
    def remove_mock_sentry_sdk(self, mock_sentry_sdk):
        yield
        mock_sentry_sdk.integrations.logging.ignore_logger.reset_mock()
        mock_sentry_sdk.init.reset_mock()
        mock_sentry_sdk.configure_scope.reset_mock()
        mock_sentry_sdk.add_breadcrumb.reset_mock()

    @pytest.fixture
    def sentry(self, mock_sentry_sdk):
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
    def sentry_custom_transport(self, mock_sentry_sdk):
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
    def sentry_minimum(self, mock_sentry_sdk):
        """
        Minimum sentry config
        """
        with conf_vars({("sentry", "sentry_on"): "True"}):
            from airflow.sdk.execution_time import sentry

            importlib.reload(sentry)
            yield sentry.Sentry

        importlib.reload(sentry)

    def test_prepare_to_enrich_errors(self, mock_sentry_sdk, sentry):
        assert is_configured(sentry)

        sentry.prepare_to_enrich_errors(executor_integration="")
        assert mock_sentry_sdk.integrations.logging.ignore_logger.mock_calls == [mock.call("airflow.task")]
        assert mock_sentry_sdk.init.mock_calls == [
            mock.call(
                integrations=[],
                default_integrations=False,
                before_send="task_sdk.execution_time.test_sentry.before_send",
            ),
        ]

    def test_prepare_to_enrich_errors_with_executor_integration(self, mock_sentry_sdk, sentry):
        assert is_configured(sentry)

        executor_integration = "task_sdk.execution_time.test_sentry.CustomIntegration"
        sentry.prepare_to_enrich_errors(executor_integration)
        assert mock_sentry_sdk.integrations.logging.ignore_logger.mock_calls == [mock.call("airflow.task")]
        assert mock_sentry_sdk.init.mock_calls == [
            mock.call(
                integrations=[import_string("task_sdk.execution_time.test_sentry.CustomIntegration")()],
                default_integrations=False,
                before_send="task_sdk.execution_time.test_sentry.before_send",
            ),
        ]

    def test_add_tagging(self, mock_sentry_sdk, sentry, dag_run, task_instance):
        """
        Test adding tags.
        """
        sentry.add_tagging(dag_run=dag_run, task_instance=task_instance)
        assert mock_sentry_sdk.configure_scope.mock_calls == [
            mock.call.__call__(),
            mock.call.__call__().__enter__(),
            mock.call.__call__().__enter__().set_tag("task_id", TASK_ID),
            mock.call.__call__().__enter__().set_tag("dag_id", DAG_ID),
            mock.call.__call__().__enter__().set_tag("try_number", TRY_NUMBER),
            mock.call.__call__().__enter__().set_tag("data_interval_start", DATA_INTERVAL[0]),
            mock.call.__call__().__enter__().set_tag("data_interval_end", DATA_INTERVAL[1]),
            mock.call.__call__().__enter__().set_tag("logical_date", LOGICAL_DATE),
            mock.call.__call__().__enter__().set_tag("operator", OPERATOR),
            mock.call.__call__().__exit__(None, None, None),
        ]

    def test_add_breadcrumbs(self, mock_supervisor_comms, mock_sentry_sdk, sentry, task_instance):
        """
        Test adding breadcrumbs.
        """
        mock_supervisor_comms.send.return_value = TaskBreadcrumbsResult.model_construct(
            breadcrumbs=[TASK_DATA],
        )

        sentry.add_breadcrumbs(task_instance=task_instance)
        assert mock_sentry_sdk.add_breadcrumb.mock_calls == [
            mock.call(category="completed_tasks", data=TASK_DATA, level="info"),
        ]

        assert mock_supervisor_comms.send.mock_calls == [
            mock.call(GetTaskBreadcrumbs(dag_id=DAG_ID, run_id=RUN_ID)),
        ]

    def test_custom_transport(self, mock_sentry_sdk, sentry_custom_transport):
        """
        Test transport gets passed to the sentry SDK
        """
        assert is_configured(sentry_custom_transport)

        sentry_custom_transport.prepare_to_enrich_errors(executor_integration="")
        assert mock_sentry_sdk.integrations.logging.ignore_logger.mock_calls == [mock.call("airflow.task")]
        assert mock_sentry_sdk.init.mock_calls == [
            mock.call(
                integrations=[],
                default_integrations=False,
                transport="task_sdk.execution_time.test_sentry.CustomTransport",
            ),
        ]

    def test_minimum_config(self, mock_sentry_sdk, sentry_minimum):
        """
        Test before_send doesn't raise an exception when not set
        """
        assert is_configured(sentry_minimum)

        sentry_minimum.prepare_to_enrich_errors(executor_integration="")
        assert mock_sentry_sdk.integrations.logging.ignore_logger.mock_calls == [mock.call("airflow.task")]
        assert mock_sentry_sdk.init.mock_calls == [mock.call(integrations=[])]
