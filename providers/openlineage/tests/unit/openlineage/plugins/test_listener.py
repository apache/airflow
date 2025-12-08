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

import uuid
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import Future
from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.transport import ConsoleTransport
from openlineage.client.transport.console import ConsoleConfig
from uuid6 import uuid7

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter
from airflow.providers.openlineage.plugins.listener import OpenLineageListener
from airflow.providers.openlineage.utils.selective_enable import disable_lineage, enable_lineage
from airflow.utils import types
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.compat import EmptyOperator, PythonOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow._shared.timezones import timezone
else:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

from airflow.providers.common.compat.sdk import BaseOperator

EXPECTED_TRY_NUMBER_1 = 1

TRY_NUMBER_BEFORE_EXECUTION = 0
TRY_NUMBER_RUNNING = 0
TRY_NUMBER_FAILED = 0
TRY_NUMBER_SUCCESS = 0
TRY_NUMBER_AFTER_EXECUTION = 0

if TYPE_CHECKING:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


class TemplateOperator(BaseOperator):
    template_fields = ["df"]

    def __init__(self, df, *args, **kwargs):
        self.df = df
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return self.df


def render_df():
    return pd.DataFrame({"col": [1, 2]})


def regular_call(self, callable, callable_name, use_fork):
    callable()


class MockExecutor:
    def __init__(self, *args, **kwargs):
        self.submitted = False
        self.succeeded = False
        self.result = None

    def submit(self, fn, /, *args, **kwargs):
        self.submitted = True
        try:
            fn(*args, **kwargs)
            self.succeeded = True
        except Exception:
            pass
        return MagicMock()

    def shutdown(self, *args, **kwargs):
        print("Shutting down")


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 tests")
class TestOpenLineageListenerAirflow2:
    @patch("airflow.models.TaskInstance.xcom_push")
    @patch("airflow.models.BaseOperator.render_template")
    def test_listener_does_not_change_task_instance(self, render_mock, xcom_push_mock):
        render_mock.return_value = render_df()
        date = timezone.datetime(2022, 1, 1)
        dag = DAG(
            "test",
            schedule=None,
            start_date=date,
            user_defined_macros={"render_df": render_df},
            params={"df": {"col": [1, 2]}},
        )
        t = TemplateOperator(task_id="template_op", dag=dag, do_xcom_push=True, df=dag.param("df"))
        run_id = str(uuid.uuid1())
        dag.create_dagrun(
            run_id=run_id,
            data_interval=(date, date),
            run_type=types.DagRunType.MANUAL,
            state=DagRunState.RUNNING,
        )
        ti = TaskInstance(t, run_id=run_id)
        ti.check_and_change_state_before_execution()  # make listener hook on running event
        ti._run_raw_task()

        # check if task returns the same DataFrame
        pd.testing.assert_frame_equal(xcom_push_mock.call_args.kwargs["value"], render_df())

        # check if render_template method always get the same unrendered field
        assert not isinstance(render_mock.call_args.args[0], pd.DataFrame)

    def _setup_mock_listener(self, mock_listener: mock.Mock, captured_try_numbers: dict[str, int]) -> None:
        """Sets up the mock listener with side effects to capture try numbers for different task instance events.

        :param mock_listener: The mock object for the listener manager.
        :param captured_try_numbers: A dictionary to store captured try numbers keyed by event names.

        This function iterates through specified event names and sets a side effect on the corresponding
        method of the listener manager's hook. The side effect is a nested function that captures the try number
        of the task instance when the method is called.

        :Example:

            captured_try_numbers = {}
            mock_listener = Mock()
            _setup_mock_listener(mock_listener, captured_try_numbers)
            # After running a task, captured_try_numbers will have the try number captured at the moment of
            execution for specified methods. F.e. {"running": 1, "success": 2} for on_task_instance_running and
            on_task_instance_success methods.
        """

        def capture_try_number(method_name):
            def inner(*args, **kwargs):
                captured_try_numbers[method_name] = kwargs["task_instance"].try_number

            return inner

        for event in ["running", "success", "failed"]:
            getattr(
                mock_listener.return_value.hook, f"on_task_instance_{event}"
            ).side_effect = capture_try_number(event)

    def _create_test_dag_and_task(
        self, python_callable: Callable, scenario_name: str
    ) -> tuple[DagRun, TaskInstance]:
        """Creates a test DAG and a task for a custom test scenario.

        :param python_callable: The Python callable to be executed by the PythonOperator.
        :param scenario_name: The name of the test scenario, used to uniquely name the DAG and task.

        :return: TaskInstance: The created TaskInstance object.

        This function creates a DAG and a PythonOperator task with the provided
        python_callable. It generates a unique run ID and creates a DAG run. This
        setup is useful for testing different scenarios in Airflow tasks.

            :Example:

                def sample_callable(**kwargs):
                    print("Hello World")

            task_instance = _create_test_dag_and_task(sample_callable, "sample_scenario")
            # Use task_instance to simulate running a task in a test.
        """
        date = timezone.datetime(2022, 1, 1)
        dag = DAG(
            f"test_{scenario_name}",
            schedule=None,
            start_date=date,
        )
        t = PythonOperator(task_id=f"test_task_{scenario_name}", dag=dag, python_callable=python_callable)
        run_id = str(uuid.uuid1())
        dagrun = create_scheduler_dag(dag).create_dagrun(
            run_id=run_id,
            data_interval=(date, date),
            run_type=types.DagRunType.MANUAL,
            state=DagRunState.RUNNING,
            execution_date=date,  # type: ignore
        )
        if AIRFLOW_V_3_1_PLUS:
            from airflow.serialization.serialized_objects import create_scheduler_operator

            task_instance = TaskInstance(
                create_scheduler_operator(t),
                run_id=run_id,
                dag_version_id=dagrun.created_dag_version_id,
            )
        elif AIRFLOW_V_3_0_PLUS:
            task_instance = TaskInstance(
                t,  # type: ignore[arg-type]
                run_id=run_id,
                dag_version_id=dagrun.created_dag_version_id,
            )
        else:
            task_instance = TaskInstance(t, run_id=run_id)  # type: ignore
        return dagrun, task_instance

    def _create_listener_and_task_instance(self) -> tuple[OpenLineageListener, TaskInstance]:
        """Creates and configures an OpenLineageListener instance and a mock TaskInstance for testing.

        :return: A tuple containing the configured OpenLineageListener and TaskInstance.

        This function instantiates an OpenLineageListener, sets up its required properties with mock objects, and
        creates a mock TaskInstance with predefined attributes. This setup is commonly used for testing the
        interaction between an OpenLineageListener and a TaskInstance in Airflow.

        :Example:

            listener, task_instance = _create_listener_and_task_instance()
            # Now you can use listener and task_instance in your tests to simulate their interaction.
        """

        def mock_dag_id(dag_id, logical_date, clear_number):
            return f"{logical_date.isoformat()}.{dag_id}.{clear_number}"

        def mock_task_id(dag_id, task_id, try_number, logical_date, map_index):
            return f"{logical_date.isoformat()}.{dag_id}.{task_id}.{try_number}.{map_index}"

        listener = OpenLineageListener()
        listener.extractor_manager = mock.Mock()

        metadata = mock.Mock()
        metadata.run_facets = {"run_facet": 1}
        listener.extractor_manager.extract_metadata.return_value = metadata

        adapter = mock.Mock()
        adapter.build_dag_run_id.side_effect = mock_dag_id
        adapter.build_task_instance_run_id.side_effect = mock_task_id
        adapter.start_task = mock.Mock()
        adapter.fail_task = mock.Mock()
        adapter.complete_task = mock.Mock()
        listener.adapter = adapter
        if AIRFLOW_V_3_0_PLUS:
            task_instance = TaskInstance(task=mock.Mock(), dag_version_id=mock.MagicMock())
        else:
            task_instance = TaskInstance(task=mock.Mock())  # type: ignore
        task_instance.dag_run = DagRun()
        task_instance.dag_run.run_id = "dag_run_run_id"
        task_instance.dag_run.data_interval_start = None
        task_instance.dag_run.data_interval_end = None
        task_instance.dag_run.clear_number = 0
        task_instance.dag_run.execution_date = timezone.datetime(2020, 1, 1, 1, 1, 1)
        task_instance.task = mock.Mock()
        task_instance.task.task_id = "task_id_from_task_and_not_ti"
        task_instance.task.dag = mock.Mock()
        task_instance.task.dag.dag_id = "dag_id"
        task_instance.task.dag.description = "Test DAG Description"
        task_instance.task.dag.doc_md = None
        task_instance.task.dag.owner = "Test Owner"
        task_instance.task.dag.tags = ["tag1", "tag2"]
        task_instance.task.owner = "task_owner"
        task_instance.task.doc = None
        task_instance.task.doc_md = "Test TASK Description"
        task_instance.task.doc_json = None
        task_instance.task.doc_yaml = None
        task_instance.task.doc_rst = None
        task_instance.task.inlets = []
        task_instance.task.outlets = []
        task_instance.dag_id = "dag_id"
        task_instance.run_id = "dag_run_run_id"
        task_instance.task_id = "task_id"
        task_instance.try_number = 1
        task_instance.state = State.RUNNING
        task_instance.start_date = timezone.datetime(2023, 1, 1, 13, 1, 1)
        task_instance.end_date = timezone.datetime(2023, 1, 3, 13, 1, 1)
        task_instance.logical_date = timezone.datetime(2020, 1, 1, 1, 1, 1)
        task_instance.map_index = -1
        task_instance.next_method = None  # Ensure this is None to reach start_task
        task_instance.get_template_context = mock.MagicMock()  # type: ignore[method-assign]
        task_instance.get_template_context.return_value = defaultdict(mock.MagicMock)
        task_instance.get_template_context()["task_reschedule_count"] = 0

        return listener, task_instance

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_start_task_is_called_with_proper_arguments(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        """Tests that the 'start_task' method of the OpenLineageAdapter is invoked with the correct arguments.

        The test checks that the job name, job description, event time, and other related data are
        correctly passed to the adapter. It also verifies that custom facets and Airflow run facets are
        correctly retrieved and included in the call. This ensures that all relevant data, including custom
        and Airflow-specific metadata, is accurately conveyed to the adapter during the initialization of a task,
        reflecting the comprehensive tracking of task execution contexts."""

        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        listener.on_task_instance_running(None, task_instance, None)
        listener.adapter.start_task.assert_called_once_with(
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            job_name="dag_id.task_id",
            job_description="Test TASK Description",
            job_description_type="text/markdown",
            event_time="2023-01-01T13:01:01+00:00",
            nominal_start_time=None,
            nominal_end_time=None,
            owners=["task_owner"],
            tags=["tag1", "tag2"],
            task=listener.extractor_manager.extract_metadata(),
            run_facets={
                "parent": 4,
                "mapped_facet": 1,
                "custom_user_facet": 2,
                "airflow_run_facet": 3,
                "debug": "packages",
            },
        )

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_start_task_is_called_with_dag_owners_when_task_owner_is_default(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.owner = "airflow"  # Simulate default owner on task to force fallback to DAG owner
        listener.on_task_instance_running(None, task_instance, None)
        assert listener.adapter.start_task.call_args.kwargs["owners"] == ["Test Owner"]

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_job_name")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_start_task_is_called_with_dag_description_when_task_doc_is_empty(
        self,
        mock_get_job_name,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_job_name.return_value = "job_name"
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.doc_md = None  # Simulate lack of task doc to force fallback to DAG description
        listener.on_task_instance_running(None, task_instance, None)
        assert listener.adapter.start_task.call_args.kwargs["job_description"] == "Test DAG Description"
        assert listener.adapter.start_task.call_args.kwargs["job_description_type"] == "text/plain"

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_proper_arguments(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
        time_machine,
    ):
        """Tests that the 'fail_task' method of the OpenLineageAdapter is invoked with the correct arguments.

        This test ensures that the job name is accurately retrieved and included, along with the generated
        run_id and task metadata. By mocking the job name retrieval and the run_id generation,
        the test verifies the integrity and consistency of the data passed to the adapter during task
        failure events, thus confirming that the adapter's failure handling is functioning as expected.
        """
        time_machine.move_to(timezone.datetime(2023, 1, 3, 13, 1, 1))

        listener, task_instance = self._create_listener_and_task_instance()
        task_instance.logical_date = timezone.datetime(2020, 1, 1, 1, 1, 1)
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow": {"task": "..."}}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        err = ValueError("test")
        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, error=err, session=None
        )
        listener.adapter.fail_task.assert_called_once_with(
            end_time="2023-01-03T13:01:01+00:00",
            job_name="dag_id.task_id",
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            owners=["task_owner"],
            tags=["tag1", "tag2"],
            nominal_start_time=None,
            nominal_end_time=None,
            job_description="Test TASK Description",
            job_description_type="text/markdown",
            task=listener.extractor_manager.extract_metadata(),
            run_facets={
                "parent": 4,
                "custom_user_facet": 2,
                "airflow": {"task": "..."},
                "debug": "packages",
            },
            error=err,
        )

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_dag_owners_when_task_owner_is_default(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.owner = "airflow"  # Simulate default owner on task to force fallback to DAG owner
        err = ValueError("test")
        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, error=err, session=None
        )
        assert listener.adapter.fail_task.call_args.kwargs["owners"] == ["Test Owner"]

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_dag_description_when_task_doc_is_empty(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.doc_md = None  # Simulate lack of task doc to force fallback to DAG description
        err = ValueError("test")
        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, error=err, session=None
        )
        assert listener.adapter.fail_task.call_args.kwargs["job_description"] == "Test DAG Description"
        assert listener.adapter.fail_task.call_args.kwargs["job_description_type"] == "text/plain"

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_proper_arguments(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
        time_machine,
    ):
        """Tests that the 'complete_task' method of the OpenLineageAdapter is called with the correct arguments.

        It checks that the job name is correctly retrieved and passed,
        along with the run_id and task metadata. The test also simulates changes in the try_number
        attribute of the task instance, as it would occur in Airflow, to ensure that the run_id is updated
        accordingly. This helps confirm the consistency and correctness of the data passed to the adapter
        during the task's lifecycle events.
        """
        time_machine.move_to(timezone.datetime(2023, 1, 3, 13, 1, 1))

        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow": {"task": "..."}}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        listener.on_task_instance_success(None, task_instance, None)
        # This run_id will be different as we did NOT simulate increase of the try_number attribute,
        # which happens in Airflow < 2.10.
        calls = listener.adapter.complete_task.call_args_list
        assert len(calls) == 1
        assert calls[0][1] == dict(
            end_time="2023-01-03T13:01:01+00:00",
            job_name="dag_id.task_id",
            run_id=f"2020-01-01T01:01:01+00:00.dag_id.task_id.{EXPECTED_TRY_NUMBER_1}.-1",
            task=listener.extractor_manager.extract_metadata(),
            owners=["task_owner"],
            tags=["tag1", "tag2"],
            nominal_start_time=None,
            nominal_end_time=None,
            job_description="Test TASK Description",
            job_description_type="text/markdown",
            run_facets={
                "parent": 4,
                "custom_user_facet": 2,
                "airflow": {"task": "..."},
                "debug": "packages",
            },
        )

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_dag_owners_when_task_owner_is_default(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.owner = "airflow"  # Simulate default owner on task to force fallback to DAG owner
        listener.on_task_instance_success(None, task_instance, None)
        assert listener.adapter.complete_task.call_args.kwargs["owners"] == ["Test Owner"]

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_dag_description_when_task_doc_is_empty(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.doc_md = None  # Simulate lack of task doc to force fallback to DAG description
        listener.on_task_instance_success(None, task_instance, None)
        assert listener.adapter.complete_task.call_args.kwargs["job_description"] == "Test DAG Description"
        assert listener.adapter.complete_task.call_args.kwargs["job_description_type"] == "text/plain"

    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_on_task_instance_running_correctly_calls_openlineage_adapter_run_id_method(self):
        """Tests the OpenLineageListener's response when a task instance is in the running state.

        This test ensures that when an Airflow task instance transitions to the running state,
        the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
        parameters derived from the task instance.
        """
        listener, task_instance = self._create_listener_and_task_instance()
        listener.on_task_instance_running(None, task_instance, None)
        listener.adapter.build_task_instance_run_id.assert_called_once_with(
            dag_id="dag_id",
            task_id="task_id",
            logical_date=timezone.datetime(2020, 1, 1, 1, 1, 1),
            try_number=1,
            map_index=-1,
        )

    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_on_task_instance_failed_correctly_calls_openlineage_adapter_run_id_method(self):
        """Tests the OpenLineageListener's response when a task instance is in the failed state.

        This test ensures that when an Airflow task instance transitions to the failed state,
        the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
        parameters derived from the task instance.
        """
        listener, task_instance = self._create_listener_and_task_instance()
        on_task_failed_kwargs = {"error": ValueError("test")}

        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, **on_task_failed_kwargs, session=None
        )
        listener.adapter.build_task_instance_run_id.assert_called_once_with(
            dag_id="dag_id",
            task_id="task_id",
            logical_date=timezone.datetime(2020, 1, 1, 1, 1, 1),
            try_number=1,
            map_index=-1,
        )

    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_on_task_instance_success_correctly_calls_openlineage_adapter_run_id_method(self):
        """Tests the OpenLineageListener's response when a task instance is in the success state.

        This test ensures that when an Airflow task instance transitions to the success state,
        the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
        parameters derived from the task instance.
        """
        listener, task_instance = self._create_listener_and_task_instance()
        listener.on_task_instance_success(None, task_instance, None)
        listener.adapter.build_task_instance_run_id.assert_called_once_with(
            dag_id="dag_id",
            task_id="task_id",
            logical_date=timezone.datetime(2020, 1, 1, 1, 1, 1),
            try_number=EXPECTED_TRY_NUMBER_1,
            map_index=-1,
        )

    @mock.patch("airflow.models.taskinstance.get_listener_manager")
    def test_listener_on_task_instance_failed_is_called_before_try_number_increment(self, mock_listener):
        """Validates the listener's on-failure method is called before try_number increment happens.

        This test ensures that when a task instance fails, Airflow's listener method for
        task failure (`on_task_instance_failed`) is invoked before the increment of the
        `try_number` attribute happens. A custom exception simulates task failure, and the test
        captures the `try_number` at the moment of this method call.
        """
        captured_try_numbers = {}
        self._setup_mock_listener(mock_listener, captured_try_numbers)

        # Just to make sure no error interferes with the test, and we do not suppress it by accident
        class CustomError(Exception):
            pass

        def fail_callable(**kwargs):
            raise CustomError("Simulated task failure")

        _, task_instance = self._create_test_dag_and_task(fail_callable, "failure")
        # try_number before execution
        assert task_instance.try_number == TRY_NUMBER_BEFORE_EXECUTION
        with suppress(CustomError):
            task_instance.run()

        # try_number at the moment of function being called
        assert captured_try_numbers["running"] == TRY_NUMBER_RUNNING
        assert captured_try_numbers["failed"] == TRY_NUMBER_FAILED

        # try_number after task has been executed
        assert task_instance.try_number == TRY_NUMBER_AFTER_EXECUTION

    @mock.patch("airflow.models.taskinstance.get_listener_manager")
    def test_listener_on_task_instance_success_is_called_after_try_number_increment(self, mock_listener):
        """Validates the listener's on-success method is called before try_number increment happens.

        This test ensures that when a task instance successfully completes, the
        `on_task_instance_success` method of Airflow's listener is called with an
        incremented `try_number` compared to the `try_number` before execution.
        The test simulates a successful task execution and captures the `try_number` at the method call.
        """
        captured_try_numbers = {}
        self._setup_mock_listener(mock_listener, captured_try_numbers)

        def success_callable(**kwargs):
            return None

        _, task_instance = self._create_test_dag_and_task(success_callable, "success")
        # try_number before execution
        assert task_instance.try_number == TRY_NUMBER_BEFORE_EXECUTION
        task_instance.run()

        # try_number at the moment of function being called
        assert captured_try_numbers["running"] == TRY_NUMBER_RUNNING
        assert captured_try_numbers["success"] == TRY_NUMBER_SUCCESS

        # try_number after task has been executed
        assert task_instance.try_number == TRY_NUMBER_AFTER_EXECUTION

    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    def test_listener_on_task_instance_running_do_not_call_adapter_when_disabled_operator(
        self, mock_get_user_provided_run_facets, mock_get_airflow_run_facet, mock_disabled
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_facet": 2}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_disabled.return_value = True

        listener.on_task_instance_running(None, task_instance, None)
        mock_disabled.assert_called_once_with(task_instance.task)
        listener.adapter.build_dag_run_id.assert_not_called()
        listener.adapter.build_task_instance_run_id.assert_not_called()
        listener.extractor_manager.extract_metadata.assert_not_called()
        listener.adapter.start_task.assert_not_called()

    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    def test_listener_on_task_instance_failed_do_not_call_adapter_when_disabled_operator(
        self, mock_get_user_provided_run_facets, mock_disabled
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_facet": 2}
        mock_disabled.return_value = True

        on_task_failed_kwargs = {"error": ValueError("test")}

        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, **on_task_failed_kwargs, session=None
        )
        mock_disabled.assert_called_once_with(task_instance.task)
        listener.adapter.build_dag_run_id.assert_not_called()
        listener.adapter.build_task_instance_run_id.assert_not_called()
        listener.extractor_manager.extract_metadata.assert_not_called()
        listener.adapter.fail_task.assert_not_called()

    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    def test_listener_on_task_instance_success_do_not_call_adapter_when_disabled_operator(
        self, mock_get_user_provided_run_facets, mock_disabled
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_facet": 2}
        mock_disabled.return_value = True

        listener.on_task_instance_success(None, task_instance, None)
        mock_disabled.assert_called_once_with(task_instance.task)
        listener.adapter.build_dag_run_id.assert_not_called()
        listener.adapter.build_task_instance_run_id.assert_not_called()
        listener.extractor_manager.extract_metadata.assert_not_called()
        listener.adapter.complete_task.assert_not_called()

    @pytest.mark.parametrize(
        ("max_workers", "expected"),
        [
            (None, 1),
            ("8", 8),
        ],
    )
    @mock.patch("airflow.providers.openlineage.plugins.listener.ProcessPoolExecutor", autospec=True)
    def test_listener_on_dag_run_state_changes_configure_process_pool_size(
        self, mock_executor, max_workers, expected
    ):
        """mock ProcessPoolExecutor and check if conf.dag_state_change_process_pool_size is applied to max_workers"""
        listener = OpenLineageListener()
        # mock ProcessPoolExecutor class
        with conf_vars({("openlineage", "dag_state_change_process_pool_size"): max_workers}):
            listener.on_dag_run_running(mock.MagicMock(), None)
        mock_executor.assert_called_once_with(max_workers=expected, initializer=mock.ANY)
        mock_executor.return_value.submit.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("method", "dag_run_state"),
        [
            ("on_dag_run_running", DagRunState.RUNNING),
            ("on_dag_run_success", DagRunState.SUCCESS),
            ("on_dag_run_failed", DagRunState.FAILED),
        ],
    )
    @patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.emit")
    def test_listener_on_dag_run_state_changes(self, mock_emit, method, dag_run_state, create_task_instance):
        mock_executor = MockExecutor()
        ti = create_task_instance(dag_id="dag", task_id="op")
        ti.start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        ti.end_date = datetime(2020, 1, 1, 1, tzinfo=timezone.utc)
        # Change the state explicitly to set end_date following the logic in the method
        ti.dag_run.set_state(dag_run_state)
        with mock.patch(
            "airflow.providers.openlineage.plugins.listener.ProcessPoolExecutor", return_value=mock_executor
        ):
            listener = OpenLineageListener()
            getattr(listener, method)(ti.dag_run, None)
            assert mock_executor.submitted is True
            assert mock_executor.succeeded is True
            mock_emit.assert_called_once()

    def test_listener_logs_failed_serialization(self):
        listener = OpenLineageListener()
        callback_future = Future()

        def set_result(*args, **kwargs):
            callback_future.set_result(True)

        listener.log = MagicMock()
        listener.log.warning = MagicMock(side_effect=set_result)
        listener.adapter = OpenLineageAdapter(
            client=OpenLineageClient(transport=ConsoleTransport(config=ConsoleConfig()))
        )
        event_time = timezone.utcnow()
        fut = listener.submit_callable(
            listener.adapter.dag_failed,
            dag_id="",
            run_id="",
            end_date=event_time,
            logical_date=callback_future,
            clear_number=0,
            dag_run_state=DagRunState.FAILED,
            task_ids=["task_id"],
            msg="",
        )
        assert fut.exception(10)
        callback_future.result(10)
        assert callback_future.done()
        listener.log.debug.assert_not_called()
        listener.log.warning.assert_called_once()


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 tests")
class TestOpenLineageListenerAirflow3:
    @pytest.mark.skip("Rendering fields is not migrated yet in Airflow 3")
    @patch("airflow.models.BaseOperator.render_template")
    def test_listener_does_not_change_task_instance(self, render_mock, mock_supervisor_comms, spy_agency):
        from airflow.sdk.execution_time.task_runner import (
            RuntimeTaskInstance,
            TaskInstance as SdkTaskInstance,
            run,
        )

        render_mock.return_value = render_df()

        date = timezone.datetime(2022, 1, 1)
        dag = DAG(
            "test",
            schedule=None,
            start_date=date,
            user_defined_macros={"render_df": render_df},
            params={"df": {"col": [1, 2]}},
        )
        task = TemplateOperator(task_id="template_op", dag=dag, do_xcom_push=True, df=dag.param("df"))
        run_id = str(uuid.uuid1())

        dagrun_kwargs = {
            "dag_version": None,
            "logical_date": date,
            "triggered_by": types.DagRunTriggeredByType.TEST,
        }

        dag.create_dagrun(
            run_id=run_id,
            data_interval=(date, date),
            run_type=types.DagRunType.MANUAL,
            state=DagRunState.QUEUED,
            **dagrun_kwargs,
        )
        ti = SdkTaskInstance(
            id=uuid7(),
            task_id="template_op",
            dag_id=dag.dag_id,
            run_id=run_id,
            try_number=1,
            map_index=-1,
            dag_version_id=uuid7(),
        )

        runtime_ti = RuntimeTaskInstance.model_construct(**ti.model_dump(exclude_unset=True), task=task)

        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)
        run(runtime_ti, None)

        # check if task returns the same DataFrame
        pd.testing.assert_frame_equal(runtime_ti.xcom_push.last_call.args[1], render_df())

        # check if render_template method always get the same unrendered field
        assert not isinstance(runtime_ti.xcom_push.last_call.args[1], pd.DataFrame)

    def _setup_mock_listener(self, mock_listener: mock.Mock, captured_try_numbers: dict[str, int]) -> None:
        """Sets up the mock listener with side effects to capture try numbers for different task instance events.

        :param mock_listener: The mock object for the listener manager.
        :param captured_try_numbers: A dictionary to store captured try numbers keyed by event names.

        This function iterates through specified event names and sets a side effect on the corresponding
        method of the listener manager's hook. The side effect is a nested function that captures the try number
        of the task instance when the method is called.

        :Example:

            captured_try_numbers = {}
            mock_listener = Mock()
            _setup_mock_listener(mock_listener, captured_try_numbers)
            # After running a task, captured_try_numbers will have the try number captured at the moment of
            execution for specified methods. F.e. {"running": 1, "success": 2} for on_task_instance_running and
            on_task_instance_success methods.
        """

        def capture_try_number(method_name):
            def inner(*args, **kwargs):
                captured_try_numbers[method_name] = kwargs["task_instance"].try_number

            return inner

        for event in ["running", "success", "failed"]:
            getattr(
                mock_listener.return_value.hook, f"on_task_instance_{event}"
            ).side_effect = capture_try_number(event)

    def _create_test_dag_and_task(
        self, python_callable: Callable, scenario_name: str
    ) -> tuple[DagRun, TaskInstance]:
        """Creates a test DAG and a task for a custom test scenario.

        :param python_callable: The Python callable to be executed by the PythonOperator.
        :param scenario_name: The name of the test scenario, used to uniquely name the DAG and task.

        :return: TaskInstance: The created TaskInstance object.

        This function creates a DAG and a PythonOperator task with the provided python_callable. It generates a unique
        run ID and creates a DAG run. This setup is useful for testing different scenarios in Airflow tasks.

        :Example:

            def sample_callable(**kwargs):
                print("Hello World")

            task_instance = _create_test_dag_and_task(sample_callable, "sample_scenario")
            # Use task_instance to simulate running a task in a test.
        """
        date = timezone.datetime(2022, 1, 1)
        dag = DAG(
            f"test_{scenario_name}",
            schedule=None,
            start_date=date,
        )
        t = PythonOperator(task_id=f"test_task_{scenario_name}", dag=dag, python_callable=python_callable)
        run_id = str(uuid.uuid1())
        dagrun_kwargs = {
            "dag_version": None,
            "logical_date": date,
            "triggered_by": types.DagRunTriggeredByType.TEST,
        }

        dagrun = create_scheduler_dag(dag).create_dagrun(
            run_id=run_id,
            data_interval=(date, date),
            start_date=date,
            run_type=types.DagRunType.MANUAL,
            state=DagRunState.QUEUED,
            **dagrun_kwargs,
        )
        task_instance = TaskInstance(t, run_id=run_id)  # type: ignore
        task_instance.dag_run = dagrun
        return dagrun, task_instance

    def _create_listener_and_task_instance(
        self, runtime_ti: bool = True
    ) -> tuple[OpenLineageListener, RuntimeTaskInstance | TaskInstance]:
        """Creates and configures an OpenLineageListener instance and a mock task instance for testing.

        :arg runtime_ti: Whether we should return mock RuntimeTaskInstance or mock TaskInstance
        :return: A tuple containing the configured OpenLineageListener and task instance.

        This function instantiates an OpenLineageListener, sets up its required properties with mock objects, and
        creates a mock task instance with predefined attributes. This setup is commonly used for testing the
        interaction between an OpenLineageListener and a task instance in Airflow.

        :Example:

            listener, task_instance = _create_listener_and_task_instance()
            # Now you can use listener and task_instance in your tests to simulate their interaction.
        """

        if not runtime_ti:
            # TaskInstance is used when on API server (when listener gets called about manual state change)
            task_instance = TaskInstance(task=MagicMock(), dag_version_id=uuid7())
            task_instance.dag_run = DagRun()
            task_instance.dag_run.dag_id = "dag_id_from_dagrun_and_not_ti"
            task_instance.dag_run.run_id = "dag_run_run_id"
            task_instance.dag_run.clear_number = 0
            task_instance.dag_run.logical_date = timezone.datetime(2020, 1, 1, 1, 1, 1)
            task_instance.dag_run.run_after = timezone.datetime(2020, 1, 1, 1, 1, 1)
            task_instance.dag_run.state = DagRunState.RUNNING
            task_instance.task = None
            task_instance.dag = None
            task_instance.task_id = "task_id"
            task_instance.dag_id = "dag_id"
            task_instance.try_number = 1
            task_instance.map_index = -1
        else:
            # RuntimeTaskInstance is used when on worker
            from airflow.sdk.api.datamodels._generated import (
                DagRun as SdkDagRun,
                DagRunState as SdkDagRunState,
                DagRunType,
                TaskInstance as SdkTaskInstance,
                TIRunContext,
            )
            from airflow.sdk.definitions.dag import DAG
            from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

            dag = DAG(
                dag_id="dag_id_from_dag_not_ti",
                description="Test DAG Description",
                tags=["tag1", "tag2"],
            )
            task = EmptyOperator(
                task_id="task_id_from_task_not_ti", dag=dag, owner="task_owner", doc_md="TASK Description"
            )
            task2 = EmptyOperator(task_id="task_id2_from_task_not_ti", dag=dag, owner="another_owner")  # noqa: F841

            sdk_task_instance = SdkTaskInstance(
                id=uuid7(),
                task_id="task_id",
                dag_id="dag_id",
                run_id="dag_run_run_id",
                try_number=1,
                map_index=-1,
                dag_version_id=uuid7(),
            )
            task_instance = RuntimeTaskInstance.model_construct(  # type: ignore
                **sdk_task_instance.model_dump(exclude_unset=True),
                task=task,
                _ti_context_from_server=TIRunContext(
                    dag_run=SdkDagRun.model_validate(
                        {
                            "dag_id": "dag_id_from_dagrun_not_ti",
                            "run_id": "dag_run_run_id_from_dagrun_not_ti",
                            "logical_date": timezone.datetime(2020, 1, 1, 1, 1, 1),
                            "start_date": timezone.datetime(2023, 1, 1, 13, 1, 1),
                            "end_date": timezone.datetime(2023, 1, 3, 13, 1, 1),
                            "run_type": DagRunType.MANUAL,
                            "run_after": timezone.datetime(2023, 1, 3, 13, 1, 1),
                            "consumed_asset_events": [],
                            **(
                                {"state": SdkDagRunState.RUNNING} if "state" in SdkDagRun.model_fields else {}
                            ),
                        }
                    ),
                    task_reschedule_count=0,
                    max_tries=1,
                    should_retry=False,
                ),
                start_date=timezone.datetime(2023, 1, 1, 13, 1, 1),
            )

        def mock_dag_id(dag_id, logical_date, clear_number):
            return f"{logical_date.isoformat()}.{dag_id}.{clear_number}"

        def mock_task_id(dag_id, task_id, try_number, logical_date, map_index):
            return f"{logical_date.isoformat()}.{dag_id}.{task_id}.{try_number}.{map_index}"

        listener = OpenLineageListener()
        listener.extractor_manager = mock.Mock()

        metadata = mock.Mock()
        metadata.run_facets = {"run_facet": 1}
        listener.extractor_manager.extract_metadata.return_value = metadata

        adapter = mock.Mock()
        adapter.build_dag_run_id.side_effect = mock_dag_id
        adapter.build_task_instance_run_id.side_effect = mock_task_id
        adapter.start_task = mock.Mock()
        adapter.fail_task = mock.Mock()
        adapter.complete_task = mock.Mock()
        listener.adapter = adapter

        return listener, task_instance

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_start_task_is_called_with_proper_arguments(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        """Tests that the 'start_task' method of the OpenLineageAdapter is invoked with the correct arguments.

        The test checks that the job name, job description, event time, and other related data are
        correctly passed to the adapter. It also verifies that custom facets and Airflow run facets are
        correctly retrieved and included in the call. This ensures that all relevant data, including custom
        and Airflow-specific metadata, is accurately conveyed to the adapter during the initialization of a task,
        reflecting the comprehensive tracking of task execution contexts."""

        listener, task_instance = self._create_listener_and_task_instance()

        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        listener.on_task_instance_running(None, task_instance)
        listener.adapter.start_task.assert_called_once_with(
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            job_name="dag_id.task_id",
            job_description="TASK Description",
            job_description_type="text/markdown",
            event_time="2023-01-01T13:01:01+00:00",
            nominal_start_time=None,
            nominal_end_time=None,
            owners=["task_owner"],
            tags={"tag1", "tag2"},
            task=listener.extractor_manager.extract_metadata(),
            run_facets={
                "mapped_facet": 1,
                "custom_user_facet": 2,
                "airflow_run_facet": 3,
                "parent": 4,
                "debug": "packages",
            },
        )

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_start_task_is_called_with_dag_owners_when_task_owner_is_default(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.owner = "airflow"  # Simulate default owner on task to force fallback to DAG owner
        listener.on_task_instance_running(None, task_instance)
        call_owners = listener.adapter.start_task.call_args.kwargs["owners"]
        assert sorted(call_owners) == ["airflow", "another_owner"]

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_job_name")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_start_task_is_called_with_dag_description_when_task_doc_is_empty(
        self,
        mock_get_job_name,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_job_name.return_value = "job_name"
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.doc_md = None  # Simulate lack of task doc to force fallback to DAG description
        listener.on_task_instance_running(None, task_instance)
        assert listener.adapter.start_task.call_args.kwargs["job_description"] == "Test DAG Description"
        assert listener.adapter.start_task.call_args.kwargs["job_description_type"] == "text/plain"

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_proper_arguments(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
        time_machine,
    ):
        """Tests that the 'fail_task' method of the OpenLineageAdapter is invoked with the correct arguments.

        This test ensures that the job name is accurately retrieved and included, along with the generated
        run_id and task metadata. By mocking the job name retrieval and the run_id generation,
        the test verifies the integrity and consistency of the data passed to the adapter during task
        failure events, thus confirming that the adapter's failure handling is functioning as expected.
        """
        time_machine.move_to(timezone.datetime(2023, 1, 3, 13, 1, 1), tick=False)

        listener, task_instance = self._create_listener_and_task_instance()
        task_instance.get_template_context()["dag_run"].logical_date = timezone.datetime(2020, 1, 1, 1, 1, 1)
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow": {"task": "..."}}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        err = ValueError("test")
        listener.on_task_instance_failed(previous_state=None, task_instance=task_instance, error=err)
        listener.adapter.fail_task.assert_called_once_with(
            end_time="2023-01-03T13:01:01+00:00",
            job_name="dag_id.task_id",
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            task=listener.extractor_manager.extract_metadata(),
            owners=["task_owner"],
            tags={"tag1", "tag2"},
            job_description="TASK Description",
            job_description_type="text/markdown",
            nominal_start_time=None,
            nominal_end_time=None,
            run_facets={
                "parent": 4,
                "custom_user_facet": 2,
                "airflow": {"task": "..."},
                "debug": "packages",
            },
            error=err,
        )

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_dag_owners_when_task_owner_is_default(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.owner = "airflow"  # Simulate default owner on task to force fallback to DAG owner
        err = ValueError("test")
        listener.on_task_instance_failed(previous_state=None, task_instance=task_instance, error=err)

        call_owners = listener.adapter.fail_task.call_args.kwargs["owners"]
        assert sorted(call_owners) == ["airflow", "another_owner"]

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_dag_description_when_task_doc_is_empty(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.doc_md = None  # Simulate lack of task doc to force fallback to DAG description
        err = ValueError("test")
        listener.on_task_instance_failed(previous_state=None, task_instance=task_instance, error=err)
        assert listener.adapter.fail_task.call_args.kwargs["job_description"] == "Test DAG Description"
        assert listener.adapter.fail_task.call_args.kwargs["job_description_type"] == "text/plain"

    @mock.patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.emit")
    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_fail_task_is_called_with_proper_arguments_for_db_task_instance_model(
        self,
        mock_get_task_parent_run_facet,
        mock_debug_facet,
        mock_debug_mode,
        mock_emit,
        time_machine,
    ):
        """Tests that the 'fail_task' method of the OpenLineageAdapter is invoked with the correct arguments.

        This particular test is using TaskInstance model available on API Server and not on worker,
        to simulate the listener being called after task's state has been manually set via API.
        """
        time_machine.move_to(timezone.datetime(2023, 1, 3, 13, 1, 1), tick=False)

        listener, task_instance = self._create_listener_and_task_instance(runtime_ti=False)
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}

        err = ValueError("test")

        listener.on_task_instance_failed(previous_state=None, task_instance=task_instance, error=err)
        mock_get_task_parent_run_facet.assert_called_once_with(
            parent_run_id="2020-01-01T01:01:01+00:00.dag_id.0",
            parent_job_name=task_instance.dag_id,
            dr_conf={},
        )
        expected_args = dict(
            end_time="2023-01-03T13:01:01+00:00",
            job_name="dag_id.task_id",
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            task=OperatorLineage(),
            nominal_start_time=None,
            nominal_end_time=None,
            tags=None,
            owners=None,
            job_description=None,
            job_description_type=None,
            run_facets={
                "parent": 4,
                "debug": "packages",
            },
            error=err,
        )
        listener.adapter.fail_task.assert_called_once_with(**expected_args)

        expected_args["run_id"] = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
        adapter = OpenLineageAdapter()
        adapter.fail_task(**expected_args)
        assert mock_emit.assert_called_once

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_proper_arguments(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
        time_machine,
    ):
        """Tests that the 'complete_task' method of the OpenLineageAdapter is called with the correct arguments.

        It checks that the job name is correctly retrieved and passed,
        along with the run_id and task metadata. The test also simulates changes in the try_number
        attribute of the task instance, as it would occur in Airflow, to ensure that the run_id is updated
        accordingly. This helps confirm the consistency and correctness of the data passed to the adapter
        during the task's lifecycle events.
        """
        time_machine.move_to(timezone.datetime(2023, 1, 3, 13, 1, 1), tick=False)

        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow": {"task": "..."}}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        listener.on_task_instance_success(None, task_instance)
        calls = listener.adapter.complete_task.call_args_list
        assert len(calls) == 1
        assert calls[0][1] == dict(
            end_time="2023-01-03T13:01:01+00:00",
            job_name="dag_id.task_id",
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            task=listener.extractor_manager.extract_metadata(),
            owners=["task_owner"],
            tags={"tag1", "tag2"},
            job_description="TASK Description",
            job_description_type="text/markdown",
            nominal_start_time=None,
            nominal_end_time=None,
            run_facets={
                "parent": 4,
                "custom_user_facet": 2,
                "airflow": {"task": "..."},
                "debug": "packages",
            },
        )

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_dag_owners_when_task_owner_is_default(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.owner = "airflow"  # Simulate default owner on task to force fallback to DAG owner
        listener.on_task_instance_success(None, task_instance)

        call_owners = listener.adapter.complete_task.call_args.kwargs["owners"]
        assert sorted(call_owners) == ["airflow", "another_owner"]

    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_mapped_task_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_dag_description_when_task_doc_is_empty(
        self,
        mock_get_user_provided_run_facets,
        mock_get_airflow_mapped_task_facet,
        mock_get_airflow_run_facet,
        mock_get_task_parent_run_facet,
        mock_disabled,
        mock_debug_facet,
        mock_debug_mode,
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_airflow_mapped_task_facet.return_value = {"mapped_facet": 1}
        mock_get_user_provided_run_facets.return_value = {"custom_user_facet": 2, "parent": 99}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}
        mock_disabled.return_value = False

        task_instance.task.doc_md = None  # Simulate lack of task doc to force fallback to DAG description
        listener.on_task_instance_success(None, task_instance)
        assert listener.adapter.complete_task.call_args.kwargs["job_description"] == "Test DAG Description"
        assert listener.adapter.complete_task.call_args.kwargs["job_description_type"] == "text/plain"

    @mock.patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.emit")
    @mock.patch("airflow.providers.openlineage.conf.debug_mode", return_value=True)
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_debug_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_task_parent_run_facet")
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_adapter_complete_task_is_called_with_proper_arguments_for_db_task_instance_model(
        self, mock_get_task_parent_run_facet, mock_debug_facet, mock_debug_mode, mock_emit, time_machine
    ):
        """Tests that the 'complete_task' method of the OpenLineageAdapter is called with the correct arguments.

        This particular test is using TaskInstance model available on API Server and not on worker,
        to simulate the listener being called after task's state has been manually set via API.
        """
        time_machine.move_to(timezone.datetime(2023, 1, 3, 13, 1, 1), tick=False)

        listener, task_instance = self._create_listener_and_task_instance(runtime_ti=False)
        mock_get_task_parent_run_facet.return_value = {"parent": 4}
        mock_debug_facet.return_value = {"debug": "packages"}

        listener.on_task_instance_success(None, task_instance)
        calls = listener.adapter.complete_task.call_args_list
        assert len(calls) == 1
        mock_get_task_parent_run_facet.assert_called_once_with(
            parent_run_id="2020-01-01T01:01:01+00:00.dag_id.0",
            parent_job_name=task_instance.dag_id,
            dr_conf={},
        )
        expected_args = dict(
            end_time="2023-01-03T13:01:01+00:00",
            job_name="dag_id.task_id",
            run_id="2020-01-01T01:01:01+00:00.dag_id.task_id.1.-1",
            task=OperatorLineage(),
            nominal_start_time=None,
            nominal_end_time=None,
            tags=None,
            owners=None,
            job_description=None,
            job_description_type=None,
            run_facets={
                "parent": 4,
                "debug": "packages",
            },
        )
        assert calls[0][1] == expected_args

        expected_args["run_id"] = "9d3b14f7-de91-40b6-aeef-e887e2c7673e"
        adapter = OpenLineageAdapter()
        adapter.complete_task(**expected_args)
        assert mock_emit.assert_called_once

    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_on_task_instance_running_correctly_calls_openlineage_adapter_run_id_method(self):
        """Tests the OpenLineageListener's response when a task instance is in the running state.

        This test ensures that when an Airflow task instance transitions to the running state,
        the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
        parameters derived from the task instance.
        """
        listener, task_instance = self._create_listener_and_task_instance()
        listener.on_task_instance_running(None, task_instance)
        listener.adapter.build_task_instance_run_id.assert_called_once_with(
            dag_id="dag_id",
            task_id="task_id",
            logical_date=timezone.datetime(2020, 1, 1, 1, 1, 1),
            try_number=1,
            map_index=-1,
        )

    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_on_task_instance_failed_correctly_calls_openlineage_adapter_run_id_method(self):
        """Tests the OpenLineageListener's response when a task instance is in the failed state.

        This test ensures that when an Airflow task instance transitions to the failed state,
        the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
        parameters derived from the task instance.
        """
        listener, task_instance = self._create_listener_and_task_instance()
        on_task_failed_kwargs = {"error": ValueError("test")}

        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, **on_task_failed_kwargs
        )
        listener.adapter.build_task_instance_run_id.assert_called_once_with(
            dag_id="dag_id",
            task_id="task_id",
            logical_date=timezone.datetime(2020, 1, 1, 1, 1, 1),
            try_number=1,
            map_index=-1,
        )

    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_on_task_instance_success_correctly_calls_openlineage_adapter_run_id_method(self):
        """Tests the OpenLineageListener's response when a task instance is in the success state.

        This test ensures that when an Airflow task instance transitions to the success state,
        the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
        parameters derived from the task instance.
        """
        listener, task_instance = self._create_listener_and_task_instance()
        listener.on_task_instance_success(None, task_instance)
        listener.adapter.build_task_instance_run_id.assert_called_once_with(
            dag_id="dag_id",
            task_id="task_id",
            logical_date=timezone.datetime(2020, 1, 1, 1, 1, 1),
            try_number=1,
            map_index=-1,
        )

    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    def test_listener_on_task_instance_running_do_not_call_adapter_when_disabled_operator(
        self, mock_get_user_provided_run_facets, mock_get_airflow_run_facet, mock_disabled
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_facet": 2}
        mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}
        mock_disabled.return_value = True

        listener.on_task_instance_running(None, task_instance)
        mock_disabled.assert_called_once_with(task_instance.task)
        listener.adapter.build_dag_run_id.assert_not_called()
        listener.adapter.build_task_instance_run_id.assert_not_called()
        listener.extractor_manager.extract_metadata.assert_not_called()
        listener.adapter.start_task.assert_not_called()

    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    def test_listener_on_task_instance_failed_do_not_call_adapter_when_disabled_operator(
        self, mock_get_user_provided_run_facets, mock_disabled
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_facet": 2}
        mock_disabled.return_value = True

        on_task_failed_kwargs = {"error": ValueError("test")}

        listener.on_task_instance_failed(
            previous_state=None, task_instance=task_instance, **on_task_failed_kwargs
        )
        mock_disabled.assert_called_once_with(task_instance.task)
        listener.adapter.build_dag_run_id.assert_not_called()
        listener.adapter.build_task_instance_run_id.assert_not_called()
        listener.extractor_manager.extract_metadata.assert_not_called()
        listener.adapter.fail_task.assert_not_called()

    @mock.patch("airflow.providers.openlineage.plugins.listener.is_operator_disabled")
    @mock.patch("airflow.providers.openlineage.plugins.listener.get_user_provided_run_facets")
    def test_listener_on_task_instance_success_do_not_call_adapter_when_disabled_operator(
        self, mock_get_user_provided_run_facets, mock_disabled
    ):
        listener, task_instance = self._create_listener_and_task_instance()
        mock_get_user_provided_run_facets.return_value = {"custom_facet": 2}
        mock_disabled.return_value = True

        listener.on_task_instance_success(None, task_instance)
        mock_disabled.assert_called_once_with(task_instance.task)
        listener.adapter.build_dag_run_id.assert_not_called()
        listener.adapter.build_task_instance_run_id.assert_not_called()
        listener.extractor_manager.extract_metadata.assert_not_called()
        listener.adapter.complete_task.assert_not_called()

    @pytest.mark.parametrize(
        ("max_workers", "expected"),
        [
            (None, 1),
            ("8", 8),
        ],
    )
    @mock.patch("airflow.providers.openlineage.plugins.listener.ProcessPoolExecutor", autospec=True)
    def test_listener_on_dag_run_state_changes_configure_process_pool_size(
        self, mock_executor, max_workers, expected
    ):
        """mock ProcessPoolExecutor and check if conf.dag_state_change_process_pool_size is applied to max_workers"""
        listener = OpenLineageListener()
        # mock ProcessPoolExecutor class
        with conf_vars({("openlineage", "dag_state_change_process_pool_size"): max_workers}):
            listener.on_dag_run_running(mock.MagicMock(), None)
        mock_executor.assert_called_once_with(max_workers=expected, initializer=mock.ANY)
        mock_executor.return_value.submit.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("method", "dag_run_state"),
        [
            ("on_dag_run_running", DagRunState.RUNNING),
            ("on_dag_run_success", DagRunState.SUCCESS),
            ("on_dag_run_failed", DagRunState.FAILED),
        ],
    )
    @patch("airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter.emit")
    def test_listener_on_dag_run_state_changes(self, mock_emit, method, dag_run_state, create_task_instance):
        mock_executor = MockExecutor()
        ti = create_task_instance(dag_id="dag", task_id="op")
        ti.start_date = datetime(2020, 1, 1, tzinfo=timezone.utc)
        ti.end_date = datetime(2020, 1, 1, 1, tzinfo=timezone.utc)
        # Change the state explicitly to set end_date following the logic in the method
        ti.dag_run.set_state(dag_run_state)
        with mock.patch(
            "airflow.providers.openlineage.plugins.listener.ProcessPoolExecutor", return_value=mock_executor
        ):
            listener = OpenLineageListener()
            getattr(listener, method)(ti.dag_run, None)
            assert mock_executor.submitted is True
            assert mock_executor.succeeded is True
            mock_emit.assert_called_once()

    def test_listener_logs_failed_serialization(self):
        listener = OpenLineageListener()
        callback_future = Future()

        def set_result(*args, **kwargs):
            callback_future.set_result(True)

        listener.log = MagicMock()
        listener.log.warning = MagicMock(side_effect=set_result)
        listener.adapter = OpenLineageAdapter(
            client=OpenLineageClient(transport=ConsoleTransport(config=ConsoleConfig()))
        )
        event_time = timezone.utcnow()
        fut = listener.submit_callable(
            listener.adapter.dag_failed,
            dag_id="",
            run_id="",
            end_date=event_time,
            logical_date=callback_future,
            clear_number=0,
            dag_run_state=DagRunState.FAILED,
            task_ids=["task_id"],
            msg="",
        )
        assert fut.exception(10)
        callback_future.result(10)
        assert callback_future.done()
        listener.log.debug.assert_not_called()
        listener.log.warning.assert_called_once()


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 tests")
class TestOpenLineageSelectiveEnableAirflow2:
    def setup_method(self):
        date = timezone.datetime(2022, 1, 1)
        self.dag = DAG(
            "test_selective_enable",
            schedule=None,
            start_date=date,
        )

        def simple_callable(**kwargs):
            return None

        self.task_1 = PythonOperator(
            task_id="test_task_selective_enable_1", dag=self.dag, python_callable=simple_callable
        )
        self.task_2 = PythonOperator(
            task_id="test_task_selective_enable_2", dag=self.dag, python_callable=simple_callable
        )
        run_id = str(uuid.uuid1())
        self.dagrun = self.dag.create_dagrun(
            run_id=run_id,
            data_interval=(date, date),
            run_type=types.DagRunType.MANUAL,
            state=DagRunState.QUEUED,
            execution_date=date,
        )
        self.task_instance_1 = TaskInstance(self.task_1, run_id=run_id, map_index=-1)
        self.task_instance_2 = TaskInstance(self.task_2, run_id=run_id, map_index=-1)
        self.task_instance_1.dag_run = self.task_instance_2.dag_run = self.dagrun

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize(
        ("selective_enable", "enable_dag", "expected_call_count"),
        [
            ("True", True, 3),
            ("False", True, 3),
            ("True", False, 0),
            ("False", False, 3),
        ],
    )
    def test_listener_with_dag_enabled(self, selective_enable, enable_dag, expected_call_count):
        """Tests listener's behaviour with selective-enable on DAG level."""

        if enable_dag:
            enable_lineage(self.dag)

        with conf_vars({("openlineage", "selective_enable"): selective_enable}):
            listener = OpenLineageListener()
            listener._executor = mock.Mock()

            # run all three DagRun-related hooks
            listener.on_dag_run_running(self.dagrun, msg="test running")
            listener.on_dag_run_failed(self.dagrun, msg="test failure")
            listener.on_dag_run_success(self.dagrun, msg="test success")

    @pytest.mark.parametrize(
        ("selective_enable", "enable_task", "expected_dag_call_count", "expected_task_call_count"),
        [
            ("True", True, 3, 3),
            ("False", True, 3, 3),
            ("True", False, 0, 0),
            ("False", False, 3, 3),
        ],
    )
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_listener_with_task_enabled(
        self, selective_enable, enable_task, expected_dag_call_count, expected_task_call_count
    ):
        """Tests listener's behaviour with selective-enable on task level."""

        if enable_task:
            enable_lineage(self.task_1)

        on_task_failed_kwargs = {"error": ValueError("test")}

        with conf_vars({("openlineage", "selective_enable"): selective_enable}):
            listener = OpenLineageListener()
            listener._executor = mock.Mock()
            listener.extractor_manager = mock.Mock()
            listener.adapter = mock.Mock()

            # run all three DagRun-related hooks
            listener.on_dag_run_running(self.dagrun, msg="test running")
            listener.on_dag_run_failed(self.dagrun, msg="test failure")
            listener.on_dag_run_success(self.dagrun, msg="test success")

            assert expected_dag_call_count == listener._executor.submit.call_count

            # run TaskInstance-related hooks for lineage enabled task
            listener.on_task_instance_running(None, self.task_instance_1, None)
            listener.on_task_instance_success(None, self.task_instance_1, None)
            listener.on_task_instance_failed(
                previous_state=None, task_instance=self.task_instance_1, **on_task_failed_kwargs, session=None
            )

            assert expected_task_call_count == listener.extractor_manager.extract_metadata.call_count

            # run TaskInstance-related hooks for lineage disabled task
            listener.on_task_instance_running(None, self.task_instance_2, None)
            listener.on_task_instance_success(None, self.task_instance_2, None)
            listener.on_task_instance_failed(
                previous_state=None, task_instance=self.task_instance_2, **on_task_failed_kwargs, session=None
            )

            # with selective-enable disabled both task_1 and task_2 should trigger metadata extraction
            if selective_enable == "False":
                expected_task_call_count *= 2

            assert expected_task_call_count == listener.extractor_manager.extract_metadata.call_count

    @pytest.mark.parametrize(
        ("selective_enable", "enable_task", "expected_call_count", "expected_task_call_count"),
        [
            ("True", True, 3, 3),
            ("False", True, 3, 3),
            ("True", False, 0, 0),
            ("False", False, 3, 3),
        ],
    )
    @mock.patch(
        "airflow.providers.openlineage.plugins.listener.OpenLineageListener._execute", new=regular_call
    )
    def test_listener_with_dag_disabled_task_enabled(
        self, selective_enable, enable_task, expected_call_count, expected_task_call_count
    ):
        """Tests listener's behaviour with selective-enable on task level with DAG disabled."""
        disable_lineage(self.dag)

        if enable_task:
            enable_lineage(self.task_1)

        on_task_failed_kwargs = {"error": ValueError("test")}

        with conf_vars({("openlineage", "selective_enable"): selective_enable}):
            listener = OpenLineageListener()
            listener._executor = mock.Mock()
            listener.extractor_manager = mock.Mock()
            listener.adapter = mock.Mock()

            # run all three DagRun-related hooks
            listener.on_dag_run_running(self.dagrun, msg="test running")
            listener.on_dag_run_failed(self.dagrun, msg="test failure")
            listener.on_dag_run_success(self.dagrun, msg="test success")

            # run TaskInstance-related hooks for lineage enabled task
            listener.on_task_instance_running(None, self.task_instance_1, session=None)
            listener.on_task_instance_success(None, self.task_instance_1, session=None)
            listener.on_task_instance_failed(
                previous_state=None, task_instance=self.task_instance_1, **on_task_failed_kwargs, session=None
            )

        assert expected_call_count == listener._executor.submit.call_count
        assert expected_task_call_count == listener.extractor_manager.extract_metadata.call_count
