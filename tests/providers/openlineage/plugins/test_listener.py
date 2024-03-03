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

import datetime as dt
import uuid
from contextlib import suppress
from typing import Callable
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pytest

from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.plugins.listener import OpenLineageListener
from airflow.utils.state import State

pytestmark = pytest.mark.db_test


class TemplateOperator(BaseOperator):
    template_fields = ["df"]

    def __init__(self, df, *args, **kwargs):
        self.df = df
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return self.df


def render_df():
    return pd.DataFrame({"col": [1, 2]})


@patch("airflow.models.TaskInstance.xcom_push")
@patch("airflow.models.BaseOperator.render_template")
def test_listener_does_not_change_task_instance(render_mock, xcom_push_mock):
    render_mock.return_value = render_df()

    dag = DAG(
        "test",
        start_date=dt.datetime(2022, 1, 1),
        user_defined_macros={"render_df": render_df},
        params={"df": render_df()},
    )
    t = TemplateOperator(task_id="template_op", dag=dag, do_xcom_push=True, df=dag.param("df"))
    run_id = str(uuid.uuid1())
    dag.create_dagrun(state=State.NONE, run_id=run_id)
    ti = TaskInstance(t, run_id=run_id)
    ti.check_and_change_state_before_execution()  # make listener hook on running event
    ti._run_raw_task()

    # check if task returns the same DataFrame
    pd.testing.assert_frame_equal(xcom_push_mock.call_args.kwargs["value"], render_df())

    # check if render_template method always get the same unrendered field
    assert not isinstance(render_mock.call_args.args[0], pd.DataFrame)


def _setup_mock_listener(mock_listener: mock.Mock, captured_try_numbers: dict[str, int]) -> None:
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


def _create_test_dag_and_task(python_callable: Callable, scenario_name: str) -> TaskInstance:
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
    dag = DAG(
        f"test_{scenario_name}",
        start_date=dt.datetime(2022, 1, 1),
    )
    t = PythonOperator(task_id=f"test_task_{scenario_name}", dag=dag, python_callable=python_callable)
    run_id = str(uuid.uuid1())
    dag.create_dagrun(state=State.NONE, run_id=run_id)  # type: ignore
    task_instance = TaskInstance(t, run_id=run_id)
    return task_instance


def _create_listener_and_task_instance() -> tuple[OpenLineageListener, TaskInstance]:
    """Creates and configures an OpenLineageListener instance and a mock TaskInstance for testing.

    :return: A tuple containing the configured OpenLineageListener and TaskInstance.

    This function instantiates an OpenLineageListener, sets up its required properties with mock objects, and
    creates a mock TaskInstance with predefined attributes. This setup is commonly used for testing the
    interaction between an OpenLineageListener and a TaskInstance in Airflow.

    :Example:

        listener, task_instance = _create_listener_and_task_instance()
        # Now you can use listener and task_instance in your tests to simulate their interaction.
    """

    def mock_task_id(dag_id, task_id, execution_date, try_number):
        return f"{dag_id}.{task_id}.{execution_date}.{try_number}"

    listener = OpenLineageListener()
    listener.log = mock.Mock()
    listener.extractor_manager = mock.Mock()

    metadata = mock.Mock()
    metadata.run_facets = {"run_facet": 1}
    listener.extractor_manager.extract_metadata.return_value = metadata

    adapter = mock.Mock()
    adapter.build_dag_run_id.side_effect = lambda x, y: f"{x}.{y}"
    adapter.build_task_instance_run_id.side_effect = mock_task_id
    adapter.start_task = mock.Mock()
    adapter.fail_task = mock.Mock()
    adapter.complete_task = mock.Mock()
    listener.adapter = adapter

    task_instance = TaskInstance(task=mock.Mock())
    task_instance.dag_run = DagRun()
    task_instance.dag_run.run_id = "dag_run_run_id"
    task_instance.dag_run.data_interval_start = None
    task_instance.dag_run.data_interval_end = None
    task_instance.task = mock.Mock()
    task_instance.task.task_id = "task_id"
    task_instance.task.dag = mock.Mock()
    task_instance.task.dag.dag_id = "dag_id"
    task_instance.task.dag.description = "Test DAG Description"
    task_instance.task.dag.owner = "Test Owner"
    task_instance.dag_id = "dag_id"
    task_instance.run_id = "dag_run_run_id"
    task_instance.try_number = 1
    task_instance.state = State.RUNNING
    task_instance.start_date = dt.datetime(2023, 1, 1, 13, 1, 1)
    task_instance.end_date = dt.datetime(2023, 1, 3, 13, 1, 1)
    task_instance.execution_date = "execution_date"
    task_instance.next_method = None  # Ensure this is None to reach start_task

    return listener, task_instance


@mock.patch("airflow.providers.openlineage.plugins.listener.get_airflow_run_facet")
@mock.patch("airflow.providers.openlineage.plugins.listener.get_custom_facets")
@mock.patch("airflow.providers.openlineage.plugins.listener.get_job_name")
def test_adapter_start_task_is_called_with_proper_arguments(
    mock_get_job_name, mock_get_custom_facets, mock_get_airflow_run_facet
):
    """Tests that the 'start_task' method of the OpenLineageAdapter is invoked with the correct arguments.

    The test checks that the job name, job description, event time, and other related data are
    correctly passed to the adapter. It also verifies that custom facets and Airflow run facets are
    correctly retrieved and included in the call. This ensures that all relevant data, including custom
    and Airflow-specific metadata, is accurately conveyed to the adapter during the initialization of a task,
    reflecting the comprehensive tracking of task execution contexts.
    """
    listener, task_instance = _create_listener_and_task_instance()
    mock_get_job_name.return_value = "job_name"
    mock_get_custom_facets.return_value = {"custom_facet": 2}
    mock_get_airflow_run_facet.return_value = {"airflow_run_facet": 3}

    listener.on_task_instance_running(None, task_instance, None)
    listener.adapter.start_task.assert_called_once_with(
        run_id="dag_id.task_id.execution_date.1",
        job_name="job_name",
        job_description="Test DAG Description",
        event_time="2023-01-01T13:01:01",
        parent_job_name="dag_id",
        parent_run_id="dag_id.dag_run_run_id",
        code_location=None,
        nominal_start_time=None,
        nominal_end_time=None,
        owners=["Test Owner"],
        task=listener.extractor_manager.extract_metadata(),
        run_facets={
            "custom_facet": 2,
            "airflow_run_facet": 3,
        },
    )


@mock.patch("airflow.providers.openlineage.plugins.listener.OpenLineageAdapter")
@mock.patch("airflow.providers.openlineage.plugins.listener.get_job_name")
def test_adapter_fail_task_is_called_with_proper_arguments(mock_get_job_name, mocked_adapter):
    """Tests that the 'fail_task' method of the OpenLineageAdapter is invoked with the correct arguments.

    This test ensures that the job name is accurately retrieved and included, along with the generated
    run_id and task metadata. By mocking the job name retrieval and the run_id generation,
    the test verifies the integrity and consistency of the data passed to the adapter during task
    failure events, thus confirming that the adapter's failure handling is functioning as expected.
    """

    def mock_task_id(dag_id, task_id, execution_date, try_number):
        return f"{dag_id}.{task_id}.{execution_date}.{try_number}"

    listener, task_instance = _create_listener_and_task_instance()
    mock_get_job_name.return_value = "job_name"
    mocked_adapter.build_task_instance_run_id.side_effect = mock_task_id
    mocked_adapter.build_dag_run_id.side_effect = lambda x, y: f"{x}.{y}"

    listener.on_task_instance_failed(None, task_instance, None)
    listener.adapter.fail_task.assert_called_once_with(
        end_time="2023-01-03T13:01:01",
        job_name="job_name",
        parent_job_name="dag_id",
        parent_run_id="dag_id.dag_run_run_id",
        run_id="dag_id.task_id.execution_date.1",
        task=listener.extractor_manager.extract_metadata(),
    )


@mock.patch("airflow.providers.openlineage.plugins.listener.OpenLineageAdapter")
@mock.patch("airflow.providers.openlineage.plugins.listener.get_job_name")
def test_adapter_complete_task_is_called_with_proper_arguments(mock_get_job_name, mocked_adapter):
    """Tests that the 'complete_task' method of the OpenLineageAdapter is called with the correct arguments.

    It checks that the job name is correctly retrieved and passed,
    along with the run_id and task metadata. The test also simulates changes in the try_number
    attribute of the task instance, as it would occur in Airflow, to ensure that the run_id is updated
    accordingly. This helps confirm the consistency and correctness of the data passed to the adapter
    during the task's lifecycle events.
    """

    def mock_task_id(dag_id, task_id, execution_date, try_number):
        return f"{dag_id}.{task_id}.{execution_date}.{try_number}"

    listener, task_instance = _create_listener_and_task_instance()
    mock_get_job_name.return_value = "job_name"
    mocked_adapter.build_task_instance_run_id.side_effect = mock_task_id
    mocked_adapter.build_dag_run_id.side_effect = lambda x, y: f"{x}.{y}"

    listener.on_task_instance_success(None, task_instance, None)
    # This run_id will be different as we did NOT simulate increase of the try_number attribute,
    # which happens in Airflow.
    listener.adapter.complete_task.assert_called_once_with(
        end_time="2023-01-03T13:01:01",
        job_name="job_name",
        parent_job_name="dag_id",
        parent_run_id="dag_id.dag_run_run_id",
        run_id="dag_id.task_id.execution_date.0",
        task=listener.extractor_manager.extract_metadata(),
    )

    # Now we simulate the increase of try_number, and the run_id should reflect that change.
    listener.adapter.complete_task.reset_mock()
    task_instance.try_number += 1
    listener.on_task_instance_success(None, task_instance, None)
    listener.adapter.complete_task.assert_called_once_with(
        end_time="2023-01-03T13:01:01",
        job_name="job_name",
        parent_job_name="dag_id",
        parent_run_id="dag_id.dag_run_run_id",
        run_id="dag_id.task_id.execution_date.1",
        task=listener.extractor_manager.extract_metadata(),
    )


@mock.patch("airflow.providers.openlineage.plugins.listener.OpenLineageAdapter")
def test_run_id_is_constant_across_all_methods(mocked_adapter):
    """Tests that the run_id remains constant across different methods of the listener.

    It ensures that the run_id generated for starting, failing, and completing a task is consistent,
    reflecting the task's identity and execution context. The test also simulates the change in the
    try_number attribute, as it would occur in Airflow, to verify that the run_id updates accordingly.
    """

    def mock_task_id(dag_id, task_id, execution_date, try_number):
        return f"{dag_id}.{task_id}.{execution_date}.{try_number}"

    listener, task_instance = _create_listener_and_task_instance()
    mocked_adapter.build_task_instance_run_id.side_effect = mock_task_id

    listener.on_task_instance_running(None, task_instance, None)
    expected_run_id = listener.adapter.start_task.call_args.kwargs["run_id"]
    assert expected_run_id == "dag_id.task_id.execution_date.1"

    listener.on_task_instance_failed(None, task_instance, None)
    assert listener.adapter.fail_task.call_args.kwargs["run_id"] == expected_run_id

    # This run_id will be different as we did NOT simulate increase of the try_number attribute,
    # which happens in Airflow.
    listener.on_task_instance_success(None, task_instance, None)
    assert listener.adapter.complete_task.call_args.kwargs["run_id"] == "dag_id.task_id.execution_date.0"

    # Now we simulate the increase of try_number, and the run_id should reflect that change.
    # This is how airflow works, and that's why we expect the run_id to remain constant across all methods.
    task_instance.try_number += 1
    listener.on_task_instance_success(None, task_instance, None)
    assert listener.adapter.complete_task.call_args.kwargs["run_id"] == expected_run_id


def test_running_task_correctly_calls_openlineage_adapter_run_id_method():
    """Tests the OpenLineageListener's response when a task instance is in the running state.

    This test ensures that when an Airflow task instance transitions to the running state,
    the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
    parameters derived from the task instance.
    """
    listener, task_instance = _create_listener_and_task_instance()
    listener.on_task_instance_running(None, task_instance, None)
    listener.adapter.build_task_instance_run_id.assert_called_once_with(
        dag_id="dag_id",
        task_id="task_id",
        execution_date="execution_date",
        try_number=1,
    )


@mock.patch("airflow.providers.openlineage.plugins.listener.OpenLineageAdapter")
def test_failed_task_correctly_calls_openlineage_adapter_run_id_method(mock_adapter):
    """Tests the OpenLineageListener's response when a task instance is in the failed state.

    This test ensures that when an Airflow task instance transitions to the failed state,
    the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
    parameters derived from the task instance.
    """
    listener, task_instance = _create_listener_and_task_instance()
    listener.on_task_instance_failed(None, task_instance, None)
    mock_adapter.build_task_instance_run_id.assert_called_once_with(
        dag_id="dag_id",
        task_id="task_id",
        execution_date="execution_date",
        try_number=1,
    )


@mock.patch("airflow.providers.openlineage.plugins.listener.OpenLineageAdapter")
def test_successful_task_correctly_calls_openlineage_adapter_run_id_method(mock_adapter):
    """Tests the OpenLineageListener's response when a task instance is in the success state.

    This test ensures that when an Airflow task instance transitions to the success state,
    the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
    parameters derived from the task instance.
    """
    listener, task_instance = _create_listener_and_task_instance()
    listener.on_task_instance_success(None, task_instance, None)
    mock_adapter.build_task_instance_run_id.assert_called_once_with(
        dag_id="dag_id",
        task_id="task_id",
        execution_date="execution_date",
        try_number=0,
    )


@mock.patch("airflow.models.taskinstance.get_listener_manager")
def test_listener_on_task_instance_failed_is_called_before_try_number_increment(mock_listener):
    """Validates the listener's on-failure method is called before try_number increment happens.

    This test ensures that when a task instance fails, Airflow's listener method for
    task failure (`on_task_instance_failed`) is invoked before the increment of the
    `try_number` attribute happens. A custom exception simulates task failure, and the test
    captures the `try_number` at the moment of this method call.
    """
    captured_try_numbers = {}
    _setup_mock_listener(mock_listener, captured_try_numbers)

    # Just to make sure no error interferes with the test, and we do not suppress it by accident
    class CustomError(Exception):
        pass

    def fail_callable(**kwargs):
        raise CustomError("Simulated task failure")

    task_instance = _create_test_dag_and_task(fail_callable, "failure")
    # try_number before execution
    assert task_instance.try_number == 1
    with suppress(CustomError):
        task_instance.run()

    # try_number at the moment of function being called
    assert captured_try_numbers["running"] == 1
    assert captured_try_numbers["failed"] == 1

    # try_number after task has been executed
    assert task_instance.try_number == 2


@mock.patch("airflow.models.taskinstance.get_listener_manager")
def test_listener_on_task_instance_success_is_called_after_try_number_increment(mock_listener):
    """Validates the listener's on-success method is called before try_number increment happens.

    This test ensures that when a task instance successfully completes, the
    `on_task_instance_success` method of Airflow's listener is called with an
    incremented `try_number` compared to the `try_number` before execution.
    The test simulates a successful task execution and captures the `try_number` at the method call.
    """
    captured_try_numbers = {}
    _setup_mock_listener(mock_listener, captured_try_numbers)

    def success_callable(**kwargs):
        return None

    task_instance = _create_test_dag_and_task(success_callable, "success")
    # try_number before execution
    assert task_instance.try_number == 1
    task_instance.run()

    # try_number at the moment of function being called
    assert captured_try_numbers["running"] == 1
    assert captured_try_numbers["success"] == 2

    # try_number after task has been executed
    assert task_instance.try_number == 2
