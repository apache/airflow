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

import sys
from typing import TYPE_CHECKING
from unittest import mock

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.sdk.api.datamodels._generated import TIRunContext
    from airflow.sdk.definitions.baseoperator import BaseOperator
    from airflow.sdk.execution_time.comms import StartupDetails
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

import pytest


@pytest.fixture
def disable_capturing():
    old_in, old_out, old_err = sys.stdin, sys.stdout, sys.stderr

    sys.stdin = sys.__stdin__
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    yield
    sys.stdin, sys.stdout, sys.stderr = old_in, old_out, old_err


@pytest.fixture
def mock_supervisor_comms():
    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as supervisor_comms:
        yield supervisor_comms


@pytest.fixture
def mocked_parse(spy_agency):
    """
    Fixture to set up an inline DAG and use it in a stubbed `parse` function. Use this fixture if you
    want to isolate and test `parse` or `run` logic without having to define a DAG file.

    This fixture returns a helper function `set_dag` that:
    1. Creates an in line DAG with the given `dag_id` and `task` (limited to one task)
    2. Constructs a `RuntimeTaskInstance` based on the provided `StartupDetails` and task.
    3. Stubs the `parse` function using `spy_agency`, to return the mocked `RuntimeTaskInstance`.

    After adding the fixture in your test function signature, you can use it like this ::

            mocked_parse(
                StartupDetails(
                    ti=TaskInstance(id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1),
                    file="",
                    requests_fd=0,
                ),
                "example_dag_id",
                CustomOperator(task_id="hello"),
            )
    """

    def set_dag(what: StartupDetails, dag_id: str, task: BaseOperator) -> RuntimeTaskInstance:
        from airflow.sdk.definitions.dag import DAG
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, parse
        from airflow.utils import timezone

        dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
        task.dag = dag
        t = dag.task_dict[task.task_id]
        ti = RuntimeTaskInstance.model_construct(**what.ti.model_dump(exclude_unset=True), task=t)
        spy_agency.spy_on(parse, call_fake=lambda _: ti)
        return ti

    return set_dag


@pytest.fixture
def create_runtime_ti(
    mocked_parse: Callable[[StartupDetails, str, BaseOperator], RuntimeTaskInstance],
    make_ti_context: Callable[..., TIRunContext],
) -> Callable[[BaseOperator, TIRunContext | None, StartupDetails | None], RuntimeTaskInstance]:
    """
    Fixture to create a Runtime TaskInstance for testing purposes without defining a dag file.

    This fixture sets up a `RuntimeTaskInstance` with default or custom `TIRunContext` and `StartupDetails`,
    making it easy to simulate task execution scenarios in tests.

    Example usage: ::

        def test_custom_task_instance(create_runtime_ti):
            class MyTaskOperator(BaseOperator):
                def execute(self, context):
                    assert context["dag_run"].run_id == "test_run"

            task = MyTaskOperator(task_id="test_task")
            ti = create_runtime_ti(task, context_from_server=make_ti_context(run_id="test_run"))
            # Further test logic...
    """
    from uuid6 import uuid7

    from airflow.sdk.api.datamodels._generated import TaskInstance
    from airflow.sdk.execution_time.comms import StartupDetails

    def _create_task_instance(
        task, context_from_server: TIRunContext | None = None, startup_details: StartupDetails | None = None
    ) -> RuntimeTaskInstance:
        if context_from_server is None:
            context_from_server = make_ti_context()

        if not startup_details:
            startup_details = StartupDetails(
                ti=TaskInstance(
                    id=uuid7(),
                    task_id=task.task_id,
                    dag_id=context_from_server.dag_run.dag_id,
                    run_id=context_from_server.dag_run.run_id,
                    try_number=1,
                ),
                file="",
                requests_fd=0,
                ti_context=context_from_server,
            )

        ti = mocked_parse(startup_details, context_from_server.dag_run.dag_id, task)
        return ti

    return _create_task_instance
