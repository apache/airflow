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

if TYPE_CHECKING:
    from datetime import datetime

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
        if what.ti_context.dag_run.conf:
            dag.params = what.ti_context.dag_run.conf  # type: ignore[assignment]
        task.dag = dag
        t = dag.task_dict[task.task_id]
        ti = RuntimeTaskInstance.model_construct(
            **what.ti.model_dump(exclude_unset=True),
            task=t,
            _ti_context_from_server=what.ti_context,
            max_tries=what.ti_context.max_tries,
        )
        spy_agency.spy_on(parse, call_fake=lambda _: ti)
        return ti

    return set_dag


@pytest.fixture
def create_runtime_ti(mocked_parse, make_ti_context):
    """
    Fixture to create a Runtime TaskInstance for testing purposes without defining a dag file.

    It mimics the behavior of the `parse` function by creating a `RuntimeTaskInstance` based on the provided
    `StartupDetails` (formed from arguments) and task. This allows you to test the logic of a task without
    having to define a DAG file, parse it, get context from the server, etc.

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
    from airflow.sdk.execution_time.comms import BundleInfo, StartupDetails

    def _create_task_instance(
        task: BaseOperator,
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        logical_date: str | datetime = "2024-12-01T01:00:00Z",
        data_interval_start: str | datetime = "2024-12-01T00:00:00Z",
        data_interval_end: str | datetime = "2024-12-01T01:00:00Z",
        start_date: str | datetime = "2024-12-01T01:00:00Z",
        run_type: str = "manual",
        try_number: int = 1,
        conf=None,
        ti_id=None,
    ) -> RuntimeTaskInstance:
        if not ti_id:
            ti_id = uuid7()

        ti_context = make_ti_context(
            dag_id=dag_id,
            run_id=run_id,
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            start_date=start_date,
            run_type=run_type,
            conf=conf,
        )

        startup_details = StartupDetails(
            ti=TaskInstance(
                id=ti_id, task_id=task.task_id, dag_id=dag_id, run_id=run_id, try_number=try_number
            ),
            dag_rel_path="",
            bundle_info=BundleInfo(name="anything", version="any"),
            requests_fd=0,
            ti_context=ti_context,
        )

        ti = mocked_parse(startup_details, dag_id, task)
        return ti

    return _create_task_instance
