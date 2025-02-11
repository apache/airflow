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

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, Protocol
from unittest import mock

import pytest

pytest_plugins = "tests_common.pytest_plugin"

# Task SDK does not need access to the Airflow database
os.environ["_AIRFLOW_SKIP_DB_TESTS"] = "true"

if TYPE_CHECKING:
    from datetime import datetime

    from structlog.typing import EventDict, WrappedLogger

    from airflow.sdk.api.datamodels._generated import TIRunContext
    from airflow.sdk.definitions.baseoperator import BaseOperator
    from airflow.sdk.execution_time.comms import StartupDetails
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


@pytest.hookimpl()
def pytest_addhooks(pluginmanager: pytest.PytestPluginManager):
    # Python 3.12 starts warning about mixing os.fork + Threads, and the pytest-rerunfailures plugin uses
    # threads internally. Since this is new code, and it should be flake free, we disable the re-run failures
    # plugin early (so that it doesn't run it's pytest_configure which is where the thread starts up if xdist
    # is discovered).
    pluginmanager.set_blocked("rerunfailures")


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    config.inicfg["airflow_deprecations_ignore"] = []

    # Always skip looking for tests in these folders!
    config.addinivalue_line("norecursedirs", "tests/test_dags")


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    if next(item.iter_markers(name="db_test"), None):
        pytest.fail("Task SDK tests must not use database")


class LogCapture:
    # Like structlog.typing.LogCapture, but that doesn't add log_level in to the event dict
    entries: list[EventDict | bytes]

    def __init__(self) -> None:
        self.entries = []

    def __call__(self, _: WrappedLogger, method_name: str, event: EventDict | bytes) -> NoReturn:
        from structlog.exceptions import DropEvent

        if isinstance(event, dict):
            if "level" not in event:
                event["_log_level"] = method_name

        self.entries.append(event)

        raise DropEvent


@pytest.fixture
def test_dags_dir():
    return Path(__file__).parent.joinpath("dags")


@pytest.fixture
def captured_logs(request):
    import structlog

    from airflow.sdk.log import configure_logging, reset_logging

    # Use our real log config
    reset_logging()
    configure_logging(enable_pretty_log=False)

    # Get log level from test parameter, which can either be a single log level or a
    # tuple of log level and desired output type, defaulting to INFO if not provided
    log_level = logging.INFO
    output = "dict"
    param = getattr(request, "param", logging.INFO)
    if isinstance(param, int):
        log_level = param
    elif isinstance(param, tuple):
        log_level = param[0]
        output = param[1]

    # We want to capture all logs, but we don't want to see them in the test output
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(log_level))

    cur_processors = structlog.get_config()["processors"]
    processors = cur_processors.copy()
    if output == "dict":
        # We need to replace remove the last processor (the one that turns JSON into text, as we want the
        # event dict for tests)
        proc = processors.pop()
        assert isinstance(
            proc, (structlog.dev.ConsoleRenderer, structlog.processors.JSONRenderer)
        ), "Pre-condition"
    try:
        cap = LogCapture()
        processors.append(cap)
        structlog.configure(processors=processors)
        yield cap.entries
    finally:
        structlog.configure(processors=cur_processors)


@pytest.fixture(autouse=True, scope="session")
def _disable_ol_plugin():
    # The OpenLineage plugin imports setproctitle, and that now causes (C) level thread calls, which on Py
    # 3.12+ issues a warning when os.fork happens. So for this plugin we disable it

    # And we load plugins when setting the priority_weight field
    import airflow.plugins_manager

    old = airflow.plugins_manager.plugins

    assert old is None, "Plugins already loaded, too late to stop them being loaded!"

    airflow.plugins_manager.plugins = []

    yield

    airflow.plugins_manager.plugins = None


class MakeTIContextCallable(Protocol):
    def __call__(
        self,
        dag_id: str = ...,
        run_id: str = ...,
        logical_date: str | datetime = ...,
        data_interval_start: str | datetime = ...,
        data_interval_end: str | datetime = ...,
        clear_number: int = ...,
        start_date: str | datetime = ...,
        run_after: str | datetime = ...,
        run_type: str = ...,
        task_reschedule_count: int = ...,
        conf: dict[str, Any] | None = ...,
    ) -> TIRunContext: ...


class MakeTIContextDictCallable(Protocol):
    def __call__(
        self,
        dag_id: str = ...,
        run_id: str = ...,
        logical_date: str = ...,
        data_interval_start: str | datetime = ...,
        data_interval_end: str | datetime = ...,
        clear_number: int = ...,
        start_date: str | datetime = ...,
        run_after: str | datetime = ...,
        run_type: str = ...,
        task_reschedule_count: int = ...,
        conf=None,
    ) -> dict[str, Any]: ...


@pytest.fixture
def make_ti_context() -> MakeTIContextCallable:
    """Factory for creating TIRunContext objects."""
    from airflow.sdk.api.datamodels._generated import DagRun, TIRunContext

    def _make_context(
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        logical_date: str | datetime = "2024-12-01T01:00:00Z",
        data_interval_start: str | datetime = "2024-12-01T00:00:00Z",
        data_interval_end: str | datetime = "2024-12-01T01:00:00Z",
        clear_number: int = 0,
        start_date: str | datetime = "2024-12-01T01:00:00Z",
        run_after: str | datetime = "2024-12-01T01:00:00Z",
        run_type: str = "manual",
        task_reschedule_count: int = 0,
        conf=None,
    ) -> TIRunContext:
        return TIRunContext(
            dag_run=DagRun(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,  # type: ignore
                data_interval_start=data_interval_start,  # type: ignore
                data_interval_end=data_interval_end,  # type: ignore
                clear_number=clear_number,  # type: ignore
                start_date=start_date,  # type: ignore
                run_type=run_type,  # type: ignore
                run_after=run_after,  # type: ignore
                conf=conf,  # type: ignore
            ),
            task_reschedule_count=task_reschedule_count,
            max_tries=0,
        )

    return _make_context


@pytest.fixture
def make_ti_context_dict(make_ti_context: MakeTIContextCallable) -> MakeTIContextDictCallable:
    """Factory for creating context dictionaries suited for API Server response."""

    def _make_context_dict(
        dag_id: str = "test_dag",
        run_id: str = "test_run",
        logical_date: str | datetime = "2024-12-01T00:00:00Z",
        data_interval_start: str | datetime = "2024-12-01T00:00:00Z",
        data_interval_end: str | datetime = "2024-12-01T01:00:00Z",
        clear_number: int = 0,
        start_date: str | datetime = "2024-12-01T00:00:00Z",
        run_after: str | datetime = "2024-12-01T00:00:00Z",
        run_type: str = "manual",
        task_reschedule_count: int = 0,
        conf=None,
    ) -> dict[str, Any]:
        context = make_ti_context(
            dag_id=dag_id,
            run_id=run_id,
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            clear_number=clear_number,
            start_date=start_date,
            run_after=run_after,
            run_type=run_type,
            conf=conf,
            task_reschedule_count=task_reschedule_count,
        )
        return context.model_dump(exclude_unset=True, mode="json")

    return _make_context_dict


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

        if not task.has_dag():
            dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
            task.dag = dag
            task = dag.task_dict[task.task_id]
        else:
            dag = task.dag
        if what.ti_context.dag_run.conf:
            dag.params = what.ti_context.dag_run.conf  # type: ignore[assignment]
        ti = RuntimeTaskInstance.model_construct(
            **what.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=what.ti_context,
            max_tries=what.ti_context.max_tries,
            start_date=what.start_date,
        )
        if hasattr(parse, "spy"):
            spy_agency.unspy(parse)
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
        map_index: int | None = -1,
        upstream_map_indexes: dict[str, int] | None = None,
        ti_id=None,
        conf=None,
    ) -> RuntimeTaskInstance:
        if not ti_id:
            ti_id = uuid7()

        if task.has_dag():
            dag_id = task.dag.dag_id

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

        if upstream_map_indexes is not None:
            ti_context.upstream_map_indexes = upstream_map_indexes

        startup_details = StartupDetails(
            ti=TaskInstance(
                id=ti_id,
                task_id=task.task_id,
                dag_id=dag_id,
                run_id=run_id,
                try_number=try_number,
                map_index=map_index,
            ),
            dag_rel_path="",
            bundle_info=BundleInfo(name="anything", version="any"),
            requests_fd=0,
            ti_context=ti_context,
            start_date=start_date,  # type: ignore
        )

        ti = mocked_parse(startup_details, dag_id, task)
        return ti

    return _create_task_instance
