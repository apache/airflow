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
    entries: list[EventDict]

    def __init__(self) -> None:
        self.entries = []

    def __call__(self, _: WrappedLogger, method_name: str, event_dict: EventDict) -> NoReturn:
        from structlog.exceptions import DropEvent

        if "level" not in event_dict:
            event_dict["_log_level"] = method_name

        self.entries.append(event_dict)

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

    # Get log level from test parameter, defaulting to INFO if not provided
    log_level = getattr(request, "param", logging.INFO)

    # We want to capture all logs, but we don't want to see them in the test output
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(log_level))

    # But we need to replace remove the last processor (the one that turns JSON into text, as we want the
    # event dict for tests)
    cur_processors = structlog.get_config()["processors"]
    processors = cur_processors.copy()
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
        start_date: str | datetime = ...,
        run_type: str = ...,
    ) -> TIRunContext: ...


class MakeTIContextDictCallable(Protocol):
    def __call__(
        self,
        dag_id: str = ...,
        run_id: str = ...,
        logical_date: str = ...,
        data_interval_start: str | datetime = ...,
        data_interval_end: str | datetime = ...,
        start_date: str | datetime = ...,
        run_type: str = ...,
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
        start_date: str | datetime = "2024-12-01T01:00:00Z",
        run_type: str = "manual",
    ) -> TIRunContext:
        return TIRunContext(
            dag_run=DagRun(
                dag_id=dag_id,
                run_id=run_id,
                logical_date=logical_date,  # type: ignore
                data_interval_start=data_interval_start,  # type: ignore
                data_interval_end=data_interval_end,  # type: ignore
                start_date=start_date,  # type: ignore
                run_type=run_type,  # type: ignore
            ),
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
        start_date: str | datetime = "2024-12-01T00:00:00Z",
        run_type: str = "manual",
    ) -> dict[str, Any]:
        context = make_ti_context(
            dag_id=dag_id,
            run_id=run_id,
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            start_date=start_date,
            run_type=run_type,
        )
        return context.model_dump(exclude_unset=True, mode="json")

    return _make_context_dict


@pytest.fixture
def mock_supervisor_comms():
    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as supervisor_comms:
        yield supervisor_comms
