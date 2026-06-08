"""Shared fixtures for dag_triage tests."""

from __future__ import annotations

import sys
import types
from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))


def _install_airflow_test_stubs() -> None:
    if "airflow" in sys.modules:
        return
    try:
        import airflow  # noqa: F401
    except ModuleNotFoundError:
        airflow_stub = types.ModuleType("airflow")
        listeners_stub = types.ModuleType("airflow.listeners")
        listeners_stub.hookimpl = lambda fn: fn
        configuration_stub = types.ModuleType("airflow.configuration")
        configuration_stub.conf = mock.MagicMock()
        plugins_manager_stub = types.ModuleType("airflow.plugins_manager")

        class AirflowPlugin:
            name = ""
            listeners: list = []

        plugins_manager_stub.AirflowPlugin = AirflowPlugin
        airflow_stub.listeners = listeners_stub
        airflow_stub.configuration = configuration_stub
        airflow_stub.plugins_manager = plugins_manager_stub
        sys.modules["airflow"] = airflow_stub
        sys.modules["airflow.listeners"] = listeners_stub
        sys.modules["airflow.configuration"] = configuration_stub
        sys.modules["airflow.plugins_manager"] = plugins_manager_stub


_install_airflow_test_stubs()


@pytest.fixture(autouse=True)
def default_log_tail_lines() -> Iterator[None]:
    with mock.patch("dag_triage.log_tail.get_log_tail_lines", return_value=200):
        yield


@pytest.fixture
def mock_ti() -> mock.MagicMock:
    ti = mock.MagicMock()
    ti.dag_id = "sample_dag"
    ti.task_id = "fail_task"
    ti.run_id = "manual__2024-01-15T00:00:00+00:00"
    ti.map_index = -1
    ti.try_number = 1
    ti.state = mock.Mock(value="failed")
    return ti


@pytest.fixture(autouse=True)
def reset_log_tail_store() -> Iterator[None]:
    import dag_triage.log_tail_store as store_module

    store_module._store = None
    yield
    store_module._store = None
