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

import json
import re
import sys
import textwrap
from types import SimpleNamespace

import pytest

from airflow.sdk._shared.module_loading.dag_file import UNUSUAL_MODULE_PREFIX
from airflow.sdk.coordinators.python import PythonCoordinator
from airflow.sdk.execution_time.coordinator import BaseCoordinator, CoordinatorManager
from airflow.sdk.task_handlers import loader as loader_mod
from airflow.sdk.task_handlers.registry import _REGISTRY


def _purge_handler_modules():
    for name in [n for n in sys.modules if n.startswith(UNUSUAL_MODULE_PREFIX)]:
        del sys.modules[name]


@pytest.fixture(autouse=True)
def _clean_registry():
    _REGISTRY.clear()
    loader_mod._imported_bundles.clear()
    loader_mod._resolved_bundles.clear()
    _purge_handler_modules()
    yield
    _REGISTRY.clear()
    loader_mod._imported_bundles.clear()
    loader_mod._resolved_bundles.clear()
    _purge_handler_modules()


def _bundle(path):
    """A stand-in for the stub's own, already-materialized Dag bundle."""
    return SimpleNamespace(name="own", version=None, path=path)


def _write_handler(path, body):
    (path / "handlers.py").write_text(textwrap.dedent(body))


class TestParseTaskHandler:
    def test_parses_handler_from_the_tasks_own_bundle(self, tmp_path):
        _write_handler(
            tmp_path,
            """
            from airflow.sdk import task_handler

            @task_handler("d", "t")
            def t(region, events):
                return len(events)
            """,
        )

        coordinator = PythonCoordinator(task_handler_bundle_name=None)
        decl = coordinator.parse_task_handler(dag_id="d", task_id="t", bundle=_bundle(tmp_path))

        assert decl is not None
        assert [p.name for p in decl.params] == ["region", "events"]

    def test_returns_none_when_no_handler_registered(self, tmp_path):
        coordinator = PythonCoordinator(task_handler_bundle_name=None)
        assert coordinator.parse_task_handler(dag_id="d", task_id="x", bundle=_bundle(tmp_path)) is None

    def test_base_coordinator_default_returns_none(self, tmp_path):
        # The default (foreign / not-adopted) coordinator declares nothing; validation defers.
        base = BaseCoordinator()
        assert base.parse_task_handler(dag_id="d", task_id="t", bundle=_bundle(tmp_path)) is None

    def test_invalid_signature_is_reported_against_the_task(self, tmp_path):
        _write_handler(
            tmp_path,
            """
            from airflow.sdk import task_handler

            @task_handler("d", "t")
            def t(*args): ...
            """,
        )

        # from_handler raises a bare signature error; parse_task_handler adds the task identity.
        coordinator = PythonCoordinator(task_handler_bundle_name=None)
        with pytest.raises(
            ValueError, match=re.escape("task handler 't' may not use variadic *args or **kwargs")
        ):
            coordinator.parse_task_handler(dag_id="d", task_id="t", bundle=_bundle(tmp_path))


class TestResolveHandler:
    def test_resolves_the_handler_from_the_tasks_own_bundle(self, tmp_path):
        _write_handler(
            tmp_path,
            """
            from airflow.sdk import task_handler

            @task_handler("d", "t")
            def t(region):
                return region
            """,
        )
        coordinator = PythonCoordinator(task_handler_bundle_name=None)
        handler = coordinator.resolve_handler(dag_id="d", task_id="t", bundle=_bundle(tmp_path))

        assert handler is not None
        assert handler.__name__ == "t"

    def test_returns_none_when_no_handler_registered(self, tmp_path):
        coordinator = PythonCoordinator(task_handler_bundle_name=None)
        assert coordinator.resolve_handler(dag_id="d", task_id="x", bundle=_bundle(tmp_path)) is None

    def test_is_not_a_base_coordinator_hook(self):
        # Execution resolution is Python-specific; it is deliberately not on BaseCoordinator.
        assert not hasattr(BaseCoordinator(), "resolve_handler")


class TestConfigurable:
    def test_configured_in_sdk_coordinators_carries_bundle_name(self, monkeypatch):
        from airflow.dag_processing.bundles import manager as mgr

        monkeypatch.setattr(mgr.DagBundlesManager, "is_bundle_configured", lambda name: True)
        monkeypatch.setenv(
            "AIRFLOW__SDK__COORDINATORS",
            json.dumps(
                {
                    "py": {
                        "classpath": "airflow.sdk.coordinators.python.PythonCoordinator",
                        "kwargs": {"task_handler_bundle_name": "handlers"},
                    }
                }
            ),
        )
        monkeypatch.setenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", json.dumps({"myqueue": "py"}))

        coordinator = CoordinatorManager.from_config().for_queue("myqueue")

        assert isinstance(coordinator, PythonCoordinator)
        assert coordinator.task_handler_bundle_name == "handlers"

    def test_unmapped_queue_falls_back_to_bundleless_python(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW__SDK__COORDINATORS", "{}")
        monkeypatch.setenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", "{}")

        coordinator = CoordinatorManager.from_config().for_queue("whatever")

        assert isinstance(coordinator, PythonCoordinator)
        assert coordinator.task_handler_bundle_name is None
