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

import pytest

from airflow.sdk import task_handler
from airflow.sdk.coordinators.python import PythonCoordinator
from airflow.sdk.execution_time.coordinator import BaseCoordinator, CoordinatorManager
from airflow.sdk.task_handlers.registry import _REGISTRY


@pytest.fixture(autouse=True)
def _clean_registry():
    _REGISTRY.clear()
    yield
    _REGISTRY.clear()


class TestDescribeTaskHandler:
    def test_describes_registered_handler_with_callable_and_params(self):
        @task_handler("d", "t")
        def t(region, events):
            return len(events)

        decl = PythonCoordinator(task_handler_bundle_name=None).describe_task_handler("d", "t")

        assert decl is not None
        assert decl.handler is t
        assert [p.name for p in decl.params] == ["region", "events"]

    def test_returns_none_when_no_handler_registered(self):
        assert PythonCoordinator(task_handler_bundle_name=None).describe_task_handler("d", "x") is None

    def test_base_coordinator_default_returns_none(self):
        # The default (foreign / not-adopted) coordinator declares nothing; validation defers.
        assert BaseCoordinator().describe_task_handler("d", "t") is None


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
