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

import json

import pytest

from airflow.sdk.configuration import conf
from airflow.sdk.execution_time.coordinator import (
    BaseCoordinator,
    CoordinatorManager,
    _PythonCoordinator,
    get_coordinator_manager,
    reset_coordinator_manager,
)


class _CoordinatorA(BaseCoordinator):
    def __init__(self, *, label: str = "a"):
        self.label = label


class _CoordinatorB(BaseCoordinator):
    pass


@pytest.fixture
def sdk_config(monkeypatch):
    """Set the ``[sdk]`` env vars consumed by :meth:`CoordinatorManager.from_config`.

    :return: Callable ``apply(*, coordinators=None, queue_to_coordinator=None)`` --
        each argument is the raw JSON string for the matching env var, or ``None``
        to unset it. The conf cache is invalidated after each call (and again on
        teardown) so ``from_config()`` re-reads the values just set.
    """
    from airflow.sdk.configuration import conf

    def _apply(*, coordinators: str | None = None, queue_to_coordinator: str | None = None) -> None:
        if coordinators is None:
            monkeypatch.delenv("AIRFLOW__SDK__COORDINATORS", raising=False)
        else:
            monkeypatch.setenv("AIRFLOW__SDK__COORDINATORS", coordinators)
        if queue_to_coordinator is None:
            monkeypatch.delenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", raising=False)
        else:
            monkeypatch.setenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", queue_to_coordinator)
        conf.invalidate_cache()

    yield _apply
    conf.invalidate_cache()


class TestCoordinatorManager:
    @pytest.fixture(autouse=True)
    def _reset_cache(self):
        reset_coordinator_manager()
        yield
        reset_coordinator_manager()

    def test_from_config_loads_specs_and_resolves_instances(self, sdk_config):
        sdk_config(
            coordinators=json.dumps(
                {
                    "alpha": {
                        "classpath": f"{_CoordinatorA.__module__}._CoordinatorA",
                        "kwargs": {"label": "alpha-label"},
                    },
                    "beta": {"classpath": f"{_CoordinatorB.__module__}._CoordinatorB", "kwargs": {}},
                }
            ),
            queue_to_coordinator=json.dumps({"queue-a": "alpha"}),
        )
        manager = CoordinatorManager.from_config()
        assert manager._queue_to_coordinator == {"queue-a": "alpha"}
        assert manager._created_coordinators == {}

        coordinator_for_queue_a = manager.for_queue("queue-a")
        assert isinstance(coordinator_for_queue_a, _CoordinatorA)
        assert manager.for_queue("queue-a") is coordinator_for_queue_a, "instance should be cached"
        assert manager._created_coordinators == {"alpha": coordinator_for_queue_a}

        coordinator_for_queue_missing = manager.for_queue("queue-1")
        assert isinstance(coordinator_for_queue_missing, _PythonCoordinator)
        assert manager.for_queue("queue-1") is coordinator_for_queue_missing
        assert manager._created_coordinators == {"alpha": coordinator_for_queue_a}

    def test_from_config_empty(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__SDK__COORDINATORS", raising=False)
        monkeypatch.delenv("AIRFLOW__SDK__QUEUE_TO_COORDINATOR", raising=False)
        conf.invalidate_cache()

        manager = CoordinatorManager.from_config()
        assert manager._coordinator_specs == {}
        assert manager._queue_to_coordinator == {}

    def test_get_coordinator_manager_is_cached(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__SDK__COORDINATORS", raising=False)

        from airflow.sdk.configuration import conf

        conf.invalidate_cache()

        m1 = get_coordinator_manager()
        m2 = get_coordinator_manager()
        assert m1 is m2
