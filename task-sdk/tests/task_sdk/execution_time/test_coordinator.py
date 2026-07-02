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
import os
import signal
from unittest import mock

import pytest

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.configuration import conf, retrieve_configuration_description
from airflow.sdk.execution_time.coordinator import (
    BaseCoordinator,
    CoordinatorManager,
    InvalidCoordinatorError,
    _PythonCoordinator,
    _warm_shutdown_signals,
    get_coordinator_manager,
    reset_coordinator_manager,
)


class _CoordinatorA(BaseCoordinator):
    def __init__(self, *, label: str = "a"):
        self.label = label


class _CoordinatorB(BaseCoordinator):
    pass


class _ExplodingCoordinator(BaseCoordinator):
    def __init__(self):
        raise RuntimeError("This coordinator must not be instantiated")


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

    def test_from_config_rejects_invalid_queue_mapping(self, sdk_config):
        sdk_config(
            coordinators=json.dumps(
                {"alpha": {"classpath": f"{_CoordinatorA.__module__}._CoordinatorA", "kwargs": {}}}
            ),
            queue_to_coordinator=json.dumps({"queue-x": "nonexistent"}),
        )
        with pytest.raises(
            ValueError,
            match=r"queue_to_coordinator references invalid coordinator key: 'nonexistent'",
        ):
            CoordinatorManager.from_config()

    @pytest.mark.parametrize(
        ("coordinator_spec", "expected_match"),
        [
            pytest.param(
                {"classpath": "nonexistent.module.MissingCoordinator", "kwargs": {}},
                r"Cannot import coordinator 'bad-coord'",
                id="classpath-cannot-be-imported",
            ),
            pytest.param(
                {
                    "classpath": f"{_CoordinatorA.__module__}._CoordinatorA",
                    "kwargs": {"unknown_param": "x"},
                },
                r"Cannot instantiate coordinator 'bad-coord'",
                id="kwargs-dont-match-constructor",
            ),
        ],
    )
    def test_for_queue_raises_invalid_coordinator_error(self, sdk_config, coordinator_spec, expected_match):
        sdk_config(
            coordinators=json.dumps({"bad-coord": coordinator_spec}),
            queue_to_coordinator=json.dumps({"queue-bad": "bad-coord"}),
        )
        manager = CoordinatorManager.from_config()
        with pytest.raises(InvalidCoordinatorError, match=expected_match):
            manager.for_queue("queue-bad")

    def test_for_queue_raises_when_spec_missing(self):
        manager = CoordinatorManager(
            coordinator_specs={},
            queue_to_coordinator={"queue-bad": "missing"},
        )
        with pytest.raises(
            InvalidCoordinatorError,
            match=r"Queue 'queue-bad' configured to nonexistent coordinator",
        ):
            manager.for_queue("queue-bad")

    def test_get_coordinator_manager_is_cached(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW__SDK__COORDINATORS", raising=False)

        from airflow.sdk.configuration import conf

        conf.invalidate_cache()

        m1 = get_coordinator_manager()
        m2 = get_coordinator_manager()
        assert m1 is m2

    @pytest.mark.parametrize(
        ("queue", "expected"),
        [
            pytest.param(
                "queue-java",
                {"pod_template_file": "/opt/airflow/pod_templates/java.yaml"},
                id="mapped-with-extra",
            ),
            pytest.param("queue-go", None, id="mapped-without-extra"),
            pytest.param("queue-unmapped", None, id="unmapped-queue"),
        ],
    )
    def test_extra_for_queue(self, sdk_config, queue, expected):
        sdk_config(
            coordinators=json.dumps(
                {
                    "java": {
                        "classpath": f"{_CoordinatorA.__module__}._CoordinatorA",
                        "extra": {"pod_template_file": "/opt/airflow/pod_templates/java.yaml"},
                    },
                    "go": {"classpath": f"{_CoordinatorB.__module__}._CoordinatorB"},
                }
            ),
            queue_to_coordinator=json.dumps({"queue-java": "java", "queue-go": "go"}),
        )
        manager = CoordinatorManager.from_config()
        # Resolving the extra must not instantiate the coordinator.
        assert manager.extra_for_queue(queue) == expected
        assert manager._created_coordinators == {}

    def test_extra_not_forwarded_to_constructor(self, sdk_config):
        """``extra`` is kept separate from ``kwargs`` and never reaches the coordinator constructor."""
        sdk_config(
            coordinators=json.dumps(
                {
                    "java": {
                        "classpath": f"{_CoordinatorA.__module__}._CoordinatorA",
                        "kwargs": {"label": "java-label"},
                        "extra": {"pod_template_file": "/opt/airflow/pod_templates/java.yaml"},
                    },
                }
            ),
            queue_to_coordinator=json.dumps({"queue-java": "java"}),
        )
        manager = CoordinatorManager.from_config()
        # _CoordinatorA only accepts ``label``; construction would raise TypeError
        # if ``extra`` were passed through.
        coordinator = manager.for_queue("queue-java")
        assert isinstance(coordinator, _CoordinatorA)
        assert coordinator.label == "java-label"
        # The extra is still readable from the spec without instantiation cost.
        assert manager.extra_for_queue("queue-java") == {
            "pod_template_file": "/opt/airflow/pod_templates/java.yaml"
        }

    def test_extra_for_queue_does_not_instantiate_coordinator(self, sdk_config):
        """Reading ``extra`` reads only the spec; a failing constructor must never run."""
        sdk_config(
            coordinators=json.dumps(
                {
                    "boom": {
                        "classpath": f"{_ExplodingCoordinator.__module__}._ExplodingCoordinator",
                        "extra": {"pod_template_file": "/opt/airflow/pod_templates/boom.yaml"},
                    },
                }
            ),
            queue_to_coordinator=json.dumps({"queue-boom": "boom"}),
        )
        manager = CoordinatorManager.from_config()
        assert manager.extra_for_queue("queue-boom") == {
            "pod_template_file": "/opt/airflow/pod_templates/boom.yaml"
        }
        assert manager._created_coordinators == {}


class TestConfigYamlCoordinatorsExample:
    """Guard the ``[sdk] coordinators`` example in ``config.yml`` against drift.

    Nothing else exercises the example, so a broken one (e.g. dropping the
    required ``jars_root`` kwarg) can ship unnoticed. Loading it through
    CoordinatorManager and constructing every entry keeps the example honest.
    """

    def test_every_example_coordinator_constructs(self, sdk_config):
        description = retrieve_configuration_description()
        coordinators_example = description["sdk"]["options"]["coordinators"]["example"]
        specs = json.loads(coordinators_example)
        assert specs, "config.yml [sdk] coordinators example must not be empty"

        # The example's own queue_to_coordinator illustrates different keys, so
        # route every coordinator through a synthetic queue to construct each one.
        queue_to_coordinator = {f"queue-{key}": key for key in specs}
        sdk_config(
            coordinators=coordinators_example,
            queue_to_coordinator=json.dumps(queue_to_coordinator),
        )
        manager = CoordinatorManager.from_config()
        assert set(manager._coordinator_specs) == set(specs)

        for queue, key in queue_to_coordinator.items():
            coordinator = manager.for_queue(queue)
            assert isinstance(coordinator, import_string(specs[key]["classpath"]))


class TestWarmShutdownSignals:
    """Tests for the warm-shutdown signal handling that wraps task supervision."""

    @pytest.fixture(autouse=True)
    def _restore_disposition(self):
        """Guarantee SIGTERM/SIGINT dispositions are restored even if a test leaks one."""
        original_term = signal.getsignal(signal.SIGTERM)
        original_int = signal.getsignal(signal.SIGINT)
        yield
        signal.signal(signal.SIGTERM, original_term)
        signal.signal(signal.SIGINT, original_int)

    def test_installs_handlers_inside_context(self):
        """While the context is active a warm-shutdown handler is installed for both signals."""
        sentinel_term = signal.getsignal(signal.SIGTERM)
        sentinel_int = signal.getsignal(signal.SIGINT)

        with _warm_shutdown_signals():
            inside_term = signal.getsignal(signal.SIGTERM)
            inside_int = signal.getsignal(signal.SIGINT)

        assert callable(inside_term)
        assert callable(inside_int)
        # The installed handler is the warm-shutdown closure, not the previous disposition.
        assert inside_term is not sentinel_term
        assert inside_int is not sentinel_int
        # Both signals share the same warm-shutdown closure.
        assert inside_term is inside_int

    def test_restores_previous_dispositions_on_exit(self):
        """The exact previous dispositions are restored when the context exits normally."""

        def _prev_term(signum, frame):  # pragma: no cover - never invoked
            pass

        def _prev_int(signum, frame):  # pragma: no cover - never invoked
            pass

        signal.signal(signal.SIGTERM, _prev_term)
        signal.signal(signal.SIGINT, _prev_int)

        with _warm_shutdown_signals():
            pass

        assert signal.getsignal(signal.SIGTERM) is _prev_term
        assert signal.getsignal(signal.SIGINT) is _prev_int

    def test_restores_previous_dispositions_on_exception(self):
        """Dispositions are restored even if the wrapped body raises."""

        def _prev_term(signum, frame):  # pragma: no cover - never invoked
            pass

        signal.signal(signal.SIGTERM, _prev_term)

        with pytest.raises(RuntimeError, match="boom"), _warm_shutdown_signals():
            raise RuntimeError("boom")

        assert signal.getsignal(signal.SIGTERM) is _prev_term

    def test_sigterm_inside_context_does_not_kill(self):
        """
        A SIGTERM delivered while supervising must be swallowed, not kill the process.

        This is the regression guard: with the default SIGTERM disposition (SIG_DFL)
        in place as the *previous* handler, sending SIGTERM to ourselves would
        terminate the process. The warm-shutdown handler installed by the context
        manager must absorb it so the running task is allowed to finish.
        """
        # Make the pre-context disposition the default so a missing warm-shutdown
        # handler would actually kill this process (and fail the test by dying).
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

        reached_after_signal = False
        with _warm_shutdown_signals():
            os.kill(os.getpid(), signal.SIGTERM)
            # If the handler did not absorb the signal, we never get here.
            reached_after_signal = True

        assert reached_after_signal
        # And the default disposition is put back afterwards.
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


class TestPythonCoordinatorWarmShutdown:
    """The Python coordinator must wrap start() and wait() in the warm-shutdown handlers."""

    def test_execute_task_wraps_start_and_wait(self, monkeypatch):
        """
        Handlers are installed for the whole start()+wait() window and restored after.

        Capturing the SIGTERM disposition at the moment ``start()`` and ``wait()``
        run proves the handler spans the RUNNING transition (start) and the
        terminal-state report / log upload (wait), with no window left uncovered.
        """
        original_term = signal.getsignal(signal.SIGTERM)
        captured: dict[str, object] = {}

        class _FakeProcess:
            final_state = "success"

            def wait(self_inner):
                captured["wait"] = signal.getsignal(signal.SIGTERM)
                return 0

        def _fake_start(*args, **kwargs):
            captured["start"] = signal.getsignal(signal.SIGTERM)
            return _FakeProcess()

        import airflow.sdk.execution_time.supervisor as supervisor_mod

        monkeypatch.setattr(supervisor_mod.ActivitySubprocess, "start", staticmethod(_fake_start))

        coordinator = _PythonCoordinator()
        result = coordinator.execute_task(
            what=mock.MagicMock(),
            dag_rel_path="some_dag.py",
            bundle_info=mock.MagicMock(),
            client=mock.MagicMock(),
            subprocess_logs_to_stdout=False,
        )

        assert result.exit_code == 0
        assert result.final_state == "success"
        # During both start() and wait() a warm-shutdown handler was installed...
        assert callable(captured["start"])
        assert callable(captured["wait"])
        assert captured["start"] is not original_term
        assert captured["start"] is captured["wait"]
        # ...and the original disposition is restored once execute_task returns.
        assert signal.getsignal(signal.SIGTERM) is original_term
