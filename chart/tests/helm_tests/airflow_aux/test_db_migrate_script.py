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
"""Unit tests for the embedded bidirectional reconciler ``chart/files/db_migrate.py``.

These exercise the runtime behaviour of the script itself (the helm template
tests only check that the script is embedded with the right env/args/RBAC).
"""

from __future__ import annotations

import importlib.util
import pathlib
import types
from unittest import mock

import pytest
from kubernetes import client as k8s_client
from sqlalchemy.exc import OperationalError

DB_MIGRATE_PATH = pathlib.Path(__file__).resolve().parents[3] / "files" / "db_migrate.py"


@pytest.fixture(scope="module")
def db_migrate():
    """Load ``chart/files/db_migrate.py`` as a module.

    The file is normally fed to ``python3 -c`` by the helm job rather than
    imported, so it lives outside the chart's Python package tree.
    """
    spec = importlib.util.spec_from_file_location("chart_db_migrate", DB_MIGRATE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------------


@pytest.fixture
def patched_revision_map(db_migrate, monkeypatch):
    """Install a stable ``{version: revision}`` mapping for ``decide_action`` tests."""
    fake_map = {
        "3.0.0": "rev_300",
        "3.1.0": "rev_310",
        "3.2.0": "rev_320",
    }
    monkeypatch.setattr(db_migrate, "_REVISION_HEADS_MAP", fake_map)
    return fake_map


@pytest.fixture
def patch_engine_returning(db_migrate, monkeypatch):
    """Factory: patch ``engine.connect()`` so ``get_current_revision()`` returns the given value."""

    def _patch(current_rev):
        class _Ctx:
            def get_current_revision(self):
                return current_rev

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        monkeypatch.setattr(db_migrate.engine, "connect", lambda: _Conn())
        monkeypatch.setattr(
            db_migrate.MigrationContext,
            "configure",
            staticmethod(lambda _conn: _Ctx()),
        )

    return _patch


@pytest.fixture
def make_pod():
    """Factory: build a fake api-server pod with optional ``Ready`` condition."""

    def _build(name, ready=True):
        return types.SimpleNamespace(
            metadata=types.SimpleNamespace(name=name),
            status=types.SimpleNamespace(
                conditions=[types.SimpleNamespace(type="Ready", status="True" if ready else "False")]
            ),
        )

    return _build


@pytest.fixture
def fake_stream():
    """Factory: build a fake kubernetes exec-stream response with the given exit code."""

    def _build(returncode):
        resp = mock.MagicMock()
        # is_open(): True once so the loop body runs, then False to exit.
        resp.is_open.side_effect = [True, False]
        resp.peek_stdout.return_value = False
        resp.peek_stderr.return_value = False
        resp.returncode = returncode
        return resp

    return _build


@pytest.fixture
def make_workload():
    """Factory: build a fake Deployment / StatefulSet object with a ``metadata.name``."""

    def _build(name):
        return types.SimpleNamespace(metadata=types.SimpleNamespace(name=name))

    return _build


# --------------------------------------------------------------------------
# decide_action
# --------------------------------------------------------------------------


def test_decide_action_unknown_target_falls_back_to_forward(db_migrate, monkeypatch):
    # Empty map: any target is unknown -> conservative forward migrate.
    monkeypatch.setattr(db_migrate, "_REVISION_HEADS_MAP", {})
    assert db_migrate.decide_action("9.9.9") == "forward"


def test_decide_action_fresh_when_db_unreachable(db_migrate, monkeypatch, patched_revision_map):
    def _raise(*_a, **_kw):
        raise OperationalError("SELECT 1", {}, Exception("unreachable"))

    monkeypatch.setattr(db_migrate.engine, "connect", _raise)
    assert db_migrate.decide_action("3.1.0") == "fresh"


def test_decide_action_fresh_when_no_alembic_row(patched_revision_map, db_migrate, patch_engine_returning):
    patch_engine_returning(current_rev=None)
    assert db_migrate.decide_action("3.1.0") == "fresh"


def test_decide_action_noop_when_current_equals_target(
    patched_revision_map, db_migrate, patch_engine_returning
):
    patch_engine_returning(current_rev="rev_310")
    assert db_migrate.decide_action("3.1.0") == "noop"


def test_decide_action_forward_when_current_is_older_than_target(
    patched_revision_map, db_migrate, patch_engine_returning
):
    # current=3.0.0 (rev_300), target=3.1.0 (rev_310) -> forward.
    patch_engine_returning(current_rev="rev_300")
    assert db_migrate.decide_action("3.1.0") == "forward"


def test_decide_action_downgrade_when_current_is_newer_than_target(
    patched_revision_map, db_migrate, patch_engine_returning
):
    """Regression test for the original blocker.

    On a real downgrade the TARGET image cannot resolve a revision newer than
    its own head; relying on ``ScriptDirectory.walk_revisions`` raised
    ``RevisionError`` and skipped the downgrade branch entirely. The new
    implementation uses a version-aware reverse lookup so the comparison
    works no matter which image is loaded.
    """
    # current=3.2.0 (rev_320, newer), target=3.1.0 (older) -> downgrade.
    patch_engine_returning(current_rev="rev_320")
    assert db_migrate.decide_action("3.1.0") == "downgrade"


def test_decide_action_forward_when_current_revision_is_unknown(
    patched_revision_map, db_migrate, patch_engine_returning
):
    """Dev / intermediate alembic rev -> conservative forward, never downgrade."""
    patch_engine_returning(current_rev="rev_unknown")
    assert db_migrate.decide_action("3.1.0") == "forward"


def test_decide_action_resolves_patch_version_via_nearest_lower(
    db_migrate, monkeypatch, patch_engine_returning
):
    """Regression test for the Copilot-flagged patch-version mapping bug.

    ``_REVISION_HEADS_MAP`` does not list every patch (e.g. ``3.2.2`` is not
    a key — its head is the same as ``3.2.0``'s). The resolver must fall back
    to the highest lower mapped version so a deploy targeting a patch release
    is correctly classified as a no-op or downgrade against the same rev.
    """
    monkeypatch.setattr(db_migrate, "_REVISION_HEADS_MAP", {"3.2.0": "rev_320"})
    patch_engine_returning(current_rev="rev_320")
    # 3.2.2 must resolve to rev_320 -> noop, not "forward" (the previous bug).
    assert db_migrate.decide_action("3.2.2") == "noop"


# --------------------------------------------------------------------------
# discover_api_server_pod
# --------------------------------------------------------------------------


def _call_discover_no_retry(db_migrate, namespace):
    """Invoke the underlying function, bypassing the @retry wrapper backoff."""
    return db_migrate.discover_api_server_pod.retry_with(stop=lambda _r: True)(namespace)


def test_discover_api_server_pod_prefers_ready(db_migrate, monkeypatch, make_pod):
    fake_api = mock.MagicMock(spec=k8s_client.CoreV1Api)
    fake_api.list_namespaced_pod.return_value.items = [
        make_pod("api-server-old", ready=False),
        make_pod("api-server-new", ready=True),
    ]
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    assert db_migrate.discover_api_server_pod("airflow") == "api-server-new"
    fake_api.list_namespaced_pod.assert_called_once_with(
        namespace="airflow",
        label_selector="component=api-server",
        field_selector="status.phase=Running",
    )


def test_discover_api_server_pod_falls_back_to_non_ready(db_migrate, monkeypatch, make_pod):
    fake_api = mock.MagicMock(spec=k8s_client.CoreV1Api)
    fake_api.list_namespaced_pod.return_value.items = [make_pod("api-server-old", ready=False)]
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    assert db_migrate.discover_api_server_pod("airflow") == "api-server-old"


def test_discover_api_server_pod_raises_when_none(db_migrate, monkeypatch):
    fake_api = mock.MagicMock(spec=k8s_client.CoreV1Api)
    fake_api.list_namespaced_pod.return_value.items = []
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    # Bypass the retry's exponential backoff so the test runs instantly.
    monkeypatch.setattr(db_migrate.discover_api_server_pod.retry, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="no Running api-server pod"):
        db_migrate.discover_api_server_pod("airflow")


def test_discover_api_server_pod_retry_recovers_after_transient_empty(db_migrate, monkeypatch, make_pod):
    """An api-server rollover (one empty poll followed by a Ready pod) must not fail the job."""
    fake_api = mock.MagicMock(spec=k8s_client.CoreV1Api)
    empty_resp = mock.MagicMock()
    empty_resp.items = []
    ready_resp = mock.MagicMock()
    ready_resp.items = [make_pod("api-server-new", ready=True)]
    fake_api.list_namespaced_pod.side_effect = [empty_resp, ready_resp]

    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    monkeypatch.setattr(db_migrate.discover_api_server_pod.retry, "sleep", lambda _s: None)

    assert db_migrate.discover_api_server_pod("airflow") == "api-server-new"
    assert fake_api.list_namespaced_pod.call_count == 2


# --------------------------------------------------------------------------
# run_downgrade_in_api_server
# --------------------------------------------------------------------------


def test_run_downgrade_returns_zero_on_success(db_migrate, monkeypatch, fake_stream):
    resp = fake_stream(returncode=0)
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", mock.MagicMock)
    monkeypatch.setattr(db_migrate, "stream", lambda *a, **kw: resp)
    assert db_migrate.run_downgrade_in_api_server("p", "ns", "3.0.0") == 0


def test_run_downgrade_propagates_nonzero(db_migrate, monkeypatch, fake_stream):
    resp = fake_stream(returncode=2)
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", mock.MagicMock)
    monkeypatch.setattr(db_migrate, "stream", lambda *a, **kw: resp)
    assert db_migrate.run_downgrade_in_api_server("p", "ns", "3.0.0") == 2


def test_run_downgrade_treats_missing_returncode_as_failure(db_migrate, monkeypatch, fake_stream):
    # Regression: previous code did ``return returncode or 0`` which silently
    # reported success when the stream closed without an exit code.
    resp = fake_stream(returncode=None)
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", mock.MagicMock)
    monkeypatch.setattr(db_migrate, "stream", lambda *a, **kw: resp)
    assert db_migrate.run_downgrade_in_api_server("p", "ns", "3.0.0") == 1


# --------------------------------------------------------------------------
# scale_release_workloads_to_zero
# --------------------------------------------------------------------------


def test_scale_release_workloads_patches_all_and_returns_when_drained(db_migrate, monkeypatch, make_workload):
    apps = mock.MagicMock(spec=k8s_client.AppsV1Api)
    core = mock.MagicMock(spec=k8s_client.CoreV1Api)
    apps.list_namespaced_deployment.return_value.items = [
        make_workload("api-server"),
        make_workload("scheduler"),
    ]
    apps.list_namespaced_stateful_set.return_value.items = [make_workload("triggerer")]
    core.list_namespaced_pod.side_effect = [
        mock.MagicMock(items=[mock.MagicMock()]),
        mock.MagicMock(items=[]),
    ]

    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "AppsV1Api", lambda: apps)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(db_migrate.time, "sleep", lambda _s: None)

    db_migrate.scale_release_workloads_to_zero("airflow", "my-release", timeout_seconds=10)

    expected_selector = (
        "release=my-release,component in (api-server,scheduler,triggerer,dag-processor,worker)"
    )
    apps.list_namespaced_deployment.assert_called_once_with("airflow", label_selector=expected_selector)
    apps.list_namespaced_stateful_set.assert_called_once_with("airflow", label_selector=expected_selector)
    deploy_calls = apps.patch_namespaced_deployment_scale.call_args_list
    sts_calls = apps.patch_namespaced_stateful_set_scale.call_args_list
    assert sorted(c.args[0] for c in deploy_calls) == ["api-server", "scheduler"]
    assert [c.args[0] for c in sts_calls] == ["triggerer"]
    for c in deploy_calls + sts_calls:
        assert c.args[2] == {"spec": {"replicas": 0}}


def test_scale_release_workloads_raises_on_timeout(db_migrate, monkeypatch):
    apps = mock.MagicMock(spec=k8s_client.AppsV1Api)
    core = mock.MagicMock(spec=k8s_client.CoreV1Api)
    apps.list_namespaced_deployment.return_value.items = []
    apps.list_namespaced_stateful_set.return_value.items = []
    core.list_namespaced_pod.return_value.items = [mock.MagicMock()]

    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "AppsV1Api", lambda: apps)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(db_migrate.time, "sleep", lambda _s: None)
    times = iter([0.0, 999.0])
    monkeypatch.setattr(db_migrate.time, "monotonic", lambda: next(times))

    with pytest.raises(TimeoutError, match="did not drain"):
        db_migrate.scale_release_workloads_to_zero("airflow", "rel", timeout_seconds=1)


def test_scale_release_workloads_honors_drain_timeout_env_var(db_migrate, monkeypatch):
    """Operators can raise the drain timeout via ``MIGRATE_JOB_DRAIN_TIMEOUT_SECONDS``."""
    apps = mock.MagicMock(spec=k8s_client.AppsV1Api)
    core = mock.MagicMock(spec=k8s_client.CoreV1Api)
    apps.list_namespaced_deployment.return_value.items = []
    apps.list_namespaced_stateful_set.return_value.items = []
    core.list_namespaced_pod.return_value.items = [mock.MagicMock()]

    monkeypatch.setenv("MIGRATE_JOB_DRAIN_TIMEOUT_SECONDS", "7200")
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "AppsV1Api", lambda: apps)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(db_migrate.time, "sleep", lambda _s: None)
    times = iter([0.0, 1_000_000.0])
    monkeypatch.setattr(db_migrate.time, "monotonic", lambda: next(times))

    with pytest.raises(TimeoutError, match=r"did not drain within 7200s"):
        db_migrate.scale_release_workloads_to_zero("airflow", "rel")
