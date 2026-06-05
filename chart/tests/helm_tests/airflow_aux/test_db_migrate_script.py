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
The decide_action regression test exists because the first iteration walked
the target image's ScriptDirectory from base to ``current_rev``, which raises
``RevisionError`` in the actual downgrade case where ``current_rev`` is newer
than anything the target image knows about. See
https://github.com/apache/airflow/issues/68072.
"""

from __future__ import annotations

import importlib.util
import pathlib
import types
from unittest import mock

import pytest
from sqlalchemy.exc import OperationalError

DB_MIGRATE_PATH = pathlib.Path(__file__).resolve().parents[3] / "files" / "db_migrate.py"


@pytest.fixture(scope="module")
def db_migrate():
    """Load chart/files/db_migrate.py as a module.

    The file is normally fed to ``python3 -c`` by the helm job rather than
    imported, so it lives outside the chart's Python package tree.
    """
    spec = importlib.util.spec_from_file_location("chart_db_migrate", DB_MIGRATE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# decide_action
# --------------------------------------------------------------------------


@pytest.fixture
def patched_revision_map(db_migrate, monkeypatch):
    """Install a stable {version: revision} mapping for decide_action tests."""
    fake_map = {
        "3.0.0": "rev_300",
        "3.1.0": "rev_310",
        "3.2.0": "rev_320",
    }
    monkeypatch.setattr(db_migrate, "_REVISION_HEADS_MAP", fake_map)
    return fake_map


def _patch_engine_returning(db_migrate, monkeypatch, current_rev):
    """Patch ``engine.connect()`` so MigrationContext.get_current_revision() returns *current_rev*."""

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


def _patch_script_dir(db_migrate, monkeypatch, ancestors_by_head):
    """Patch ScriptDirectory.from_config so walk_revisions("base", X) yields ancestors_by_head[X]."""

    class _Rev:
        def __init__(self, revision):
            self.revision = revision

    class _ScriptDir:
        def walk_revisions(self, base, head):
            assert base == "base"
            return [_Rev(r) for r in ancestors_by_head[head]]

    monkeypatch.setattr(db_migrate.ScriptDirectory, "from_config", staticmethod(lambda _cfg: _ScriptDir()))


def test_decide_action_unknown_target_falls_back_to_forward(db_migrate, monkeypatch):
    # Empty map: any target is unknown -> conservative forward migrate.
    monkeypatch.setattr(db_migrate, "_REVISION_HEADS_MAP", {})
    assert db_migrate.decide_action("9.9.9") == "forward"


def test_decide_action_fresh_when_db_unreachable(db_migrate, monkeypatch, patched_revision_map):
    def _raise(*_a, **_kw):
        raise OperationalError("SELECT 1", {}, Exception("unreachable"))

    monkeypatch.setattr(db_migrate.engine, "connect", _raise)
    assert db_migrate.decide_action("3.1.0") == "fresh"


def test_decide_action_fresh_when_no_alembic_row(db_migrate, monkeypatch, patched_revision_map):
    _patch_engine_returning(db_migrate, monkeypatch, current_rev=None)
    assert db_migrate.decide_action("3.1.0") == "fresh"


def test_decide_action_noop_when_current_equals_target(db_migrate, monkeypatch, patched_revision_map):
    _patch_engine_returning(db_migrate, monkeypatch, current_rev="rev_310")
    assert db_migrate.decide_action("3.1.0") == "noop"


def test_decide_action_forward_when_current_is_ancestor_of_target(
    db_migrate, monkeypatch, patched_revision_map
):
    # current=3.0.0, target=3.1.0 -> rev_300 is in target's ancestor set.
    _patch_engine_returning(db_migrate, monkeypatch, current_rev="rev_300")
    _patch_script_dir(db_migrate, monkeypatch, ancestors_by_head={"rev_310": ["rev_300", "rev_310"]})
    assert db_migrate.decide_action("3.1.0") == "forward"


def test_decide_action_downgrade_when_current_not_in_target_ancestors(
    db_migrate, monkeypatch, patched_revision_map
):
    """Regression test for the original blocker.

    On a real downgrade, the TARGET image's ScriptDirectory does NOT contain
    ``current_rev`` (current is newer than target). The previous implementation
    called ``walk_revisions("base", current_rev)`` which raised
    ``RevisionError`` and never reached the downgrade branch. The fix walks to
    ``target_rev`` (always present in target's scripts) and checks membership.
    """
    # current=3.2.0 (newer), target=3.1.0 (older). rev_320 is unknown to target.
    _patch_engine_returning(db_migrate, monkeypatch, current_rev="rev_320")
    _patch_script_dir(db_migrate, monkeypatch, ancestors_by_head={"rev_310": ["rev_300", "rev_310"]})
    assert db_migrate.decide_action("3.1.0") == "downgrade"


# --------------------------------------------------------------------------
# discover_api_server_pod
# --------------------------------------------------------------------------


def _pod(name, ready=True):
    pod = types.SimpleNamespace()
    pod.metadata = types.SimpleNamespace(name=name)
    pod.status = types.SimpleNamespace(
        conditions=[types.SimpleNamespace(type="Ready", status="True" if ready else "False")]
    )
    return pod


def test_discover_api_server_pod_prefers_ready(db_migrate, monkeypatch):
    fake_api = mock.MagicMock()
    fake_api.list_namespaced_pod.return_value.items = [
        _pod("api-server-old", ready=False),
        _pod("api-server-new", ready=True),
    ]
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    assert db_migrate.discover_api_server_pod("airflow") == "api-server-new"
    fake_api.list_namespaced_pod.assert_called_once_with(
        namespace="airflow",
        label_selector="component=api-server",
        field_selector="status.phase=Running",
    )


def test_discover_api_server_pod_falls_back_to_non_ready(db_migrate, monkeypatch):
    fake_api = mock.MagicMock()
    fake_api.list_namespaced_pod.return_value.items = [_pod("api-server-old", ready=False)]
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    assert db_migrate.discover_api_server_pod("airflow") == "api-server-old"


def test_discover_api_server_pod_raises_when_none(db_migrate, monkeypatch):
    fake_api = mock.MagicMock()
    fake_api.list_namespaced_pod.return_value.items = []
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: fake_api)
    with pytest.raises(RuntimeError, match="no Running api-server pod"):
        db_migrate.discover_api_server_pod("airflow")


# --------------------------------------------------------------------------
# run_downgrade_in_api_server
# --------------------------------------------------------------------------


def _fake_stream(returncode):
    resp = mock.MagicMock()
    # is_open(): True once so the loop body runs, then False to exit.
    resp.is_open.side_effect = [True, False]
    resp.peek_stdout.return_value = False
    resp.peek_stderr.return_value = False
    resp.returncode = returncode
    return resp


def test_run_downgrade_returns_zero_on_success(db_migrate, monkeypatch):
    resp = _fake_stream(returncode=0)
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: mock.MagicMock())
    monkeypatch.setattr(db_migrate, "stream", lambda *a, **kw: resp)
    assert db_migrate.run_downgrade_in_api_server("p", "ns", "3.0.0") == 0


def test_run_downgrade_propagates_nonzero(db_migrate, monkeypatch):
    resp = _fake_stream(returncode=2)
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: mock.MagicMock())
    monkeypatch.setattr(db_migrate, "stream", lambda *a, **kw: resp)
    assert db_migrate.run_downgrade_in_api_server("p", "ns", "3.0.0") == 2


def test_run_downgrade_treats_missing_returncode_as_failure(db_migrate, monkeypatch):
    # Regression: previous code did ``return returncode or 0`` which silently
    # reported success when the stream closed without an exit code.
    resp = _fake_stream(returncode=None)
    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: mock.MagicMock())
    monkeypatch.setattr(db_migrate, "stream", lambda *a, **kw: resp)
    assert db_migrate.run_downgrade_in_api_server("p", "ns", "3.0.0") == 1


# --------------------------------------------------------------------------
# scale_release_workloads_to_zero
# --------------------------------------------------------------------------


def _wl(name):
    return types.SimpleNamespace(metadata=types.SimpleNamespace(name=name))


def test_scale_release_workloads_patches_all_and_returns_when_drained(db_migrate, monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value.items = [_wl("api-server"), _wl("scheduler")]
    apps.list_namespaced_stateful_set.return_value.items = [_wl("triggerer")]
    # First poll: still one pod. Second poll: drained.
    core.list_namespaced_pod.side_effect = [
        mock.MagicMock(items=[mock.MagicMock()]),
        mock.MagicMock(items=[]),
    ]

    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "AppsV1Api", lambda: apps)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(db_migrate.time, "sleep", lambda _s: None)

    db_migrate.scale_release_workloads_to_zero("airflow", "my-release", timeout_seconds=10)

    # Selector scopes to this release and the DB-touching components.
    expected_selector = (
        "release=my-release,component in (api-server,scheduler,triggerer,dag-processor,worker)"
    )
    apps.list_namespaced_deployment.assert_called_once_with("airflow", label_selector=expected_selector)
    apps.list_namespaced_stateful_set.assert_called_once_with("airflow", label_selector=expected_selector)
    # Both Deployments + the StatefulSet got patched to replicas=0.
    deploy_calls = apps.patch_namespaced_deployment_scale.call_args_list
    sts_calls = apps.patch_namespaced_stateful_set_scale.call_args_list
    assert sorted(c.args[0] for c in deploy_calls) == ["api-server", "scheduler"]
    assert [c.args[0] for c in sts_calls] == ["triggerer"]
    for c in deploy_calls + sts_calls:
        assert c.args[2] == {"spec": {"replicas": 0}}


def test_scale_release_workloads_raises_on_timeout(db_migrate, monkeypatch):
    apps = mock.MagicMock()
    core = mock.MagicMock()
    apps.list_namespaced_deployment.return_value.items = []
    apps.list_namespaced_stateful_set.return_value.items = []
    # Always returns a pod -> never drains.
    core.list_namespaced_pod.return_value.items = [mock.MagicMock()]

    monkeypatch.setattr(db_migrate.k8s_config, "load_incluster_config", lambda: None)
    monkeypatch.setattr(db_migrate.client, "AppsV1Api", lambda: apps)
    monkeypatch.setattr(db_migrate.client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(db_migrate.time, "sleep", lambda _s: None)
    # Force time.monotonic to immediately exceed the deadline on the 2nd call.
    times = iter([0.0, 999.0])
    monkeypatch.setattr(db_migrate.time, "monotonic", lambda: next(times))

    with pytest.raises(TimeoutError, match="did not drain"):
        db_migrate.scale_release_workloads_to_zero("airflow", "rel", timeout_seconds=1)
