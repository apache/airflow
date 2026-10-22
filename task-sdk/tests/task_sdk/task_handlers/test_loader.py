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
import textwrap
from types import SimpleNamespace

import pytest

from airflow.sdk._shared.module_loading.dag_file import UNUSUAL_MODULE_PREFIX
from airflow.sdk.task_handlers import declare_handler, get_handler, load_handler_bundle, loader as loader_mod
from airflow.sdk.task_handlers.registry import _REGISTRY


@pytest.fixture(autouse=True)
def _clean():
    _REGISTRY.clear()
    loader_mod._loaded_bundles.clear()
    stale = [name for name in sys.modules if name.startswith(UNUSUAL_MODULE_PREFIX)]
    for name in stale:
        del sys.modules[name]
    yield
    _REGISTRY.clear()
    loader_mod._loaded_bundles.clear()


def _fake_bundle(monkeypatch, name, path):
    from airflow.dag_processing.bundles import manager as mgr

    bundle = SimpleNamespace(name=name, path=path, initialize=lambda: None)
    monkeypatch.setattr(mgr.DagBundlesManager, "get_bundle", lambda self, bundle_name, *a, **k: bundle)


def _write_handler(path, dag_id, task_id, params="region, events"):
    path.write_text(
        textwrap.dedent(
            f"""
            from airflow.sdk import task_handler

            @task_handler({dag_id!r}, {task_id!r})
            def {task_id}({params}):
                return 1
            """
        )
    )


class TestLoadHandlerBundle:
    def test_imports_modules_and_populates_registry(self, tmp_path, monkeypatch):
        _write_handler(tmp_path / "handlers.py", "events", "transform")
        _fake_bundle(monkeypatch, "b", tmp_path)

        load_handler_bundle("b")

        assert get_handler("events", "transform") is not None

    def test_none_is_a_noop(self):
        load_handler_bundle(None)
        assert get_handler("x", "y") is None

    def test_idempotent_second_call_does_not_reimport(self, tmp_path, monkeypatch):
        # A re-import would re-run @task_handler and hit the duplicate-registration guard.
        _write_handler(tmp_path / "handlers.py", "d", "t", params="a")
        _fake_bundle(monkeypatch, "b", tmp_path)

        load_handler_bundle("b")
        load_handler_bundle("b")

        assert get_handler("d", "t") is not None

    def test_two_bundles_with_same_filename_do_not_collide(self, tmp_path, monkeypatch):
        b1, b2 = tmp_path / "b1", tmp_path / "b2"
        b1.mkdir()
        b2.mkdir()
        _write_handler(b1 / "handlers.py", "dag1", "a", params="x")
        _write_handler(b2 / "handlers.py", "dag2", "b", params="y")

        _fake_bundle(monkeypatch, "one", b1)
        load_handler_bundle("one")
        _fake_bundle(monkeypatch, "two", b2)
        load_handler_bundle("two")

        assert get_handler("dag1", "a") is not None
        assert get_handler("dag2", "b") is not None


class TestDeclarationCarriesHandler:
    def test_declare_handler_attaches_the_callable(self):
        def fn(region, events):
            return len(events)

        decl = declare_handler("transform", fn)
        assert decl.handler is fn
        assert [p.name for p in decl.params] == ["region", "events"]

    def test_handler_is_excluded_from_equality(self):
        def a(x): ...

        def b(x): ...

        # Same task_id + signature compare equal even though the callables differ.
        assert declare_handler("t", a) == declare_handler("t", b)
