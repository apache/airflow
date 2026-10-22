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
from airflow.sdk.task_handlers import (
    get_handler,
    import_handler_modules,
    loader as loader_mod,
    resolve_handler_bundle,
)
from airflow.sdk.task_handlers.registry import _REGISTRY


def _purge_handler_modules():
    for name in [n for n in sys.modules if n.startswith(UNUSUAL_MODULE_PREFIX)]:
        del sys.modules[name]


@pytest.fixture(autouse=True)
def _clean():
    _REGISTRY.clear()
    loader_mod._imported_bundles.clear()
    loader_mod._resolved_bundles.clear()
    _purge_handler_modules()
    yield
    _REGISTRY.clear()
    loader_mod._imported_bundles.clear()
    loader_mod._resolved_bundles.clear()
    _purge_handler_modules()


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


def _materialized(name, path):
    """Stand-in for an already-materialized bundle (name + path)."""
    return SimpleNamespace(name=name, version=None, path=path)


class TestImportHandlerModules:
    def test_imports_modules_and_populates_registry(self, tmp_path):
        _write_handler(tmp_path / "handlers.py", "events", "transform")

        import_handler_modules(_materialized("b", tmp_path))

        assert get_handler("events", "transform") is not None

    def test_handler_module_can_use_dataclass_with_future_annotations(self, tmp_path):
        (tmp_path / "handlers.py").write_text(
            textwrap.dedent(
                """
                from __future__ import annotations

                from dataclasses import dataclass

                from airflow.sdk import task_handler

                @dataclass
                class Config:
                    factor: int

                @task_handler("d", "t")
                def t(region):
                    return Config(factor=2)
                """
            )
        )

        import_handler_modules(_materialized("b", tmp_path))

        assert get_handler("d", "t") is not None

    def test_airflowignore_skips_ignored_files(self, tmp_path):
        _write_handler(tmp_path / "handlers.py", "d", "keep", params="a")
        _write_handler(tmp_path / "scratch.py", "d", "skip", params="a")
        (tmp_path / ".airflowignore").write_text("scratch.py\n")

        import_handler_modules(_materialized("b", tmp_path))

        assert get_handler("d", "keep") is not None
        assert get_handler("d", "skip") is None

    def test_idempotent_by_bundle_name(self, tmp_path):
        # A re-import would re-run @task_handler and hit the duplicate-registration guard.
        _write_handler(tmp_path / "handlers.py", "d", "t", params="a")
        bundle = _materialized("b", tmp_path)

        import_handler_modules(bundle)
        import_handler_modules(bundle)

        assert get_handler("d", "t") is not None

    def test_two_bundles_with_same_filename_do_not_collide(self, tmp_path):
        b1, b2 = tmp_path / "b1", tmp_path / "b2"
        b1.mkdir()
        b2.mkdir()
        _write_handler(b1 / "handlers.py", "dag1", "a", params="x")
        _write_handler(b2 / "handlers.py", "dag2", "b", params="y")

        import_handler_modules(_materialized("one", b1))
        import_handler_modules(_materialized("two", b2))

        assert get_handler("dag1", "a") is not None
        assert get_handler("dag2", "b") is not None


class TestResolveHandlerBundle:
    def test_materializes_and_pins_once(self, monkeypatch):
        calls = {"pin": 0}
        pinned = SimpleNamespace(name="b", version="v1", path="/does-not-matter")

        def _pin(target, logger):
            calls["pin"] += 1
            return pinned

        monkeypatch.setattr("airflow.sdk.execution_time.bundles.initialize_pinned_bundle", _pin)

        assert resolve_handler_bundle("b") is pinned
        assert resolve_handler_bundle("b") is pinned  # memoized: materialized once per process
        assert calls["pin"] == 1

    def test_resolve_then_import_populates_registry(self, tmp_path, monkeypatch):
        _write_handler(tmp_path / "handlers.py", "d", "t", params="a")
        pinned = _materialized("b", tmp_path)
        monkeypatch.setattr(
            "airflow.sdk.execution_time.bundles.initialize_pinned_bundle",
            lambda target, logger: pinned,
        )

        import_handler_modules(resolve_handler_bundle("b"))

        assert get_handler("d", "t") is not None
