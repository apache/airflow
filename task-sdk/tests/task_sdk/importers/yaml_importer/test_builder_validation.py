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
from airflow.sdk.importers.base import FilesystemDagDefinition
from airflow.sdk.importers.yaml_importer.importer import YamlDagImporter
from airflow.sdk.task_handlers import TaskHandlerDeclaration, TaskHandlerParam, loader as loader_mod
from airflow.sdk.task_handlers.registry import _REGISTRY

HEAD_SCHEMA = "https://airflow.apache.org/schemas/dag/2026-10-30.json"


@pytest.fixture(autouse=True)
def _clean_handlers():
    def _reset():
        _REGISTRY.clear()
        loader_mod._imported_bundles.clear()
        loader_mod._resolved_bundles.clear()
        for name in [n for n in sys.modules if n.startswith(UNUSUAL_MODULE_PREFIX)]:
            del sys.modules[name]

    _reset()
    yield
    _reset()


@pytest.fixture
def mock_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(name="test_bundle", version=None, path=bundle_dir)


def _build(mock_bundle, tasks: str):
    body = f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks:\n{textwrap.indent(textwrap.dedent(tasks), '  ')}"
    path = mock_bundle.path / "dag.yaml"
    path.write_text(body)
    definition = FilesystemDagDefinition(path=path)
    return YamlDagImporter().import_definition(definition, bundle=mock_bundle)


def _patch_declaration(monkeypatch, declaration):
    coordinator = SimpleNamespace(parse_task_handler=lambda dag_id, task_id, bundle: declaration)
    manager = SimpleNamespace(for_queue=lambda queue: coordinator)
    monkeypatch.setattr("airflow.sdk.execution_time.coordinator.get_coordinator_manager", lambda: manager)


def test_no_declaration_available_skips_and_builds(mock_bundle):
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors
    assert getattr(result.dags[0].get_task("t"), "is_stub", False) is True


def test_matching_arguments_pass(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration([TaskHandlerParam(name="region", required=True)]),
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors


def test_missing_required_argument_errors(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration([TaskHandlerParam(name="region", required=True)]),
    )
    result = _build(mock_bundle, "- {id: t, run: {}}")
    assert not result.dags
    assert any("missing required handler argument(s) ['region']" in e.message for e in result.errors)


def test_unknown_run_argument_errors(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration([TaskHandlerParam(name="region", required=True)]),
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu, bogus: 1}}")
    assert not result.dags
    assert any("run argument(s) ['bogus'] are not parameters" in e.message for e in result.errors)


def _patch_raising_coordinator(monkeypatch, exc):
    def describe(dag_id, task_id, bundle):
        raise exc

    coordinator = SimpleNamespace(parse_task_handler=describe)
    manager = SimpleNamespace(for_queue=lambda queue: coordinator)
    monkeypatch.setattr("airflow.sdk.execution_time.coordinator.get_coordinator_manager", lambda: manager)


def test_handler_resolution_failure_skips_and_builds(mock_bundle, monkeypatch):
    _patch_raising_coordinator(monkeypatch, RuntimeError("bundle fetch failed"))
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors
    assert getattr(result.dags[0].get_task("t"), "is_stub", False) is True


def test_optional_argument_may_be_omitted(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration(
            params=[
                TaskHandlerParam(name="region", required=True),
                TaskHandlerParam(name="limit", required=False),
            ],
        ),
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors


def test_coordinator_resolved_by_task_queue(mock_bundle, monkeypatch):
    queues_seen = []
    declaration = TaskHandlerDeclaration([TaskHandlerParam(name="region", required=True)])
    coordinator = SimpleNamespace(parse_task_handler=lambda dag_id, task_id, bundle: declaration)

    def for_queue(queue):
        queues_seen.append(queue)
        return coordinator

    monkeypatch.setattr(
        "airflow.sdk.execution_time.coordinator.get_coordinator_manager",
        lambda: SimpleNamespace(for_queue=for_queue),
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors
    assert queues_seen == [result.dags[0].get_task("t").queue]


def test_colocated_handler_is_validated(mock_bundle):
    # With no configured handler bundle, a handler in the stub's own bundle is
    # auto-discovered and validated at parse time -- no coordinator patch here.
    (mock_bundle.path / "handlers.py").write_text(
        textwrap.dedent(
            """
            from airflow.sdk import task_handler

            @task_handler("d", "t")
            def t(region):
                return region
            """
        )
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu, bogus: 1}}")
    assert not result.dags
    assert any("run argument(s) ['bogus'] are not parameters" in e.message for e in result.errors)
