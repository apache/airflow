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

import textwrap
from types import SimpleNamespace

import pytest

from airflow.sdk.importers.base import FilesystemDagDefinition
from airflow.sdk.importers.yaml_importer.importer import YamlDagImporter
from airflow.sdk.task_handlers import TaskHandlerDeclaration, TaskHandlerParam

HEAD_SCHEMA = "https://airflow.apache.org/schemas/dag/2026-10-30.json"


@pytest.fixture
def mock_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(name="test_bundle", path=bundle_dir)


def _build(mock_bundle, tasks: str):
    body = f"$schema: {HEAD_SCHEMA}\ndag_id: d\ntasks:\n{textwrap.indent(textwrap.dedent(tasks), '  ')}"
    path = mock_bundle.path / "dag.yaml"
    path.write_text(body)
    definition = FilesystemDagDefinition(path=path)
    return YamlDagImporter().import_definition(definition, bundle=mock_bundle)


def _patch_declaration(monkeypatch, declaration):
    coordinator = SimpleNamespace(describe_task_handler=lambda dag_id, task_id: declaration)
    manager = SimpleNamespace(for_queue=lambda queue: coordinator)
    monkeypatch.setattr("airflow.sdk.execution_time.coordinator.get_coordinator_manager", lambda: manager)


def test_no_declaration_available_skips_and_builds(mock_bundle):
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors
    assert getattr(result.dags[0].get_task("t"), "is_stub", False) is True


def test_matching_arguments_pass(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration(task_id="t", params=[TaskHandlerParam(name="region", required=True)]),
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors


def test_missing_required_argument_errors(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration(task_id="t", params=[TaskHandlerParam(name="region", required=True)]),
    )
    result = _build(mock_bundle, "- {id: t, run: {}}")
    assert not result.dags
    assert any("missing required handler argument(s) ['region']" in e.message for e in result.errors)


def test_unknown_run_argument_errors(mock_bundle, monkeypatch):
    _patch_declaration(
        monkeypatch,
        TaskHandlerDeclaration(task_id="t", params=[TaskHandlerParam(name="region", required=True)]),
    )
    result = _build(mock_bundle, "- {id: t, run: {region: eu, bogus: 1}}")
    assert not result.dags
    assert any("run argument(s) ['bogus'] are not parameters" in e.message for e in result.errors)


def test_none_declaration_skips(mock_bundle, monkeypatch):
    _patch_declaration(monkeypatch, None)
    result = _build(mock_bundle, "- {id: t, run: {region: eu}}")
    assert result.success, result.errors
