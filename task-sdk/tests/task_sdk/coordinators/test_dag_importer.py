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

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airflow.sdk.coordinators._dag_importer import CoordinatorDagImporter
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.importers import DagSourceCode, FilesystemDagDefinition


class _BundleImporter(CoordinatorDagImporter):
    artifact_suffix = ".min.mjs"
    supported_extensions = [".mjs"]

    def get_source_code(self, definition) -> DagSourceCode:
        return DagSourceCode("", "typescript")

    def might_contain_dag(self, definition, safe_mode: bool) -> bool:
        return not definition.path.name.startswith("skip")


@pytest.fixture
def importer() -> _BundleImporter:
    return _BundleImporter(coordinator=MagicMock(spec=SubprocessCoordinator))


@pytest.mark.parametrize(("path", "expected"), [("dags/main.min.mjs", True), ("dags/main.mjs", False)])
def test_handles_only_its_artifacts(importer, path, expected):
    assert importer.can_handle(path) is expected


def test_lists_only_its_artifacts(importer, tmp_path):
    for name in ("main.min.mjs", "helper.mjs", "skip.min.mjs"):
        (tmp_path / name).write_text("")

    definitions = list(importer.list_dag_definitions(SimpleNamespace(name="testing", path=tmp_path)))

    assert [d.path.name for d in definitions] == ["main.min.mjs"]


def test_import_definition_reports_that_only_the_dag_processor_parses_it(importer, tmp_path):
    bundle_file = tmp_path / "main.min.mjs"
    bundle_file.write_text("")
    definition = FilesystemDagDefinition(bundle_file)

    result = importer.import_definition(definition, SimpleNamespace(name="testing", path=tmp_path))

    assert result.dags == []
    assert [error.message for error in result.errors] == [
        "A native Lang-SDK Dag is parsed only by the Dag processor"
    ]
