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
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.coordinators._dag_importer import (
    CoordinatorDagImporter,
    build_coordinator_dag_importers,
    find_claiming_importer,
)
from airflow.sdk.execution_time.coordinator import (
    BaseCoordinator,
    CoordinatorManager,
    InvalidCoordinatorError,
    _CoordinatorSpec,
    get_coordinator_manager,
)
from airflow.sdk.importers import DagSourceCode, FilesystemDagDefinition, reset_importer_registry

from tests_common.test_utils.config import conf_vars


class _NativeCoordinator(BaseCoordinator):
    pass


class _OtherCoordinator(BaseCoordinator):
    pass


class _NativeImporter(CoordinatorDagImporter):
    coordinator_classpath = f"{__name__}._NativeCoordinator"
    artifact_suffix = ".min.native"
    supported_extensions = [".native"]

    def get_source_code(self, definition, dag_id: str | None = None) -> DagSourceCode:
        return DagSourceCode("", "native")

    def might_contain_dag(self, definition, safe_mode: bool) -> bool:
        return not definition.path.name.startswith("skip")


class _OtherImporter(_NativeImporter):
    coordinator_classpath = f"{__name__}._OtherCoordinator"


_IMPORTERS = (f"{__name__}._NativeImporter", f"{__name__}._OtherImporter")


def _coordinator_config(*keys: str, mapping: dict[str, str] | None = None) -> dict[tuple[str, str], str]:
    config = {
        ("sdk", "coordinators"): json.dumps(
            {key: {"classpath": f"{__name__}._NativeCoordinator"} for key in keys}
        )
    }
    if mapping is not None:
        config[("sdk", "dag_bundle_to_coordinator")] = json.dumps(mapping)
    return config


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_importer_registry()
    yield
    reset_importer_registry()


@pytest.fixture
def importer() -> _NativeImporter:
    return _NativeImporter(bundle_name="testing")


@pytest.mark.parametrize(("path", "expected"), [("dags/main.min.native", True), ("dags/main.native", False)])
def test_handles_only_its_artifacts(importer, path, expected):
    assert importer.can_handle(path) is expected


def test_lists_only_its_artifacts(importer, tmp_path):
    for name in ("main.min.native", "helper.native", "skip.min.native"):
        (tmp_path / name).write_text("")

    definitions = list(importer.list_dag_definitions(SimpleNamespace(name="testing", path=tmp_path)))

    assert [d.path.name for d in definitions] == ["main.min.native"]


def test_import_definition_reports_that_only_the_dag_processor_parses_it(importer, tmp_path):
    bundle_file = tmp_path / "main.min.native"
    bundle_file.write_text("")
    definition = FilesystemDagDefinition(bundle_file)

    result = importer.import_definition(definition, SimpleNamespace(name="testing", path=tmp_path))

    assert result.dags == []
    assert [error.message for error in result.errors] == [
        "A native Lang-SDK Dag is parsed only by the Dag processor"
    ]


class TestGetParsingCoordinator:
    def test_returns_the_only_coordinator_of_the_class(self, importer):
        with conf_vars(_coordinator_config("native")):
            coordinator = importer.get_parsing_coordinator()

            assert coordinator is get_coordinator_manager().get_coordinator("native")
        assert isinstance(coordinator, _NativeCoordinator)

    def test_returns_the_coordinator_mapped_to_its_bundle(self, importer):
        with conf_vars(_coordinator_config("first", "second", mapping={"testing": "second"})):
            coordinator = importer.get_parsing_coordinator()

            assert coordinator is get_coordinator_manager().get_coordinator("second")

    def test_raises_when_several_coordinators_and_no_entry_for_its_bundle(self, importer):
        with conf_vars(_coordinator_config("first", "second", mapping={"other": "second"})):
            with pytest.raises(
                InvalidCoordinatorError, match=r"Dag bundle 'testing' has 2 _NativeCoordinator"
            ):
                importer.get_parsing_coordinator()


class TestBuildCoordinatorDagImporters:
    @mock.patch("airflow.sdk.coordinators._dag_importer.COORDINATOR_DAG_IMPORTERS", _IMPORTERS)
    @mock.patch(
        "airflow.sdk.coordinators._dag_importer.import_string", autospec=True, side_effect=import_string
    )
    def test_builds_nothing_and_imports_nothing_without_coordinators(self, mock_import_string):
        manager = CoordinatorManager(coordinator_specs={}, queue_to_coordinator={})

        assert build_coordinator_dag_importers(manager, "testing") == []
        mock_import_string.assert_not_called()

    @mock.patch("airflow.sdk.coordinators._dag_importer.COORDINATOR_DAG_IMPORTERS", _IMPORTERS)
    def test_builds_the_importer_of_each_runtime_that_has_a_coordinator(self):
        manager = CoordinatorManager(
            coordinator_specs={
                "first": _CoordinatorSpec(classpath=f"{__name__}._NativeCoordinator"),
                "second": _CoordinatorSpec(classpath=f"{__name__}._NativeCoordinator"),
            },
            queue_to_coordinator={},
        )

        importers = build_coordinator_dag_importers(manager, "testing")

        assert [type(importer) for importer in importers] == [_NativeImporter]
        assert importers[0].bundle_name == "testing"
        assert manager._created_coordinators == {}


class TestFindClaimingImporter:
    @pytest.fixture(autouse=True)
    def _native_importers(self):
        with mock.patch(
            "airflow.sdk.coordinators._dag_importer.COORDINATOR_DAG_IMPORTERS",
            (f"{__name__}._NativeImporter",),
        ):
            yield

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_returns_the_importer_that_claims_the_file(self, path_type):
        with conf_vars(_coordinator_config("native")):
            claiming = find_claiming_importer(path_type("dags/main.min.native"), "testing")

            assert isinstance(claiming, _NativeImporter)
            assert claiming.bundle_name == "testing"
            assert get_coordinator_manager()._created_coordinators == {}

    @pytest.mark.parametrize(
        "path",
        [
            pytest.param("dags/main.py", id="python-file"),
            pytest.param("dags/main.native", id="last-suffix-only"),
            pytest.param("dags/main.txt", id="unknown-suffix"),
        ],
    )
    def test_returns_none_for_a_file_no_coordinator_importer_claims(self, path):
        with conf_vars(_coordinator_config("native")):
            assert find_claiming_importer(path, "testing") is None

    def test_returns_none_without_coordinators(self):
        with conf_vars({("sdk", "coordinators"): "{}"}):
            assert find_claiming_importer("dags/main.min.native", "testing") is None

    @mock.patch(
        "airflow.sdk.coordinators._dag_importer.COORDINATOR_DAG_IMPORTERS", ("nonexistent.module.Importer",)
    )
    def test_raises_when_the_bundles_importers_could_not_be_built(self):
        with conf_vars(_coordinator_config("native")):
            with pytest.raises(RuntimeError, match="Cannot build the coordinator Dag importers") as exc_info:
                find_claiming_importer("dags/main.py", "testing")

        assert isinstance(exc_info.value.__cause__, ImportError)
