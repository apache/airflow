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

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.dag_processing.lang_sdk_processor import LangSDKDagFileProcessorProcess
from airflow.dag_processing.processor import DagFileParsingResult
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
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG

from tests_common.test_utils.config import conf_vars

FIXTURES = Path(__file__).parent / "fixtures"


class _NativeCoordinator(BaseCoordinator):
    pass


class _OtherCoordinator(BaseCoordinator):
    pass


class _NativeImporter(CoordinatorDagImporter):
    coordinator_classpath = f"{__name__}._NativeCoordinator"
    artifact_suffix = ".min.native"
    supported_extensions = [".native"]

    def get_source_code(self, definition) -> DagSourceCode:
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


def _read_payloads(path: Path) -> list[dict]:
    recorded = json.loads(path.read_text())
    if "serialized_dags" in recorded:
        return [serialized["data"] for serialized in recorded["serialized_dags"]]
    # What the TS SDK's tests/conformance/serialize_typescript.ts writes: payloads keyed by Dag id.
    return list(recorded.values())


def _load_payloads() -> list:
    return [
        pytest.param(data, id=f"{path.stem}-{data['dag']['dag_id']}")
        for path in sorted(FIXTURES.glob("*.json"))
        for data in _read_payloads(path)
    ]


def _get_payload(dag_id: str) -> dict:
    return copy.deepcopy(
        next(p.values[0] for p in _load_payloads() if p.values[0]["dag"]["dag_id"] == dag_id)
    )


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_importer_registry()
    yield
    reset_importer_registry()


@pytest.mark.parametrize("data", _load_payloads())
def test_recorded_runtime_payloads_are_valid(data):
    DagSerialization.validate_serialized_dag(data)


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


class TestImportDefinition:
    @staticmethod
    def _import(importer, tmp_path):
        definition = FilesystemDagDefinition(tmp_path / "main.min.native")
        return importer.import_definition(definition, SimpleNamespace(name="testing", path=tmp_path))

    @mock.patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True)
    def test_returns_the_dags_the_runtime_serialized(self, mock_run, importer, tmp_path):
        mock_run.return_value = DagFileParsingResult(
            fileloc=str(tmp_path / "main.min.native"),
            serialized_dags=[LazyDeserializedDAG(data=_get_payload("conformance_minimal"))],
            import_errors={"main.min.native": "one failed", "main.ts": "two failed"},
        )

        result = self._import(importer, tmp_path)

        [dag] = result.dags
        assert isinstance(dag, SerializedDAG)
        assert (dag.dag_id, dag.task_ids) == ("conformance_minimal", ["solo"])
        assert [(e.source_reference, e.message) for e in result.errors] == [
            (str(tmp_path / "main.min.native"), "one failed"),
            (str(tmp_path / "main.min.native"), "main.ts: two failed"),
        ]
        mock_run.assert_called_once_with(
            path=tmp_path / "main.min.native",
            bundle_path=tmp_path,
            bundle_name="testing",
            dag_file_rel_path="main.min.native",
            logger=mock.ANY,
        )

    @pytest.mark.parametrize("data", _load_payloads())
    @mock.patch.object(LangSDKDagFileProcessorProcess, "run", autospec=True)
    def test_imports_every_recorded_runtime_payload(self, mock_run, importer, tmp_path, data):
        mock_run.return_value = DagFileParsingResult(
            fileloc=data["dag"]["fileloc"], serialized_dags=[LazyDeserializedDAG(data=data)]
        )

        [dag] = self._import(importer, tmp_path).dags

        assert {t.task_id: t.downstream_task_ids for t in dag.tasks} == {
            t["__var"]["task_id"]: set(t["__var"].get("downstream_task_ids", []))
            for t in data["dag"]["tasks"]
        }


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
