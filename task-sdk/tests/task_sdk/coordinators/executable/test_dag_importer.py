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

import pytest
from task_sdk.coordinators.executable._bundle_test_utils import ENTRYPOINT_PATH, write_bundle

from airflow.sdk.coordinators._dag_importer import find_claiming_importer
from airflow.sdk.coordinators.executable._dag_importer import ExecutableDagImporter
from airflow.sdk.coordinators.executable.coordinator import ExecutableCoordinator, _digest_cache
from airflow.sdk.importers import (
    DagSourceCode,
    FilesystemDagDefinition,
    get_importer_registry,
    reset_importer_registry,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

EXECUTABLE_COORDINATOR = "airflow.sdk.coordinators.executable.ExecutableCoordinator"
ORDERS = b'package main\n\nfunc orders() { dag.New("orders") }\n'
REPORTS = b'package dags\n\nfunc reports() { dag.New("reports") }\n'
REPORTS_PATH = "example/bundle/dags/reports.go"
COORDINATORS = {("sdk", "coordinators"): json.dumps({"go": {"classpath": EXECUTABLE_COORDINATOR}})}


@pytest.fixture(autouse=True)
def _clean_state():
    _digest_cache.clear()
    reset_importer_registry()
    yield
    reset_importer_registry()


@pytest.fixture
def importer() -> ExecutableDagImporter:
    return ExecutableDagImporter(bundle_name="dags-folder")


def _bundle(root: SimpleNamespace | Path) -> SimpleNamespace:
    return SimpleNamespace(name="dags-folder", path=root)


class TestCanHandle:
    @pytest.mark.parametrize("name", ["go-bundle", "go-bundle.bin", "go-bundle.exe"])
    def test_claims_a_bundle_whatever_its_name(self, importer, tmp_path, name):
        path = write_bundle(tmp_path / name, "orders")

        assert importer.can_handle(path) is True
        assert importer.can_handle(str(path)) is True
        assert importer.can_handle(FilesystemDagDefinition(path)) is True

    @pytest.mark.parametrize(
        "content",
        [b"", b"short", b"x" * 100, b"AFBNDL01 and then more bytes"],
        ids=["empty", "shorter-than-magic", "plain", "magic-not-at-the-end"],
    )
    def test_ignores_a_file_that_does_not_end_with_the_magic(self, importer, tmp_path, content):
        path = tmp_path / "plain.bin"
        path.write_bytes(content)

        assert importer.can_handle(path) is False

    def test_ignores_a_python_file(self, importer, tmp_path):
        path = tmp_path / "dag.py"
        path.write_text("from airflow.sdk import dag\n")

        assert importer.can_handle(path) is False

    @pytest.mark.parametrize("name", ["missing", "."])
    def test_ignores_a_path_it_cannot_read(self, importer, tmp_path, name):
        assert importer.can_handle(tmp_path / name) is False

    def test_declares_no_extension(self, importer):
        assert importer.artifact_suffix == ""
        assert importer.supported_extensions == []


class TestListDagDefinitions:
    def test_lists_every_bundle_under_the_root(self, importer, tmp_path):
        top = write_bundle(tmp_path / "top", "orders")
        (tmp_path / "team").mkdir()
        nested = write_bundle(tmp_path / "team" / "nested.bin", "reports")
        (tmp_path / "README.md").write_text("docs")
        (tmp_path / "dag.py").write_text("x = 1\n")

        definitions = list(importer.list_dag_definitions(_bundle(tmp_path)))

        assert sorted(d.path for d in definitions) == sorted([top, nested])

    def test_honors_airflowignore(self, importer, tmp_path):
        kept = write_bundle(tmp_path / "kept", "orders")
        write_bundle(tmp_path / "handlers-only", "reports")
        (tmp_path / ".airflowignore").write_text("handlers-only\n")

        definitions = list(importer.list_dag_definitions(_bundle(tmp_path)))

        assert [d.path for d in definitions] == [kept]

    def test_a_single_file_root_scopes_the_listing_to_it(self, importer, tmp_path):
        target = write_bundle(tmp_path / "target", "orders")
        write_bundle(tmp_path / "other", "reports")

        assert [d.path for d in importer.list_dag_definitions(_bundle(target))] == [target]

    def test_a_single_file_root_that_is_not_a_bundle_lists_nothing(self, importer, tmp_path):
        plain = tmp_path / "plain"
        plain.write_bytes(b"not a bundle")

        assert list(importer.list_dag_definitions(_bundle(plain))) == []


class TestMightContainDag:
    @pytest.mark.parametrize("safe_mode", [True, False])
    def test_keeps_every_bundle(self, importer, tmp_path, safe_mode):
        handlers_only = write_bundle(tmp_path / "handlers", "orders", dag_source_paths={}, sources={})

        assert importer.might_contain_dag(FilesystemDagDefinition(handlers_only), safe_mode) is True

    def test_keeps_an_unreadable_file(self, importer, tmp_path):
        assert importer.might_contain_dag(FilesystemDagDefinition(tmp_path / "gone"), True) is True


class TestGetSourceCode:
    @pytest.fixture
    def bundle(self, tmp_path) -> FilesystemDagDefinition:
        path = write_bundle(
            tmp_path / "bundle",
            "orders",
            "reports",
            sources={ENTRYPOINT_PATH: ORDERS, REPORTS_PATH: REPORTS},
            dag_source_paths={"orders": ENTRYPOINT_PATH, "reports": REPORTS_PATH},
        )
        return FilesystemDagDefinition(path)

    def test_returns_each_dags_own_file(self, importer, bundle):
        assert importer.get_source_code(bundle, "orders") == DagSourceCode(ORDERS.decode(), "go")
        assert importer.get_source_code(bundle, "reports") == DagSourceCode(REPORTS.decode(), "go")

    def test_returns_the_entrypoint_without_a_dag_id(self, importer, bundle):
        assert importer.get_source_code(bundle) == DagSourceCode(ORDERS.decode(), "go")

    def test_returns_the_entrypoint_for_an_unmapped_dag(self, importer, bundle):
        assert importer.get_source_code(bundle, "dynamic") == DagSourceCode(ORDERS.decode(), "go")

    def test_returns_a_notice_when_the_bundle_embeds_no_source(self, importer, tmp_path):
        path = write_bundle(tmp_path / "bundle", "orders", omit_sources=True)

        source_code = importer.get_source_code(FilesystemDagDefinition(path), "orders")

        assert source_code == DagSourceCode(
            "// Source code is not available: the bundle embeds no source.\n", "go"
        )

    def test_falls_back_to_text_without_a_language(self, importer, tmp_path):
        path = write_bundle(tmp_path / "bundle", "orders", language=None)

        assert importer.get_source_code(FilesystemDagDefinition(path)).language == "text"

    def test_raises_for_an_invalid_bundle(self, importer, tmp_path):
        path = write_bundle(tmp_path / "bundle", "orders", entrypoint_path="missing.go")

        with pytest.raises(ValueError, match="not one of its sources"):
            importer.get_source_code(FilesystemDagDefinition(path))


class TestRegistry:
    @pytest.mark.parametrize("name", ["go-bundle", "go-bundle.bin"])
    def test_routes_a_bundle_to_the_importer(self, tmp_path, name):
        path = write_bundle(tmp_path / name, "orders")
        with conf_vars(COORDINATORS):
            routed = get_importer_registry("dags-folder").get_importer(path)
            claiming = find_claiming_importer(path, "dags-folder")

        assert isinstance(routed, ExecutableDagImporter)
        assert isinstance(claiming, ExecutableDagImporter)
        assert claiming.bundle_name == "dags-folder"

    def test_leaves_other_files_to_the_python_importers(self, tmp_path):
        python_file = tmp_path / "dag.py"
        python_file.write_text("x = 1\n")
        plain = tmp_path / "notes.txt"
        plain.write_text("notes")
        with conf_vars(COORDINATORS):
            assert find_claiming_importer(python_file, "dags-folder") is None
            assert find_claiming_importer(plain, "dags-folder") is None

    def test_a_python_file_ending_with_the_magic_still_belongs_to_the_python_importer(self, tmp_path):
        path = tmp_path / "dag.py"
        path.write_bytes(b"from airflow.sdk import DAG\n" + b"AFBNDL01")
        with conf_vars(COORDINATORS):
            assert find_claiming_importer(path, "dags-folder") is None
            listed = [
                (type(importer).__name__, item.path)
                for importer, item in get_importer_registry("dags-folder").list_dag_definitions(
                    _bundle(tmp_path)
                )
            ]

        assert [name for name, _ in listed] == ["PythonDagImporter"]

    def test_registry_listing_keeps_suffixless_and_bin_bundles(self, tmp_path):
        suffixless = write_bundle(tmp_path / "go-bundle", "orders")
        with_suffix = write_bundle(tmp_path / "other.bin", "reports")
        with conf_vars(COORDINATORS):
            listed = [
                item.path
                for importer, item in get_importer_registry("dags-folder").list_dag_definitions(
                    _bundle(tmp_path)
                )
                if isinstance(importer, ExecutableDagImporter)
            ]

        assert sorted(listed) == sorted([suffixless, with_suffix])

    def test_registers_nothing_without_an_executable_coordinator(self, tmp_path):
        path = write_bundle(tmp_path / "go-bundle", "orders")
        with conf_vars({("sdk", "coordinators"): "{}"}):
            assert find_claiming_importer(path, "dags-folder") is None

    def test_parsing_coordinator_is_the_executable_coordinator(self, importer):
        with conf_vars(COORDINATORS):
            assert isinstance(importer.get_parsing_coordinator(), ExecutableCoordinator)
