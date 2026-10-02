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
import zipfile
from unittest.mock import patch

import pytest
from task_sdk.coordinators.java._jar_test_utils import SCHEMA_VERSION, make_jar

from airflow.sdk.coordinators.java import JavaCoordinator
from airflow.sdk.coordinators.java._dag_importer import JavaDagImporter, _find_source_entry
from airflow.sdk.importers import FilesystemDagDefinition, get_importer_registry, reset_importer_registry

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)

MAIN_CLASS = "com.example.Dags"
SOURCES_INDEX = "META-INF/airflow/sources.json"
ENTRYPOINT = "com/example/Dags.java"
REPORTS = "com/example/dags/Reports.kt"
LONG_MAIN_CLASS = "org.apache.airflow.example.nativedag.generated.VeryLongNativeDagBundleBuilder"


def _importer(main_class: str = "") -> JavaDagImporter:
    return JavaDagImporter(coordinator=JavaCoordinator(main_class=main_class))


def _definition(path) -> FilesystemDagDefinition:
    return FilesystemDagDefinition(path=path)


class _Bundle:
    def __init__(self, path):
        self.path = path
        self.name = "dags-folder"


class TestJavaDagImporter:
    def test_claims_jar_files(self):
        importer = _importer()

        assert importer.supported_extensions == [".jar"]
        assert importer.can_handle("dags/app.jar")
        assert not importer.can_handle("dags/app.zip")

    def test_coordinator_hands_out_an_importer_bound_to_it(self):
        coordinator = JavaCoordinator()

        importer = coordinator.get_dag_importer()

        assert isinstance(importer, JavaDagImporter)
        assert importer.coordinator is coordinator

    @pytest.mark.parametrize(
        ("attributes", "pin", "expected"),
        [
            pytest.param(
                {"Main-Class": MAIN_CLASS, "Airflow-Supervisor-Schema-Version": SCHEMA_VERSION},
                "",
                True,
                id="fat",
            ),
            pytest.param({"Main-Class": MAIN_CLASS}, "", True, id="thin-app"),
            pytest.param({"Airflow-Supervisor-Schema-Version": SCHEMA_VERSION}, "", False, id="dependency"),
            pytest.param(None, "", False, id="no-manifest"),
            pytest.param({"Main-Class": LONG_MAIN_CLASS}, LONG_MAIN_CLASS, True, id="folded-main-class"),
            pytest.param({"Main-Class": MAIN_CLASS}, MAIN_CLASS, True, id="pin-match"),
            pytest.param({"Main-Class": MAIN_CLASS}, "com.example.Other", False, id="pin-mismatch"),
        ],
    )
    def test_might_contain_dag(self, tmp_path, attributes, pin, expected):
        jar = make_jar(tmp_path / "app.jar", attributes=attributes, entries={"a.class": b""})

        assert _importer(pin).might_contain_dag(_definition(jar), safe_mode=True) is expected

    def test_keeps_a_jar_it_cannot_read(self, tmp_path):
        jar = tmp_path / "partial.jar"
        jar.write_bytes(b"PK\x03\x04 truncated")

        assert _importer().might_contain_dag(_definition(jar), safe_mode=True) is True

    @pytest.mark.parametrize("safe_mode", [True, False])
    def test_lists_only_executable_jars(self, tmp_path, safe_mode):
        app = make_jar(tmp_path / "app.jar", attributes={"Main-Class": MAIN_CLASS})
        (tmp_path / "libs").mkdir()
        make_jar(tmp_path / "libs" / "dep.jar", entries={"dep/Dep.class": b""})
        make_jar(
            tmp_path / "libs" / "airflow-sdk.jar",
            attributes={"Airflow-Supervisor-Schema-Version": SCHEMA_VERSION},
        )

        listed = list(_importer().list_dag_definitions(_Bundle(tmp_path), safe_mode=safe_mode))

        assert [d.path for d in listed] == [app]


def _sources(index, files):
    """JAR entries for a source index and the files it packs."""
    raw_index = index if isinstance(index, str) else json.dumps(index)
    return {SOURCES_INDEX: raw_index, **{f"META-INF/airflow/sources/{p}": c for p, c in files.items()}}


class TestGetSourceCode:
    def _jar(self, tmp_path, *, index=None, files=None, attributes=None):
        attributes = {
            "Main-Class": MAIN_CLASS,
            "Airflow-Java-SDK-Sources": SOURCES_INDEX,
            **(attributes or {}),
        }
        entries = _sources(index, files or {}) if index is not None else {}
        return make_jar(tmp_path / "app.jar", attributes=attributes, entries=entries)

    def test_returns_the_entrypoint_source(self, tmp_path):
        source = "public class Dags {}\n"
        jar = self._jar(
            tmp_path,
            index={"entrypoint_path": ENTRYPOINT, "dag_source_paths": {"reports": REPORTS}},
            files={ENTRYPOINT: source, REPORTS: "class Reports"},
            attributes={"Main-Class": LONG_MAIN_CLASS},
        )
        assert b"\r\n " in zipfile.ZipFile(jar).read("META-INF/MANIFEST.MF")

        result = _importer().get_source_code(_definition(jar))

        assert (result.source_code, result.language) == (source, "java")

    @pytest.mark.parametrize(
        ("dag_id", "expected"),
        [
            pytest.param("reports", REPORTS, id="mapped"),
            pytest.param("orders", ENTRYPOINT, id="unmapped"),
            pytest.param(None, ENTRYPOINT, id="no-dag-id"),
        ],
    )
    def test_maps_a_dag_to_its_file_with_the_entrypoint_as_fallback(self, tmp_path, dag_id, expected):
        jar = self._jar(
            tmp_path,
            index={"entrypoint_path": ENTRYPOINT, "dag_source_paths": {"reports": REPORTS}},
            files={ENTRYPOINT: "class Dags", REPORTS: "class Reports"},
        )

        with zipfile.ZipFile(jar) as zf:
            assert _find_source_entry(zf, dag_id).filename == f"META-INF/airflow/sources/{expected}"

    @pytest.mark.parametrize(
        ("attributes", "index", "files"),
        [
            pytest.param(
                {"Airflow-Java-SDK-Sources": ""},
                {"entrypoint_path": ENTRYPOINT},
                {ENTRYPOINT: "x"},
                id="no-attribute",
            ),
            pytest.param({}, None, {}, id="no-index"),
            pytest.param({}, "{not json", {}, id="bad-index"),
            pytest.param({}, ["not", "an", "object"], {}, id="index-not-an-object"),
            pytest.param({}, {"dag_source_paths": {}}, {}, id="no-entrypoint"),
            pytest.param({}, {"entrypoint_path": ENTRYPOINT}, {}, id="no-entry"),
            pytest.param({}, {"entrypoint_path": ENTRYPOINT}, {ENTRYPOINT: ""}, id="empty-entry"),
        ],
    )
    def test_placeholder_when_no_source_is_embedded(self, tmp_path, attributes, index, files):
        jar = self._jar(tmp_path, index=index, files=files, attributes=attributes)

        result = _importer().get_source_code(_definition(jar))

        assert "embeds no Dag source" in result.source_code
        assert result.language == "java"

    def test_placeholder_for_a_jar_without_manifest(self, tmp_path):
        jar = make_jar(
            tmp_path / "lib.jar", entries=_sources({"entrypoint_path": ENTRYPOINT}, {ENTRYPOINT: "x"})
        )

        assert "embeds no Dag source" in _importer().get_source_code(_definition(jar)).source_code

    def test_placeholder_when_the_source_file_is_too_large(self, tmp_path):
        jar = self._jar(tmp_path, index={"entrypoint_path": ENTRYPOINT}, files={ENTRYPOINT: "x" * 200})

        with patch("airflow.sdk.coordinators.java._dag_importer._MAX_SOURCE_BYTES", 100):
            result = _importer().get_source_code(_definition(jar))

        assert "is not shown" in result.source_code

    def test_replaces_invalid_utf8(self, tmp_path):
        jar = self._jar(tmp_path, index={"entrypoint_path": ENTRYPOINT}, files={ENTRYPOINT: b"a\xffb"})

        assert _importer().get_source_code(_definition(jar)).source_code == "a\ufffdb"

    def test_bad_zip_raises(self, tmp_path):
        jar = tmp_path / "broken.jar"
        jar.write_bytes(b"not a zip")

        with pytest.raises(zipfile.BadZipFile):
            _importer().get_source_code(_definition(jar))


class TestRegistry:
    @pytest.fixture(autouse=True)
    def _clean_registry(self):
        reset_importer_registry()
        yield
        reset_importer_registry()

    def _coordinators(self, kwargs: dict) -> dict[tuple[str, str], str]:
        spec = {"java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator", "kwargs": kwargs}}
        return {("sdk", "coordinators"): json.dumps(spec)}

    def test_a_bundle_backed_coordinator_registers_its_importer(self):
        with conf_vars(self._coordinators({})):
            importer = get_importer_registry("dags-folder").get_importer("dags/app.jar")

        assert isinstance(importer, JavaDagImporter)

    def test_a_named_bundle_coordinator_registers_only_in_its_bundle(self):
        bundles = [
            {"name": name, "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}}
            for name in ("dags-folder", "java-jars")
        ]
        with conf_vars(
            {
                **self._coordinators({"dag_bundle_name": "java-jars"}),
                ("dag_processor", "dag_bundle_config_list"): json.dumps(bundles),
            }
        ):
            in_named = get_importer_registry("java-jars").get_importer("java-jars/app.jar")
            in_other = get_importer_registry("dags-folder").get_importer("dags/app.jar")

        assert isinstance(in_named, JavaDagImporter)
        assert in_other is None
