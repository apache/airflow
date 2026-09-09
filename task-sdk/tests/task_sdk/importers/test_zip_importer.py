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
"""Tests for ZipImporter."""

from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers import (
    AbstractDagImporter,
    DagDefinition,
    DagImportResult,
    DagSourceCode,
    FileDagDefinition,
    PythonDagImporter,
    ZipFileDagDefinition,
    ZipImporter,
)


class CustomInternalNonExtensionImporter(AbstractDagImporter):
    """An internal importer that routes archive members by filename pattern."""

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        path_str = str(getattr(definition, "file_path", definition))
        return "workflow" in path_str

    def list_dag_definitions(self, bundle, **kwargs):
        return iter([])

    def import_definition(self, definition, bundle=None, **kwargs):
        from airflow.sdk import DAG

        return DagImportResult(dags=[DAG("workflow_dag")])

    def get_source_code(self, definition):
        return DagSourceCode(source_code="", language="text")


class TestZipImporter:
    """Test the ZipImporter composite implementation."""

    @pytest.fixture
    def mock_bundle(self, tmp_path):
        return SimpleNamespace(name="test_bundle", path=tmp_path)

    def test_list_dag_definitions(self, mock_bundle):
        zip_path = mock_bundle.path / "sample.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.py", "from airflow.sdk import DAG\n")

        definitions = list(ZipImporter().list_dag_definitions(mock_bundle))
        assert len(definitions) == 1
        assert definitions[0].path == zip_path

    def test_import_zip_archive_with_dags(self, mock_bundle):
        zip_path = mock_bundle.path / "sample_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag_a.py", "from airflow.sdk import DAG\ndag = DAG('zip_dag_a')\n")
            z.writestr("helper.py", "def util(): return 1\n")

        result = ZipImporter().import_definition(FileDagDefinition(path=zip_path), bundle=mock_bundle)
        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "zip_dag_a"
        assert result.dags[0].bundle_name == "test_bundle"
        assert len(result.errors) == 0

    def test_zipslip_traversal_and_metadata_skipped(self, mock_bundle):
        zip_path = mock_bundle.path / "malicious.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("subfolder/", "")
            z.writestr("__MACOSX/._dag.py", "apple double metadata")
            z.writestr("../evil_dag.py", "from airflow.sdk import DAG\ndag = DAG('evil_dag')\n")
            z.writestr("valid_dag.py", "from airflow.sdk import DAG\ndag = DAG('valid_dag')\n")

        result = ZipImporter().import_definition(FileDagDefinition(path=zip_path), bundle=mock_bundle)
        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "valid_dag"
        assert not (mock_bundle.path.parent / "evil_dag.py").exists()

    def test_corrupted_zip_file(self, mock_bundle):
        bad_zip = mock_bundle.path / "corrupted.zip"
        bad_zip.write_bytes(b"not a real zip")

        result = ZipImporter().import_definition(FileDagDefinition(path=bad_zip), bundle=mock_bundle)
        assert len(result.errors) == 1
        assert result.errors[0].error_type == "zip_read_error"

    def test_get_source_code_archive_and_member(self, tmp_path):
        zip_path = tmp_path / "source_dags.zip"
        dag_content = "from airflow.sdk import DAG\ndag = DAG('src_dag')\n"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("my_dag.py", dag_content)

        importer = ZipImporter()
        src_archive = importer.get_source_code(FileDagDefinition(path=zip_path))
        assert src_archive.language == "python"
        assert src_archive.source_code == dag_content

        src_member = importer.get_source_code(ZipFileDagDefinition(zip_path=zip_path, file_path="my_dag.py"))
        assert src_member.language == "python"
        assert src_member.source_code == dag_content

    def test_zip_dag_definition_freshness_token(self, tmp_path):
        zip_path = tmp_path / "fresh_bundle.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.py", "from airflow.sdk import DAG\n")

        member_def = ZipFileDagDefinition(zip_path=zip_path, file_path="dag.py")
        stat = zip_path.stat()
        assert member_def.freshness_token == f"{stat.st_mtime_ns}-{stat.st_size}-dag.py"

    def test_zip_importer_internal_importers_from_list_of_specs(self, tmp_path, mock_bundle):
        importer = ZipImporter(
            internal_importers=[
                {
                    "classpath": "airflow.sdk.importers.python_importer.PythonDagImporter",
                    "extensions": [".custom_py"],
                }
            ]
        )
        assert ".custom_py" in importer._internal_extension_importers
        assert ".py" not in importer._internal_extension_importers

        zip_path = mock_bundle.path / "custom_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.custom_py", "from airflow.sdk import DAG\ndag = DAG('custom_zip_dag')\n")

        res = importer.import_definition(FileDagDefinition(path=zip_path), bundle=mock_bundle)
        assert len(res.dags) == 1
        assert res.dags[0].dag_id == "custom_zip_dag"

    def test_zip_importer_internal_importers_from_dict(self):
        importer = ZipImporter(
            internal_importers={
                ".py": PythonDagImporter(),
                ".alt": {
                    "classpath": "airflow.sdk.importers.python_importer.PythonDagImporter",
                    "extensions": [".alt"],
                },
            }
        )
        assert ".py" in importer._internal_extension_importers
        assert ".alt" in importer._internal_extension_importers

    @pytest.mark.parametrize(
        ("config", "match"),
        [
            ("not_list_or_dict", "must be a list or dictionary"),
            ([{"extensions": [".py"]}], "Missing required 'classpath'"),
            ([{"classpath": "invalid.path"}], "Failed to load DAG importer"),
            (
                [{"classpath": "builtins.dict"}],
                r"must inherit from AbstractDagImporter",
            ),
            ({".py": {"kwargs": {}}}, "Missing required 'classpath'"),
            ({".py": "invalid"}, "expected AbstractDagImporter or dictionary"),
        ],
    )
    def test_zip_importer_invalid_configurations(self, config, match):
        with pytest.raises(AirflowConfigException, match=match):
            ZipImporter(internal_importers=config)

    def test_zip_importer_custom_extensions(self):
        importer = ZipImporter(extensions=[".bundle", ".zip"])
        assert importer.can_handle("test.bundle")
        assert importer.can_handle("test.zip")
        assert set(importer.supported_extensions) == {".bundle", ".zip"}

    def test_zip_importer_internal_importers_non_extension(self, mock_bundle):
        """ZipImporter can route archive members to non-extension internal importers."""
        importer = ZipImporter(
            internal_importers=[
                {
                    "classpath": f"{__name__}.CustomInternalNonExtensionImporter",
                }
            ]
        )
        zip_path = mock_bundle.path / "workflow_archive.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("my_workflow_file", "steps:\n  - run: echo hello\n")

        res = importer.import_definition(FileDagDefinition(path=zip_path), bundle=mock_bundle)
        assert len(res.dags) == 1
        assert res.dags[0].dag_id == "workflow_dag"
