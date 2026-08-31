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
from types import SimpleNamespace

import pytest

from airflow.sdk.importers import (
    FileDagDefinition,
    ZipFileDagDefinition,
    ZipImporter,
)


@pytest.fixture
def mock_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(name="test_bundle", path=bundle_dir)


class TestZipImporter:
    """Test the ZipImporter composite implementation."""

    def test_import_zip_archive_with_dags(self, mock_bundle):
        zip_path = mock_bundle.path / "sample_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr(
                "dag_a.py",
                "from airflow.sdk import DAG\n"
                "dag = DAG('zip_dag_a')\n",
            )
            z.writestr("helper.py", "def util(): return 1\n")

        importer = ZipImporter()
        definition = FileDagDefinition(path=zip_path)
        result = importer.import_definition(definition=definition, bundle=mock_bundle)

        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "zip_dag_a"
        assert result.dags[0].bundle_name == "test_bundle"
        assert len(result.errors) == 0

    def test_zipslip_traversal_skipped(self, mock_bundle):
        zip_path = mock_bundle.path / "malicious.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr(
                "../evil_dag.py",
                "from airflow.sdk import DAG\n"
                "dag = DAG('evil_dag')\n",
            )

        importer = ZipImporter()
        definition = FileDagDefinition(path=zip_path)
        result = importer.import_definition(definition=definition, bundle=mock_bundle)

        assert len(result.dags) == 0
        assert not (mock_bundle.path.parent / "evil_dag.py").exists()

    def test_get_source_code_archive_and_member(self, mock_bundle):
        zip_path = mock_bundle.path / "source_dags.zip"
        dag_content = "from airflow.sdk import DAG\ndag = DAG('src_dag')\n"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("my_dag.py", dag_content)

        importer = ZipImporter()
        # Source code via zip file definition
        archive_def = FileDagDefinition(path=zip_path)
        src_archive = importer.get_source_code(archive_def)
        assert src_archive.language == "python"
        assert src_archive.source_code == dag_content

        # Source code via nested ZipFileDagDefinition
        member_def = ZipFileDagDefinition(zip_path=zip_path, file_path="my_dag.py")
        src_member = importer.get_source_code(member_def)
        assert src_member.language == "python"
        assert src_member.source_code == dag_content

    def test_zip_dag_definition_freshness_token(self, mock_bundle):
        zip_path = mock_bundle.path / "fresh_bundle.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag.py", "from airflow.sdk import DAG\n")

        member_def = ZipFileDagDefinition(zip_path=zip_path, file_path="dag.py")
        stat = zip_path.stat()
        expected_token = f"{stat.st_mtime_ns}-{stat.st_size}-dag.py"
        assert member_def.freshness_token == expected_token
