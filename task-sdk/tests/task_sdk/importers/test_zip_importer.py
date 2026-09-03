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

from airflow.sdk.importers import (
    FileDagDefinition,
    ZipFileDagDefinition,
    ZipImporter,
)


class TestZipImporter:
    """Test the ZipImporter composite implementation."""

    def test_import_zip_archive_with_dags(self, tmp_path):
        zip_path = tmp_path / "sample_dags.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("dag_a.py", "from airflow.sdk import DAG\ndag = DAG('zip_dag_a')\n")
            z.writestr("helper.py", "def util(): return 1\n")

        result = ZipImporter().import_definition(
            FileDagDefinition(path=zip_path), bundle_name="test_bundle", bundle_path=tmp_path
        )
        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "zip_dag_a"
        assert result.dags[0].bundle_name == "test_bundle"
        assert len(result.errors) == 0

    def test_zipslip_traversal_skipped(self, tmp_path):
        zip_path = tmp_path / "malicious.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("../evil_dag.py", "from airflow.sdk import DAG\ndag = DAG('evil_dag')\n")

        result = ZipImporter().import_definition(FileDagDefinition(path=zip_path))
        assert len(result.dags) == 0
        assert not (tmp_path.parent / "evil_dag.py").exists()

    def test_corrupted_zip_file(self, tmp_path):
        bad_zip = tmp_path / "corrupted.zip"
        bad_zip.write_bytes(b"not a real zip")

        result = ZipImporter().import_definition(FileDagDefinition(path=bad_zip))
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
