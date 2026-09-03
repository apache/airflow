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
"""Tests for PythonDagImporter."""

from __future__ import annotations

import sys

from airflow.sdk.importers import (
    FileDagDefinition,
    PythonDagImporter,
)


class TestPythonDagImporter:
    """Test the PythonDagImporter implementation."""

    def test_import_successful_dag(self, tmp_path):
        dag_file = tmp_path / "sample_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('test_dag_1')\n")

        importer = PythonDagImporter()
        definition = FileDagDefinition(path=dag_file)
        result = importer.import_definition(definition, bundle_name="test_bundle", bundle_path=tmp_path)

        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "test_dag_1"
        assert result.dags[0].bundle_name == "test_bundle"
        assert result.dags[0].relative_fileloc == "sample_dag.py"
        assert len(result.errors) == 0

    def test_import_syntax_error_cleans_sys_modules(self, tmp_path):
        dag_file = tmp_path / "bad_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndef broken(\n")

        importer = PythonDagImporter()
        result = importer.import_definition(FileDagDefinition(path=dag_file))

        assert len(result.errors) == 1
        assert result.errors[0].error_type == "import"
        assert not any("bad_dag" in m for m in sys.modules)

    def test_skip_non_dag_file_in_safe_mode(self, tmp_path):
        helper_file = tmp_path / "helper.py"
        helper_file.write_text("def util(): return 42\n")

        importer = PythonDagImporter()
        definition = FileDagDefinition(path=helper_file)
        result = importer.import_definition(definition, safe_mode=True)

        assert len(result.dags) == 0
        assert len(result.errors) == 0
        assert result.skipped_definitions == [definition]

    def test_list_dag_definitions(self, tmp_path):
        dag_file = tmp_path / "sample_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\ndag = DAG('test_dag_1')\n")
        (tmp_path / "notes.txt").write_text("hello")

        importer = PythonDagImporter()
        defs = list(importer.list_dag_definitions("test_bundle", tmp_path))
        assert len(defs) == 1
        assert defs[0].path == dag_file

    def test_get_source_code(self, tmp_path):
        dag_file = tmp_path / "source_dag.py"
        content = "# My DAG\nfrom airflow.sdk import DAG\n"
        dag_file.write_text(content)

        src = PythonDagImporter().get_source_code(FileDagDefinition(path=dag_file))
        assert src.language == "python"
        assert src.source_code == content

    def test_file_dag_definition_freshness_token(self, tmp_path):
        dag_file = tmp_path / "fresh_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\n")
        stat = dag_file.stat()
        assert FileDagDefinition(path=dag_file).freshness_token == f"{stat.st_mtime_ns}-{stat.st_size}"
