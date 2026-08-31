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
from types import SimpleNamespace

import pytest

from airflow.sdk.importers import (
    FileDagDefinition,
    PythonDagImporter,
)


@pytest.fixture
def mock_bundle(tmp_path):
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(name="test_bundle", path=bundle_dir)


class TestPythonDagImporter:
    """Test the PythonDagImporter implementation."""

    def test_import_successful_dag(self, mock_bundle):
        dag_file = mock_bundle.path / "sample_dag.py"
        dag_file.write_text(
            "from airflow.sdk import DAG\n"
            "dag = DAG('test_dag_1')\n"
        )

        importer = PythonDagImporter()
        definition = FileDagDefinition(path=dag_file)
        result = importer.import_definition(definition=definition, bundle=mock_bundle)

        assert len(result.dags) == 1
        assert result.dags[0].dag_id == "test_dag_1"
        assert result.dags[0].bundle_name == "test_bundle"
        assert result.dags[0].relative_fileloc == "sample_dag.py"
        assert len(result.errors) == 0

    def test_import_syntax_error_cleans_sys_modules(self, mock_bundle):
        dag_file = mock_bundle.path / "bad_dag.py"
        dag_file.write_text(
            "from airflow.sdk import DAG\n"
            "def broken(\n"
        )

        importer = PythonDagImporter()
        definition = FileDagDefinition(path=dag_file)
        result = importer.import_definition(definition=definition, bundle=mock_bundle)

        assert len(result.errors) == 1
        assert result.errors[0].error_type == "import"
        matching_mods = [m for m in sys.modules if "bad_dag" in m]
        assert len(matching_mods) == 0

    def test_skip_non_dag_file_in_safe_mode(self, mock_bundle):
        helper_file = mock_bundle.path / "helper.py"
        helper_file.write_text("def util(): return 42\n")

        importer = PythonDagImporter()
        definition = FileDagDefinition(path=helper_file)
        result = importer.import_definition(definition=definition, bundle=mock_bundle, safe_mode=True)

        assert len(result.dags) == 0
        assert len(result.errors) == 0
        assert len(result.skipped_definitions) == 1
        assert result.skipped_definitions[0] == definition

    def test_get_source_code(self, mock_bundle):
        dag_file = mock_bundle.path / "source_dag.py"
        content = "# My DAG\nfrom airflow.sdk import DAG\n"
        dag_file.write_text(content)

        importer = PythonDagImporter()
        definition = FileDagDefinition(path=dag_file)
        src = importer.get_source_code(definition)

        assert src.language == "python"
        assert src.source_code == content

    def test_file_dag_definition_freshness_token(self, mock_bundle):
        dag_file = mock_bundle.path / "fresh_dag.py"
        dag_file.write_text("from airflow.sdk import DAG\n")
        definition = FileDagDefinition(path=dag_file)

        stat = dag_file.stat()
        expected_token = f"{stat.st_mtime_ns}-{stat.st_size}"
        assert definition.freshness_token == expected_token
