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

import zipfile

import pytest

from airflow.dag_processing.importers import PythonDagImporter


class TestPythonDagImporterListDagFiles:
    @pytest.mark.parametrize(
        ("file_name", "expected_listed"),
        [
            pytest.param("dags.zip", True, id="zip"),
            pytest.param("DAGS.ZIP", True, id="zip-uppercase"),
            pytest.param("library.jar", False, id="jar"),
            pytest.param("package-1.0-py3-none-any.whl", False, id="wheel"),
            pytest.param("package-1.0-py3.12.egg", False, id="egg"),
            pytest.param("report.docx", False, id="docx"),
            pytest.param("dags.zip.bak", False, id="zip-backup"),
            pytest.param("archive", False, id="no-suffix"),
        ],
    )
    def test_only_zip_suffix_is_listed_as_archive(self, tmp_path, file_name, expected_listed):
        archive = tmp_path / file_name
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("my_dag.py", "from airflow.sdk import DAG\n")

        listed = list(PythonDagImporter().list_dag_files(tmp_path))

        assert listed == ([str(archive)] if expected_listed else [])
