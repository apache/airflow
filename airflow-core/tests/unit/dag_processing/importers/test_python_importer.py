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

import zipfile

from airflow.dag_processing.importers.python_importer import PythonDagImporter


class TestPythonDagImporterListDagFiles:
    def test_list_dag_files_ignores_non_zip_zip_format_files(self, tmp_path):
        """Files that are zip-format (PK magic bytes) but don't have a .zip suffix should be ignored.

        Regression test for https://github.com/apache/airflow/issues/71125: a .jar, .pptx,
        .docx, etc. dropped into the DAGs folder happens to share the zip container format,
        but must not be treated as a DAG zip bundle just because zipfile.is_zipfile() is True.
        """
        # A real DAG file.
        (tmp_path / "dag.py").write_text("from airflow.sdk import DAG\ndag = DAG(dag_id='x')\n")

        # A real, legitimately named zip DAG bundle.
        zip_bundle = tmp_path / "bundle.zip"
        with zipfile.ZipFile(zip_bundle, "w") as zf:
            zf.writestr("inner_dag.py", "from airflow.sdk import DAG\ndag = DAG(dag_id='y')\n")

        # A non-.zip file that nonetheless has the zip container format (e.g. a .jar/.pptx/.docx
        # would look like this too), so zipfile.is_zipfile() returns True for it.
        fake_jar = tmp_path / "dependency.jar"
        with zipfile.ZipFile(fake_jar, "w") as zf:
            zf.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        assert zipfile.is_zipfile(fake_jar)  # sanity check: it really does sniff as a zip

        importer = PythonDagImporter()
        detected_files = set(importer.list_dag_files(tmp_path, safe_mode=False))

        assert str(tmp_path / "dag.py") in detected_files
        assert str(zip_bundle) in detected_files
        assert str(fake_jar) not in detected_files