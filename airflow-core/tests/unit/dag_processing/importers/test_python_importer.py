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

from types import ModuleType

from airflow.dag_processing.importers import DagImportResult
from airflow.dag_processing.importers.python_importer import PythonDagImporter
from airflow.sdk import DAG


def mock_module(name: str, file: str, **kwargs) -> ModuleType:
    mod = ModuleType(name)
    mod.__file__ = file
    for k, v in kwargs.items():
        setattr(mod, k, v)
    return mod


class TestPythonDagImporter:
    def test_process_modules(self):
        mods = [
            mock_module("mod1", "bundle/dag1/file1", dag1=DAG(dag_id="hello")),
            mock_module("mod2", "bundle/dag2/file2", foo=DAG(dag_id="foo")),
        ]
        bundle_path = "bundle"
        result = DagImportResult(file_path="path")

        PythonDagImporter._process_modules(mods, bundle_path, result)

        assert result.file_path == "path"
        assert len(result.dags) == 2

        # Sort dags for testing
        result.dags.sort(key=lambda x: x.dag_id)

        assert result.dags[0].dag_id == "foo"
        assert result.dags[0].fileloc == "bundle/dag2/file2"
        assert result.dags[0].relative_fileloc == "dag2/file2"

        assert result.dags[1].dag_id == "hello"
        assert result.dags[1].fileloc == "bundle/dag1/file1"
        assert result.dags[1].relative_fileloc == "dag1/file1"
