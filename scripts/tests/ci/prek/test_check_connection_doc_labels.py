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

from check_connection_doc_labels import collect_files


def test_collect_files_skips_volatile_dependency_directories(tmp_path):
    source_file = tmp_path / "provider" / "docs" / "connection.rst"
    source_file.parent.mkdir(parents=True)
    source_file.touch()

    node_modules_file = tmp_path / "ui" / "node_modules" / "package" / "docs" / "connection.rst"
    node_modules_file.parent.mkdir(parents=True)
    node_modules_file.touch()

    pnpm_store_file = tmp_path / "ui" / ".pnpm-store" / "package" / "docs" / "connection.rst"
    pnpm_store_file.parent.mkdir(parents=True)
    pnpm_store_file.touch()

    assert collect_files(tmp_path, ".rst") == [source_file]


def test_collect_files_matches_suffix(tmp_path):
    python_file = tmp_path / "src" / "module.py"
    python_file.parent.mkdir(parents=True)
    python_file.touch()
    (python_file.parent / "module.pyi").touch()

    assert collect_files(tmp_path, ".py") == [python_file]
