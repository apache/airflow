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

from pathlib import Path

from common_prek_utils import coordinator_distribution_dirs, coordinator_ids


def test_coordinator_distribution_dirs_returns_empty_when_root_is_missing(tmp_path: Path):
    assert coordinator_distribution_dirs(tmp_path / "missing") == []


def test_coordinator_distribution_dirs_discovers_pyproject_directories(tmp_path: Path):
    java = tmp_path / "java"
    python = tmp_path / "python"
    docs = tmp_path / "docs"
    java.mkdir()
    python.mkdir()
    docs.mkdir()
    (java / "pyproject.toml").touch()
    (python / "pyproject.toml").touch()

    assert coordinator_distribution_dirs(tmp_path) == [java, python]
    assert coordinator_ids(tmp_path) == ["java", "python"]
