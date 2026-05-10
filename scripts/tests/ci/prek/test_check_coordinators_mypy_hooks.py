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

import check_coordinators_mypy_hooks


def test_missing_mypy_hook_distribution_ids_returns_missing_distribution(tmp_path: Path, monkeypatch):
    dist = tmp_path / "java"
    dist.mkdir()
    monkeypatch.setattr(check_coordinators_mypy_hooks, "coordinator_distribution_dirs", lambda: [dist])

    assert check_coordinators_mypy_hooks.missing_mypy_hook_distribution_ids() == ["java"]


def test_missing_mypy_hook_distribution_ids_accepts_matching_config(tmp_path: Path, monkeypatch):
    dist = tmp_path / "java"
    dist.mkdir()
    (dist / ".pre-commit-config.yaml").write_text("id: mypy-coordinators-java\n")
    monkeypatch.setattr(check_coordinators_mypy_hooks, "coordinator_distribution_dirs", lambda: [dist])

    assert check_coordinators_mypy_hooks.missing_mypy_hook_distribution_ids() == []
