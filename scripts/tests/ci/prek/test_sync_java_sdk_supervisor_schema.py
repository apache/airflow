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

import json
import os
import subprocess
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from ci.prek import sync_java_sdk_supervisor_schema as sync

if TYPE_CHECKING:
    from pathlib import Path

CONFIGURED_VERSION = "2026-06-16"


def _write_schema(path: Path, api_version: str, **body) -> None:
    path.write_text(json.dumps({"api_version": api_version, **body}))


@pytest.fixture
def schemas(tmp_path, monkeypatch):
    """Point the hook at a temp gradle.properties, vendored schema and Task SDK snapshot."""
    gradle_properties = tmp_path / "gradle.properties"
    gradle_properties.write_text(f"airflowSupervisorSchemaVersion={CONFIGURED_VERSION}\n")
    vendored = tmp_path / "java-sdk" / "schema.json"
    snapshot = tmp_path / "task-sdk" / "schema.json"
    vendored.parent.mkdir()
    snapshot.parent.mkdir()
    monkeypatch.setattr(sync, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(sync, "GRADLE_PROPERTIES", gradle_properties)
    monkeypatch.setattr(sync, "SCHEMA_FILE", vendored)
    monkeypatch.setattr(sync, "MONOREPO_SCHEMA_FILE", snapshot)
    return vendored, snapshot


@mock.patch.object(sync.subprocess, "run", autospec=True)
class TestMain:
    def test_leaves_a_vendored_schema_that_matches_the_snapshot(self, mock_run, schemas, capsys):
        vendored, snapshot = schemas
        _write_schema(snapshot, CONFIGURED_VERSION)
        _write_schema(vendored, CONFIGURED_VERSION)
        os.utime(vendored, ns=(0, 0))

        assert sync.main() == 0

        assert "matches the Task SDK snapshot" in capsys.readouterr().out
        assert vendored.stat().st_mtime_ns == 0
        mock_run.assert_not_called()

    def test_copies_the_snapshot_over_a_differing_vendored_schema(self, mock_run, schemas):
        vendored, snapshot = schemas
        _write_schema(snapshot, CONFIGURED_VERSION, definitions={"New": {}})
        _write_schema(vendored, CONFIGURED_VERSION)

        assert sync.main() == 0

        assert vendored.read_bytes() == snapshot.read_bytes()
        mock_run.assert_not_called()

    def test_runs_gradle_when_the_snapshot_declares_another_version(self, mock_run, schemas):
        vendored, snapshot = schemas
        _write_schema(snapshot, "2026-01-01")
        _write_schema(vendored, "2025-01-01")
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        assert sync.main() == 0

        mock_run.assert_called_once_with(
            [str(sync.JAVA_SDK_DIR / "gradlew"), "-p", str(sync.JAVA_SDK_DIR), ":sdk:syncSupervisorSchema"],
            check=False,
        )
        assert json.loads(vendored.read_text()) == {"api_version": "2025-01-01"}
