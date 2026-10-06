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
import subprocess
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from ci.prek import sync_java_sdk_supervisor_schema as sync

if TYPE_CHECKING:
    from pathlib import Path

CONFIGURED_VERSION = "2026-06-16"


@pytest.fixture
def vendored_schema(tmp_path, monkeypatch) -> Path:
    """Point the hook at a temp gradle.properties and a vendored schema path."""
    gradle_properties = tmp_path / "gradle.properties"
    gradle_properties.write_text(f"airflowSupervisorSchemaVersion={CONFIGURED_VERSION}\n")
    vendored = tmp_path / "schema.json"
    monkeypatch.setattr(sync, "GRADLE_PROPERTIES", gradle_properties)
    monkeypatch.setattr(sync, "SCHEMA_FILE", vendored)
    return vendored


@mock.patch.object(sync.subprocess, "run", autospec=True)
class TestMain:
    def test_skips_gradle_when_the_vendored_schema_is_at_the_configured_version(
        self, mock_run, vendored_schema, capsys
    ):
        vendored_schema.write_text(json.dumps({"api_version": CONFIGURED_VERSION}))

        assert sync.main() == 0

        assert "up-to-date" in capsys.readouterr().out
        mock_run.assert_not_called()

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(None, id="missing"),
            pytest.param(json.dumps({"api_version": "2025-01-01"}), id="other-version"),
            pytest.param('{"api_version": ', id="truncated"),
        ],
    )
    def test_hands_over_to_gradle_when_the_vendored_schema_is_not_at_the_configured_version(
        self, mock_run, vendored_schema, content
    ):
        if content is not None:
            vendored_schema.write_text(content)
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)

        assert sync.main() == 0

        mock_run.assert_called_once_with(
            [str(sync.JAVA_SDK_DIR / "gradlew"), "-p", str(sync.JAVA_SDK_DIR), ":sdk:syncSupervisorSchema"],
            check=False,
        )

    def test_returns_the_exit_code_of_gradle(self, mock_run, vendored_schema):
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=3)

        assert sync.main() == 3
