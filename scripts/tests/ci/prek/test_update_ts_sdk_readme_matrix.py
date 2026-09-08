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

import pytest
import yaml
from ci.prek import lang_sdk_compat_matrix as matrix, update_ts_sdk_readme_matrix as hook

SCHEMA_VERSION = "2026-06-16"


def _doc() -> dict:
    def entry(supported: bool) -> dict:
        return {"supported": supported, "since": "3.3" if supported else None, "note": ""}

    return {
        "sdk": "ts",
        "supervisor_schema_version": SCHEMA_VERSION,
        "min_airflow_version": "3.3",
        "states": {state: entry(True) for state, _ in matrix.STATE_DIMENSIONS},
        "capabilities": {cap.name: entry(True) for cap in matrix.CAPABILITY_DIMENSIONS},
    }


class TestMain:
    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        capabilities_yaml = tmp_path / "capabilities.yaml"
        capabilities_yaml.write_text(yaml.safe_dump(_doc()))
        readme = tmp_path / "README.md"
        readme.write_text(f"intro\n\n{matrix.README_MATRIX_HEADER}\n{matrix.README_MATRIX_FOOTER}\n\noutro\n")
        ts_supervisor = tmp_path / "supervisor.ts"
        ts_supervisor.write_text(f'export const SUPERVISOR_API_VERSION = "{SCHEMA_VERSION}" as const;\n')
        monkeypatch.setattr(
            hook,
            "LANG_SDKS",
            [{"id": "ts", "capabilities_yaml": capabilities_yaml, "readme": readme}],
        )
        monkeypatch.setattr(hook, "TS_SUPERVISOR", ts_supervisor)
        return readme

    def test_generates_target_then_is_idempotent(self, wired):
        assert hook.main() == 1
        content = wired.read_text()
        assert "| Dimension | Tier | Supported | Since | Notes |" in content
        assert matrix.SUPPORTED_MARK in content
        assert content.startswith("intro\n") and content.endswith("outro\n")

        assert hook.main() == 0

    def test_schema_version_disagreeing_with_ts_source_fails_without_writing(self, wired):
        hook.TS_SUPERVISOR.write_text('export const SUPERVISOR_API_VERSION = "2020-01-01" as const;\n')
        assert hook.main() == 1
        assert matrix.README_MATRIX_HEADER + "\n" + matrix.README_MATRIX_FOOTER in wired.read_text()

    def test_schema_version_matches_ts_source(self):
        sdk = next(entry for entry in matrix.LANG_SDKS if entry["id"] == hook.SDK_ID)
        doc = matrix.load_capabilities(sdk["capabilities_yaml"], expected_sdk=hook.SDK_ID)
        assert doc["supervisor_schema_version"] == hook.read_ts_schema_version()
