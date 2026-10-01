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
from ci.prek import lang_sdk_compat_matrix as matrix, update_java_sdk_readme_matrix as hook

SCHEMA_VERSION = "2026-06-16"


def _doc() -> dict:
    def entry(supported: bool) -> dict:
        return {"supported": supported, "since": "3.3" if supported else None, "note": ""}

    return {
        "sdk": "java",
        "supervisor_schema_version": SCHEMA_VERSION,
        "min_airflow_version": "3.3",
        "states": {state: entry(True) for state, _ in matrix.STATE_DIMENSIONS},
        "capabilities": {cap.name: entry(True) for cap in matrix.CAPABILITY_DIMENSIONS},
    }


class TestMain:
    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        """Point the hook at a temp capabilities.yaml plus temp README and Dokka module doc."""
        capabilities_yaml = tmp_path / "capabilities.yaml"
        capabilities_yaml.write_text(yaml.safe_dump(_doc()))
        gradle_properties = tmp_path / "gradle.properties"
        gradle_properties.write_text(
            f"projectVersion=1.0.0-SNAPSHOT\n{hook.SCHEMA_VERSION_PROPERTY}={SCHEMA_VERSION}\n"
        )
        targets = []
        for name in ("README.md", "module.md"):
            target = tmp_path / name
            target.write_text(
                f"intro\n\n{matrix.README_MATRIX_HEADER}\n{matrix.README_MATRIX_FOOTER}\n\noutro\n"
            )
            targets.append(target)
        readme, module_doc = targets
        monkeypatch.setattr(
            hook,
            "LANG_SDKS",
            [{"id": "java", "capabilities_yaml": capabilities_yaml, "readme": readme}],
        )
        monkeypatch.setattr(hook, "DOKKA_MODULE_DOC", module_doc)
        monkeypatch.setattr(hook, "GRADLE_PROPERTIES", gradle_properties)
        return readme, module_doc

    def test_generates_both_targets_then_is_idempotent(self, wired):
        assert hook.main() == 1
        for target in wired:
            content = target.read_text()
            assert "| Dimension | Tier | Supported | Since | Notes |" in content
            assert matrix.SUPPORTED_MARK in content
            assert content.startswith("intro\n") and content.endswith("outro\n")

        assert hook.main() == 0

    def test_schema_version_disagreeing_with_gradle_fails_without_writing(self, wired):
        hook.GRADLE_PROPERTIES.write_text(f"{hook.SCHEMA_VERSION_PROPERTY}=2020-01-01\n")
        assert hook.main() == 1
        for target in wired:
            assert matrix.README_MATRIX_HEADER + "\n" + matrix.README_MATRIX_FOOTER in target.read_text()

    def test_schema_version_matches_gradle_properties(self):
        """The committed manifest agrees with the version that stamps the JAR."""
        sdk = next(entry for entry in matrix.LANG_SDKS if entry["id"] == hook.SDK_ID)
        doc = matrix.load_capabilities(sdk["capabilities_yaml"], expected_sdk=hook.SDK_ID)
        assert doc["supervisor_schema_version"] == hook.read_gradle_schema_version()
