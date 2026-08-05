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

import pytest
from ci.prek import lang_sdk_compat_matrix as matrix, update_java_sdk_readme_matrix as hook


def _doc() -> dict:
    def entry(supported: bool) -> dict:
        return {"supported": supported, "since": "3.3" if supported else None, "note": ""}

    return {
        "sdk": "java",
        "supervisor_schema_version": "2026-06-16",
        "min_airflow_version": "3.3",
        "states": {state: entry(True) for state, _ in matrix.STATE_DIMENSIONS},
        "capabilities": {cap.name: entry(True) for cap in matrix.CAPABILITY_DIMENSIONS},
    }


class TestMain:
    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        """Point the hook at a temp capabilities.json plus temp README and Dokka module doc."""
        capabilities_json = tmp_path / "capabilities.json"
        capabilities_json.write_text(json.dumps(_doc()))
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
            [{"id": "java", "capabilities_json": capabilities_json, "readme": readme}],
        )
        monkeypatch.setattr(hook, "DOKKA_MODULE_DOC", module_doc)
        return readme, module_doc

    def test_generates_both_targets_then_is_idempotent(self, wired):
        assert hook.main() == 1
        for target in wired:
            content = target.read_text()
            assert "| Dimension | Tier | Supported | Since | Notes |" in content
            assert matrix.SUPPORTED_MARK in content
            assert content.startswith("intro\n") and content.endswith("outro\n")

        assert hook.main() == 0
