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
from ci.prek import lang_sdk_compat_matrix as matrix, update_lang_sdk_central_matrix as hook


def _entry(supported: bool, since: str | None = None, note: str = "") -> dict:
    return {"supported": supported, "since": since, "note": note}


def _doc(sdk_id: str = "go", **overrides) -> dict:
    doc = {
        "sdk": sdk_id,
        "supervisor_schema_version": "2026-06-16",
        "min_airflow_version": "3.3",
        "states": {state: _entry(True, since="3.3") for state, _ in matrix.STATE_DIMENSIONS},
        "capabilities": {cap.name: _entry(True, since="3.3") for cap in matrix.CAPABILITY_DIMENSIONS},
    }
    doc.update(overrides)
    return doc


class TestLoadSdkCapabilities:
    def test_loads_published_manifest_and_preserves_unpublished_sdk(self, tmp_path):
        go_yaml = tmp_path / "go.yaml"
        go_yaml.write_text(yaml.safe_dump(_doc()))
        registry = [
            {"id": "go", "capabilities_yaml": go_yaml, "readme": tmp_path / "go.md"},
            {"id": "ts", "capabilities_yaml": tmp_path / "ts.yaml", "readme": tmp_path / "ts.md"},
        ]

        loaded = hook.load_sdk_capabilities(registry)

        assert loaded == [
            hook.SdkCapabilities("Go", _doc()),
            hook.SdkCapabilities("TypeScript", None),
        ]


class TestRenderCentralTable:
    def test_lists_registered_sdks_and_absent_marks(self):
        sdk_capabilities = [
            hook.SdkCapabilities("Go", None),
            hook.SdkCapabilities("Java", None),
            hook.SdkCapabilities("TypeScript", None),
        ]

        rendered = "".join(hook.render_central_table(sdk_capabilities))

        assert ".. list-table:: Language SDK compatibility matrix" in rendered
        for display_name in ("Go", "Java", "TypeScript"):
            assert f"     - {display_name}\n" in rendered
        assert f"     - {matrix.SUPPORTED_MARK}\n" not in rendered
        assert f"   * - Min. Airflow version\n     - {hook.ABSENT_MARK}\n" in rendered

    def test_renders_manifest_marks_and_metadata(self):
        doc = _doc()
        doc["capabilities"][matrix.NATIVE_DAG_GATE] = _entry(False)
        for capability in matrix.CAPABILITY_DIMENSIONS:
            if capability.gated:
                doc["capabilities"][capability.name] = _entry(False)
        matrix.validate_capabilities(doc, source="test")

        rendered = "".join(hook.render_central_table([hook.SdkCapabilities("Go", doc)]))

        assert "``success`` (MUST)" in rendered
        assert "``retry-policy`` (MAY)" in rendered
        assert f"   * - ``success`` (MUST)\n     - {matrix.SUPPORTED_MARK}\n" in rendered
        assert f"   * - ``native-dag-authoring`` (SHOULD)\n     - {matrix.UNSUPPORTED_MARK}\n" in rendered
        assert f"   * - ``task-args`` (MUST †)\n     - {matrix.NA_MARK}\n" in rendered
        assert f"   * - ``object-store`` (MAY †)\n     - {matrix.NA_MARK}\n" in rendered
        assert "3.3" in rendered and "2026-06-16" in rendered


class TestMain:
    @pytest.fixture
    def wired(self, tmp_path, monkeypatch):
        """Point the hook at a temporary index and registry."""
        index = tmp_path / "index.rst"
        index.write_text(f"intro\n\n{hook.CENTRAL_MATRIX_HEADER}\n{hook.CENTRAL_MATRIX_FOOTER}\n\noutro\n")
        capabilities_yaml = tmp_path / "capabilities.yaml"
        capabilities_yaml.write_text(yaml.safe_dump(_doc()))
        monkeypatch.setattr(hook, "INDEX_RST", index)
        monkeypatch.setattr(
            matrix,
            "LANG_SDKS",
            [{"id": "go", "capabilities_yaml": capabilities_yaml, "readme": tmp_path / "README.md"}],
        )
        return index

    def test_generates_then_is_idempotent(self, wired):
        assert hook.main() == 1
        content = wired.read_text()
        assert ".. list-table:: Language SDK compatibility matrix" in content
        assert content.startswith("intro\n") and content.endswith("outro\n")
        assert matrix.SUPPORTED_MARK in content

        assert hook.main() == 0
