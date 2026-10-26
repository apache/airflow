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


class TestLoadSdkCapabilities:
    def test_loads_published_manifest_and_preserves_unpublished_sdk(self, tmp_path, capabilities_doc):
        go_yaml = tmp_path / "go.yaml"
        go_yaml.write_text(yaml.safe_dump(capabilities_doc()))
        registry = [
            {
                "id": "go",
                "display_name": "Go",
                "capabilities_yaml": go_yaml,
                "readme": tmp_path / "go.md",
            },
            {
                "id": "ts",
                "display_name": "TypeScript",
                "capabilities_yaml": tmp_path / "ts.yaml",
                "readme": tmp_path / "ts.md",
            },
        ]

        loaded = hook.load_sdk_capabilities(registry)

        assert loaded == [
            hook.SdkCapabilities("Go", matrix.parse_capabilities(capabilities_doc(), source="test")),
            hook.SdkCapabilities("TypeScript", None),
        ]


class TestRenderCentralTable:
    def test_absent_mark_is_distinct_from_the_other_marks(self):
        """ "Not published" must not read as "not applicable" or as a missing version."""
        assert hook.ABSENT_MARK == "n/p"
        assert hook.ABSENT_MARK not in (
            matrix.NA_MARK,
            matrix.NO_VERSION_MARK,
            matrix.UNSUPPORTED_MARK,
        )

    def test_lists_registered_sdks_and_absent_marks(self):
        sdk_capabilities = [
            hook.SdkCapabilities("Go", None),
            hook.SdkCapabilities("Java", None),
            hook.SdkCapabilities("TypeScript", None),
        ]

        rendered = "".join(hook.render_central_table(sdk_capabilities))

        for title in (
            "Language SDK versions",
            "Common to both authoring modes",
            "Dag definition",
            "Python Dag with stub TaskHandler",
        ):
            assert f".. list-table:: {title}\n" in rendered
        for display_name in ("Go", "Java", "TypeScript"):
            assert f"     - {display_name}\n" in rendered
        assert f"     - {matrix.SUPPORTED_MARK}\n" not in rendered
        assert f"   * - Min. Airflow version\n     - {hook.ABSENT_MARK}\n" in rendered

    def test_renders_manifest_marks_and_metadata(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"][matrix.DAG_DEFINITION_GATE] = capabilities_entry(False)
        for capability in matrix.CAPABILITY_DIMENSIONS:
            if capability.gated and capability.name in matrix.SHARED_CAPABILITY_NAMES:
                doc["capabilities"][capability.name][matrix.DAG_DEFINITION_MODE] = capabilities_entry(False)
            elif capability.gated:
                doc["capabilities"][capability.name] = capabilities_entry(False)
        rendered = "".join(
            hook.render_central_table(
                [hook.SdkCapabilities("Go", matrix.parse_capabilities(doc, source="test"))]
            )
        )

        assert f"   * - ``success`` (MUST)\n     - {matrix.SUPPORTED_MARK}\n" in rendered
        assert f"   * - ``object-store`` (MAY)\n     - {matrix.SUPPORTED_MARK}\n" in rendered
        assert f"   * - ``dag-definition-authoring`` (SHOULD)\n     - {matrix.UNSUPPORTED_MARK}\n" in rendered
        assert f"   * - ``task-args`` (MUST †)\n     - {matrix.NA_MARK}\n" in rendered
        assert f"   * - ``mapped-tasks`` (MAY †)\n     - {matrix.NA_MARK}\n" in rendered
        assert f"   * - ``mapped-tasks`` (MAY)\n     - {matrix.SUPPORTED_MARK}\n" in rendered
        assert "   * - Min. Airflow version\n     - 3.3\n" in rendered
        assert "2026-06-16" in rendered

    def test_supported_mark_shows_sdk_version_when_present(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["xcom-read-write"] = capabilities_entry(True, since="3.4.0", sdk_version="1.0.0")

        rendered = "".join(
            hook.render_central_table(
                [hook.SdkCapabilities("Java", matrix.parse_capabilities(doc, source="test"))]
            )
        )

        assert f"   * - ``xcom-read-write`` (MUST)\n     - {matrix.SUPPORTED_MARK} 1.0.0\n" in rendered


class TestMain:
    @pytest.fixture
    def wired(self, tmp_path, monkeypatch, capabilities_doc):
        """Point the hook at a temporary index and registry."""
        index = tmp_path / "index.rst"
        index.write_text(f"intro\n\n{hook.CENTRAL_MATRIX_HEADER}\n{hook.CENTRAL_MATRIX_FOOTER}\n\noutro\n")
        capabilities_yaml = tmp_path / "capabilities.yaml"
        capabilities_yaml.write_text(yaml.safe_dump(capabilities_doc()))
        monkeypatch.setattr(hook, "INDEX_RST", index)
        monkeypatch.setattr(
            matrix,
            "LANG_SDKS",
            [
                {
                    "id": "go",
                    "display_name": "Go",
                    "capabilities_yaml": capabilities_yaml,
                    "readme": tmp_path / "README.md",
                }
            ],
        )
        return index

    def test_generates_then_is_idempotent(self, wired):
        assert hook.main() == 1
        content = wired.read_text()
        assert ".. list-table:: Dag definition" in content
        assert content.startswith("intro\n") and content.endswith("outro\n")
        assert matrix.SUPPORTED_MARK in content

        assert hook.main() == 0
