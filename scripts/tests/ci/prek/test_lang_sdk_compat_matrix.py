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
from ci.prek import lang_sdk_compat_matrix as matrix


def _entry(supported: bool, since: str | None = None, note: str = "") -> dict:
    return {"supported": supported, "since": since, "note": note}


def _doc(sdk_id: str = "go", **overrides) -> dict:
    """A fully-populated, valid capabilities document for ``sdk_id``."""
    doc = {
        "sdk": sdk_id,
        "supervisor_schema_version": "2026-06-16",
        "min_airflow_version": "3.3",
        "states": {state: _entry(True, since="3.3") for state, _ in matrix.STATE_DIMENSIONS},
        "capabilities": {cap.name: _entry(True, since="3.3") for cap in matrix.CAPABILITY_DIMENSIONS},
    }
    doc.update(overrides)
    return doc


class TestValidateCapabilities:
    def test_valid_doc_passes(self):
        matrix.validate_capabilities(_doc(), source="test")

    def test_non_dict_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="must be a mapping"):
            matrix.validate_capabilities([], source="test")

    @pytest.mark.parametrize(
        "key", ["sdk", "supervisor_schema_version", "min_airflow_version", "states", "capabilities"]
    )
    def test_missing_required_key_raises(self, key):
        doc = _doc()
        del doc[key]
        with pytest.raises(matrix.CapabilitiesError, match="missing required keys"):
            matrix.validate_capabilities(doc, source="test")

    def test_unknown_top_level_key_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="unknown top-level keys: capability"):
            matrix.validate_capabilities(_doc(capability={}), source="test")

    def test_unknown_entry_key_raises(self):
        doc = _doc()
        doc["capabilities"]["branching"] = {"supported": True, "supported_since": "3.3"}
        with pytest.raises(matrix.CapabilitiesError, match="branching has unknown keys: supported_since"):
            matrix.validate_capabilities(doc, source="test")

    def test_unknown_sdk_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="unknown sdk"):
            matrix.validate_capabilities(_doc(sdk_id="rust"), source="test")

    def test_missing_state_raises(self):
        doc = _doc()
        del doc["states"]["deferred"]
        with pytest.raises(matrix.CapabilitiesError, match="states keys mismatch"):
            matrix.validate_capabilities(doc, source="test")

    def test_unknown_capability_raises(self):
        doc = _doc()
        doc["capabilities"]["telepathy"] = _entry(True)
        with pytest.raises(matrix.CapabilitiesError, match="capabilities keys mismatch"):
            matrix.validate_capabilities(doc, source="test")

    def test_non_boolean_supported_raises(self):
        doc = _doc()
        doc["states"]["success"] = {"supported": "true"}
        with pytest.raises(matrix.CapabilitiesError, match="boolean 'supported'"):
            matrix.validate_capabilities(doc, source="test")

    def test_sdk_mismatch_against_expected_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="belongs to 'go'"):
            matrix.validate_capabilities(_doc(sdk_id="java"), source="test", expected_sdk="go")

    def test_expected_sdk_match_passes(self):
        matrix.validate_capabilities(_doc(sdk_id="go"), source="test", expected_sdk="go")

    def test_non_string_note_raises(self):
        doc = _doc()
        doc["states"]["success"] = {"supported": True, "since": "3.3", "note": 123}
        with pytest.raises(matrix.CapabilitiesError, match="note must be a string"):
            matrix.validate_capabilities(doc, source="test")

    def test_non_string_since_raises(self):
        doc = _doc()
        doc["capabilities"]["xcom-read-write"] = {"supported": True, "since": 3, "note": ""}
        with pytest.raises(matrix.CapabilitiesError, match="since must be a string or null"):
            matrix.validate_capabilities(doc, source="test")

    def test_non_string_schema_version_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="supervisor_schema_version must be a string"):
            matrix.validate_capabilities(_doc(supervisor_schema_version=123), source="test")

    def test_unsupported_entry_carrying_since_raises(self):
        doc = _doc()
        doc["capabilities"]["branching"] = _entry(False, since="3.3")
        with pytest.raises(matrix.CapabilitiesError, match="not supported but carries since"):
            matrix.validate_capabilities(doc, source="test")

    def test_supported_gated_capability_without_native_dag_authoring_raises(self):
        doc = _doc()
        doc["capabilities"][matrix.NATIVE_DAG_GATE] = _entry(False)
        with pytest.raises(
            matrix.CapabilitiesError,
            match="gated capabilities cannot be supported.*branching",
        ):
            matrix.validate_capabilities(doc, source="test")

    def test_supported_entry_without_since_passes(self):
        doc = _doc()
        doc["states"]["success"] = _entry(True, since=None)
        matrix.validate_capabilities(doc, source="test")

    def test_entry_omitting_optional_keys_passes(self):
        doc = _doc()
        doc["states"]["success"] = {"supported": True}
        matrix.validate_capabilities(doc, source="test")


class TestLoadCapabilities:
    def test_reads_yaml(self, tmp_path):
        path = tmp_path / "capabilities.yaml"
        path.write_text(yaml.safe_dump(_doc()))
        assert matrix.load_capabilities(path, expected_sdk="go") == _doc()

    def test_committed_manifests_are_valid(self):
        """Every manifest an SDK has actually committed passes validation."""
        declared = [sdk for sdk in matrix.LANG_SDKS if sdk["capabilities_yaml"].exists()]
        assert declared, "expected at least one Language SDK to declare its capabilities"
        for sdk in declared:
            matrix.load_capabilities(sdk["capabilities_yaml"], expected_sdk=sdk["id"])


class TestRenderMarkdownTable:
    def test_retry_policy_is_an_optional_runtime_capability(self):
        retry_policy = next(cap for cap in matrix.CAPABILITY_DIMENSIONS if cap.name == "retry-policy")
        assert retry_policy == matrix.Capability("retry-policy", "MAY", "runtime", False)

    def test_rows_cover_every_dimension(self):
        rendered = "".join(matrix.render_markdown_table(_doc()))
        for state, tier in matrix.STATE_DIMENSIONS:
            assert f"| state: `{state}` | {tier} |" in rendered
        for cap in matrix.CAPABILITY_DIMENSIONS:
            assert f"| capability: `{cap.name}` | {matrix._tier_label(cap)} |" in rendered
        assert "supervisor schema: 2026-06-16" in rendered

    def test_supported_and_unsupported_marks(self):
        doc = _doc()
        doc["states"]["deferred"] = _entry(False, note="no triggerer bridge")
        rendered = "".join(matrix.render_markdown_table(doc))
        assert f"| state: `success` | MUST | {matrix.SUPPORTED_MARK} | 3.3 |" in rendered
        assert (
            f"| state: `deferred` | MAY | {matrix.UNSUPPORTED_MARK} | {matrix.NO_VERSION_MARK} |" in rendered
        )

    def test_gated_capabilities_are_na_without_the_native_dag_gate(self):
        doc = _doc()
        doc["capabilities"][matrix.NATIVE_DAG_GATE] = _entry(False)
        rendered = "".join(matrix.render_markdown_table(doc))
        assert f"| capability: `{matrix.NATIVE_DAG_GATE}` | SHOULD | {matrix.UNSUPPORTED_MARK} |" in rendered
        for cap in matrix.CAPABILITY_DIMENSIONS:
            if cap.gated:
                assert f"| capability: `{cap.name}` | {cap.tier} † | {matrix.NA_MARK} |" in rendered

    def test_gated_capabilities_resolve_to_their_own_mark_once_the_gate_is_supported(self):
        doc = _doc()
        doc["capabilities"]["branching"] = _entry(False, note="no branch construct")
        rendered = "".join(matrix.render_markdown_table(doc))
        gated = [cap for cap in matrix.CAPABILITY_DIMENSIONS if cap.gated]
        assert gated, "expected at least one gated capability"
        # With the gate supported, a gated capability reports its real support, never n/a.
        for cap in gated:
            if cap.name != "branching":
                assert (
                    f"| capability: `{cap.name}` | {cap.tier} † | {matrix.SUPPORTED_MARK} | 3.3 |" in rendered
                )
        assert f"| capability: `branching` | SHOULD † | {matrix.UNSUPPORTED_MARK} |" in rendered
        assert matrix.NA_MARK not in rendered.replace(matrix.LEGEND, "")

    def test_pipe_in_note_is_escaped(self):
        doc = _doc()
        doc["capabilities"]["xcom-read-write"] = _entry(True, since="3.3", note="read | write")
        rendered = "".join(matrix.render_markdown_table(doc))
        assert "read \\| write" in rendered
