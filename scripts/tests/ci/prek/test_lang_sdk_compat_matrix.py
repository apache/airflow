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


class TestParseCapabilities:
    def test_valid_doc_passes(self, capabilities_doc):
        matrix.parse_capabilities(capabilities_doc(), source="test")

    def test_non_dict_raises(self):
        with pytest.raises(matrix.CapabilitiesError, match="must be a mapping"):
            matrix.parse_capabilities([], source="test")

    @pytest.mark.parametrize(
        "key", ["sdk", "supervisor_schema_version", "min_airflow_version", "states", "capabilities"]
    )
    def test_missing_required_key_raises(self, key, capabilities_doc):
        doc = capabilities_doc()
        del doc[key]
        with pytest.raises(matrix.CapabilitiesError, match="missing required keys"):
            matrix.parse_capabilities(doc, source="test")

    def test_unknown_top_level_key_raises(self, capabilities_doc):
        with pytest.raises(matrix.CapabilitiesError, match="unknown top-level keys: capability"):
            matrix.parse_capabilities(capabilities_doc(capability={}), source="test")

    def test_unknown_entry_key_raises(self, capabilities_doc):
        doc = capabilities_doc()
        doc["capabilities"]["if-else-branching"] = {"supported": True, "supported_since": "3.3"}
        with pytest.raises(
            matrix.CapabilitiesError, match="if-else-branching has unknown keys: supported_since"
        ):
            matrix.parse_capabilities(doc, source="test")

    def test_unknown_sdk_raises(self, capabilities_doc):
        with pytest.raises(matrix.CapabilitiesError, match="unknown sdk"):
            matrix.parse_capabilities(capabilities_doc(sdk_id="rust"), source="test")

    def test_missing_state_raises(self, capabilities_doc):
        doc = capabilities_doc()
        del doc["states"]["deferred"]
        with pytest.raises(matrix.CapabilitiesError, match="states keys mismatch"):
            matrix.parse_capabilities(doc, source="test")

    def test_unknown_capability_raises(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["telepathy"] = capabilities_entry(True)
        with pytest.raises(matrix.CapabilitiesError, match="capabilities keys mismatch"):
            matrix.parse_capabilities(doc, source="test")

    def test_non_boolean_supported_raises(self, capabilities_doc):
        doc = capabilities_doc()
        doc["states"]["success"] = {"supported": "true"}
        with pytest.raises(matrix.CapabilitiesError, match="boolean 'supported'"):
            matrix.parse_capabilities(doc, source="test")

    def test_sdk_mismatch_against_expected_raises(self, capabilities_doc):
        with pytest.raises(matrix.CapabilitiesError, match="belongs to 'go'"):
            matrix.parse_capabilities(capabilities_doc(sdk_id="java"), source="test", expected_sdk="go")

    def test_expected_sdk_match_passes(self, capabilities_doc):
        matrix.parse_capabilities(capabilities_doc(sdk_id="go"), source="test", expected_sdk="go")

    def test_non_string_note_raises(self, capabilities_doc):
        doc = capabilities_doc()
        doc["states"]["success"] = {"supported": True, "since": "3.3", "note": 123}
        with pytest.raises(matrix.CapabilitiesError, match="note must be a string"):
            matrix.parse_capabilities(doc, source="test")

    def test_non_string_since_raises(self, capabilities_doc):
        doc = capabilities_doc()
        doc["capabilities"]["xcom-read-write"] = {"supported": True, "since": 3, "note": ""}
        with pytest.raises(matrix.CapabilitiesError, match="since must be a string or null"):
            matrix.parse_capabilities(doc, source="test")

    def test_non_string_schema_version_raises(self, capabilities_doc):
        with pytest.raises(matrix.CapabilitiesError, match="supervisor_schema_version must be a string"):
            matrix.parse_capabilities(capabilities_doc(supervisor_schema_version=123), source="test")

    def test_unsupported_entry_carrying_since_raises(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["if-else-branching"] = capabilities_entry(False, since="3.3")
        with pytest.raises(matrix.CapabilitiesError, match="not supported but carries since"):
            matrix.parse_capabilities(doc, source="test")

    def test_unsupported_entry_carrying_sdk_version_raises(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["if-else-branching"] = capabilities_entry(False, sdk_version="1.0.0")
        with pytest.raises(matrix.CapabilitiesError, match="not supported but carries sdk_version"):
            matrix.parse_capabilities(doc, source="test")

    def test_non_string_sdk_version_raises(self, capabilities_doc):
        doc = capabilities_doc()
        doc["capabilities"]["xcom-read-write"] = {"supported": True, "since": "3.3", "sdk_version": 3}
        with pytest.raises(matrix.CapabilitiesError, match="sdk_version must be a string or null"):
            matrix.parse_capabilities(doc, source="test")

    def test_supported_gated_capability_without_dag_definition_raises(
        self, capabilities_doc, capabilities_entry
    ):
        doc = capabilities_doc()
        doc["capabilities"][matrix.DAG_DEFINITION_GATE] = capabilities_entry(False)
        with pytest.raises(
            matrix.CapabilitiesError,
            match="gated capabilities cannot be supported.*if-else-branching",
        ):
            matrix.parse_capabilities(doc, source="test")

    def test_shared_capability_missing_a_mode_raises(self, capabilities_doc):
        doc = capabilities_doc()
        del doc["capabilities"]["mapped-tasks"][matrix.STUB_TASK_HANDLER_MODE]
        with pytest.raises(matrix.CapabilitiesError, match="capabilities.mapped-tasks keys mismatch"):
            matrix.parse_capabilities(doc, source="test")

    def test_shared_capability_declared_as_a_single_entry_raises(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["mapped-tasks"] = capabilities_entry(True, since="3.3")
        with pytest.raises(matrix.CapabilitiesError, match="capabilities.mapped-tasks keys mismatch"):
            matrix.parse_capabilities(doc, source="test")

    def test_invalid_shared_mode_entry_raises(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["asset-inlets-outlets"][matrix.STUB_TASK_HANDLER_MODE] = capabilities_entry(
            False, since="3.3"
        )
        with pytest.raises(
            matrix.CapabilitiesError,
            match="capabilities.asset-inlets-outlets.stub-task-handler is not supported",
        ):
            matrix.parse_capabilities(doc, source="test")

    def test_shared_capability_dag_definition_mode_is_gated(
        self, capabilities_doc_without_dag_definition, capabilities_entry
    ):
        # Everything gated is off except the Dag-definition side of mapped-tasks, so the
        # error names that one shared capability and nothing else.
        doc = capabilities_doc_without_dag_definition()
        doc["capabilities"]["mapped-tasks"][matrix.DAG_DEFINITION_MODE] = capabilities_entry(
            True, since="3.3"
        )
        with pytest.raises(matrix.CapabilitiesError, match="cannot be supported.*: mapped-tasks$"):
            matrix.parse_capabilities(doc, source="test")

    def test_shared_capability_stub_task_handler_mode_is_not_gated(
        self, capabilities_doc_without_dag_definition
    ):
        # Every gated dimension is off, but the stub-task-handler side of a shared capability
        # stays supported: the gate covers the Dag-definition side only.
        matrix.parse_capabilities(capabilities_doc_without_dag_definition(), source="test")

    def test_supported_entry_without_since_passes(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["states"]["success"] = capabilities_entry(True, since=None)
        matrix.parse_capabilities(doc, source="test")

    def test_entry_omitting_optional_keys_passes(self, capabilities_doc):
        doc = capabilities_doc()
        doc["states"]["success"] = {"supported": True}
        matrix.parse_capabilities(doc, source="test")


class TestLoadCapabilities:
    def test_reads_yaml(self, tmp_path, capabilities_doc):
        path = tmp_path / "capabilities.yaml"
        path.write_text(yaml.safe_dump(capabilities_doc()))
        assert matrix.load_capabilities(path, expected_sdk="go") == matrix.parse_capabilities(
            capabilities_doc(), source="test"
        )

    def test_committed_manifests_are_valid(self):
        """Every manifest an SDK has actually committed passes validation."""
        declared = [sdk for sdk in matrix.LANG_SDKS if sdk["capabilities_yaml"].exists()]
        assert declared, "expected at least one Language SDK to declare its capabilities"
        for sdk in declared:
            matrix.load_capabilities(sdk["capabilities_yaml"], expected_sdk=sdk["id"])


class TestEntryLookup:
    def test_shared_capability_resolves_per_authoring_mode(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["mapped-tasks"][matrix.STUB_TASK_HANDLER_MODE] = capabilities_entry(False)
        parsed = matrix.parse_capabilities(doc, source="test")
        parts = matrix.build_matrix_parts()
        dag_definition_row, stub_row = (
            next(row for row in part.groups[0].rows if row.name == "mapped-tasks")
            for part in (parts.dag_definition, parts.stub_task_handler)
        )

        assert parsed.get_entry(dag_definition_row)["supported"] is True
        assert parsed.get_entry(stub_row)["supported"] is False

    def test_unshared_capability_carries_no_mode(self, capabilities_doc):
        parsed = matrix.parse_capabilities(capabilities_doc(), source="test")
        row = next(
            row
            for row in matrix.build_matrix_parts().dag_definition.groups[0].rows
            if row.name == "task-group"
        )

        assert row.mode is None
        assert row.entry_key == ("capability", "task-group", None)
        assert parsed.get_entry(row) is parsed.entries["capability", "task-group", None]

    def test_state_and_capability_of_the_same_name_do_not_collide(self):
        state_row = matrix.MatrixRow("state", "removed", "MAY", False, None)
        capability_row = matrix.MatrixRow("capability", "removed", "MAY", False, None)

        assert state_row.entry_key != capability_row.entry_key


class TestRenderMarkdownTable:
    def test_retry_policy_is_an_optional_runtime_capability(self):
        retry_policy = next(cap for cap in matrix.CAPABILITY_DIMENSIONS if cap.name == "retry-policy")
        assert retry_policy == matrix.Capability("retry-policy", "MAY", "runtime", False)

    def test_rows_cover_every_dimension(self, capabilities_doc):
        rendered = "".join(
            matrix.render_markdown_table(matrix.parse_capabilities(capabilities_doc(), source="test"))
        )
        for state, tier in matrix.STATE_DIMENSIONS:
            assert f"| state: `{state}` | {tier} |" in rendered
        for cap in matrix.CAPABILITY_DIMENSIONS:
            assert f"| capability: `{cap.name}` |" in rendered
        assert "supervisor schema: 2026-06-16" in rendered

    def test_parts_split_dimensions_by_authoring_mode(self):
        common, dag_definition, stub_task_handler = matrix.build_matrix_parts()
        dag_definition_names = [row.name for row in dag_definition.groups[0].rows]
        stub_names = [row.name for row in stub_task_handler.groups[0].rows]

        assert [group.label for group in common.groups] == ["TaskInstance states", "Task runtime"]
        assert "object-store" in [row.name for row in common.groups[1].rows]
        assert dag_definition_names[0] == matrix.DAG_DEFINITION_GATE
        assert stub_names == [
            "mixed-lang-stub-target",
            "taskflow-binding",
            "mapped-tasks",
            "asset-inlets-outlets",
            "task-callbacks",
        ]
        assert set(dag_definition_names) >= matrix.SHARED_CAPABILITY_NAMES
        assert all(not row.gated for row in stub_task_handler.groups[0].rows)

    def test_shared_capability_renders_each_mode(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["mapped-tasks"][matrix.DAG_DEFINITION_MODE] = capabilities_entry(
            False, note="no expand API"
        )
        rendered = "".join(matrix.render_markdown_table(matrix.parse_capabilities(doc, source="test")))
        assert f"| capability: `mapped-tasks` | MAY † | {matrix.UNSUPPORTED_MARK} |" in rendered
        assert f"| capability: `mapped-tasks` | MAY | {matrix.SUPPORTED_MARK} | 3.3 |" in rendered

    def test_supported_and_unsupported_marks(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["states"]["deferred"] = capabilities_entry(False, note="no triggerer bridge")
        rendered = "".join(matrix.render_markdown_table(matrix.parse_capabilities(doc, source="test")))
        assert f"| state: `success` | MUST | {matrix.SUPPORTED_MARK} | 3.3 |" in rendered
        assert (
            f"| state: `deferred` | MAY | {matrix.UNSUPPORTED_MARK} | {matrix.NO_VERSION_MARK} |" in rendered
        )

    def test_gated_capabilities_are_na_without_the_dag_definition_gate(
        self, capabilities_doc_without_dag_definition
    ):
        doc = capabilities_doc_without_dag_definition()
        rendered = "".join(matrix.render_markdown_table(matrix.parse_capabilities(doc, source="test")))
        assert (
            f"| capability: `{matrix.DAG_DEFINITION_GATE}` | SHOULD | {matrix.UNSUPPORTED_MARK} |" in rendered
        )
        for row in matrix.build_matrix_parts().dag_definition.groups[0].rows:
            if row.gated:
                assert f"| capability: `{row.name}` | {row.tier} † | {matrix.NA_MARK} |" in rendered

    def test_gated_capabilities_resolve_to_their_own_mark_once_the_gate_is_supported(
        self, capabilities_doc, capabilities_entry
    ):
        doc = capabilities_doc()
        doc["capabilities"]["if-else-branching"] = capabilities_entry(False, note="no branch construct")
        rendered = "".join(matrix.render_markdown_table(matrix.parse_capabilities(doc, source="test")))
        gated = [row for row in matrix.build_matrix_parts().dag_definition.groups[0].rows if row.gated]
        assert gated, "expected at least one gated capability"
        # With the gate supported, a gated capability reports its real support, never n/a.
        for row in gated:
            if row.name != "if-else-branching":
                assert (
                    f"| capability: `{row.name}` | {row.tier} † | {matrix.SUPPORTED_MARK} | 3.3 |" in rendered
                )
        assert f"| capability: `if-else-branching` | SHOULD † | {matrix.UNSUPPORTED_MARK} |" in rendered
        assert matrix.NA_MARK not in rendered.replace(matrix.LEGEND, "")

    def test_sdk_version_renders_in_its_own_column(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["xcom-read-write"] = capabilities_entry(True, since="3.4.0", sdk_version="1.0.0")
        rendered = "".join(matrix.render_markdown_table(matrix.parse_capabilities(doc, source="test")))
        assert (
            f"| capability: `xcom-read-write` | MUST | {matrix.SUPPORTED_MARK} | 3.4.0 | 1.0.0 |" in rendered
        )

    def test_pipe_in_note_is_escaped(self, capabilities_doc, capabilities_entry):
        doc = capabilities_doc()
        doc["capabilities"]["xcom-read-write"] = capabilities_entry(True, since="3.3", note="read | write")
        rendered = "".join(matrix.render_markdown_table(matrix.parse_capabilities(doc, source="test")))
        assert "read \\| write" in rendered
