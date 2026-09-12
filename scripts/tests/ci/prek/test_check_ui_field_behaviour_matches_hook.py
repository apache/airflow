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
from check_provider_conn_fields import (
    check_ui_field_behaviour_for_entry,
    normalize_behaviour_value,
)

YAML_PATH = "providers/my_provider/provider.yaml"
HOOK_CLASS = "my_provider.hooks.my_hook.MyHook"
CONN_TYPE = "my_conn_type"


def _entry(behaviour: dict | None) -> dict:
    entry: dict = {"hook-class-name": HOOK_CLASS, "connection-type": CONN_TYPE}
    if behaviour is not None:
        entry["ui-field-behaviour"] = behaviour
    return entry


def _hook(behaviour: dict | None):
    """Return a get_behaviour callable that always returns the given dict."""
    return lambda _hook_class_name: behaviour


def _raise(_hook_class_name: str) -> None:
    raise RuntimeError("boom")


class TestNormalizeBehaviourValue:
    @pytest.mark.parametrize(
        "left, right",
        [
            pytest.param("plain text", "plain text\n", id="trailing-newline"),
            pytest.param("  padded  ", "padded", id="surrounding-whitespace"),
            pytest.param('{"a": 1, "b": [2]}', '{\n  "a": 1,\n  "b": [\n    2\n  ]\n}\n', id="json-indent"),
            pytest.param('{"b": [2], "a": 1}', '{"a": 1, "b": [2]}', id="json-key-order"),
        ],
    )
    def test_insignificant_formatting_is_equal(self, left, right):
        assert normalize_behaviour_value(left) == normalize_behaviour_value(right)

    @pytest.mark.parametrize(
        "left, right",
        [
            pytest.param('{"a": 1}', '{"a": 2}', id="json-content"),
            pytest.param("host url", "host  url", id="inner-whitespace"),
        ],
    )
    def test_real_differences_stay_different(self, left, right):
        assert normalize_behaviour_value(left) != normalize_behaviour_value(right)


class TestCheckUiFieldBehaviourForEntry:
    def test_skips_when_hook_has_no_behaviour(self):
        assert check_ui_field_behaviour_for_entry(_entry(None), YAML_PATH, _hook(None)) == []

    def test_flags_missing_yaml_section(self):
        errors = check_ui_field_behaviour_for_entry(
            _entry(None), YAML_PATH, _hook({"hidden_fields": ["port"]})
        )
        assert len(errors) == 1
        assert "no ui-field-behaviour section" in errors[0]

    def test_matching_behaviour_passes(self):
        yaml_behaviour = {
            "hidden-fields": ["port", "schema"],
            "relabeling": {"host": "Server URL"},
            "placeholders": {"extra": '{"a": 1, "b": 2}'},
        }
        hook_behaviour = {
            "hidden_fields": ["schema", "port"],
            "relabeling": {"host": "Server URL"},
            "placeholders": {"extra": '{\n  "b": 2,\n  "a": 1\n}\n'},
        }
        assert (
            check_ui_field_behaviour_for_entry(_entry(yaml_behaviour), YAML_PATH, _hook(hook_behaviour)) == []
        )

    def test_flags_hidden_fields_drift(self):
        errors = check_ui_field_behaviour_for_entry(
            _entry({"hidden-fields": ["port"]}), YAML_PATH, _hook({"hidden_fields": ["port", "schema"]})
        )
        assert len(errors) == 1
        assert "hidden-fields differ" in errors[0]
        assert "schema" in errors[0]

    def test_flags_relabeling_drift(self):
        errors = check_ui_field_behaviour_for_entry(
            _entry({"relabeling": {"host": "Server URL"}}),
            YAML_PATH,
            _hook({"relabeling": {"host": "Server URL (optional)"}}),
        )
        assert len(errors) == 1
        assert "relabeling differ for: host" in errors[0]

    def test_flags_placeholder_content_drift(self):
        errors = check_ui_field_behaviour_for_entry(
            _entry({"placeholders": {"extra": '{"model": "old"}'}}),
            YAML_PATH,
            _hook({"placeholders": {"extra": '{"model": "new"}', "login": "user"}}),
        )
        assert len(errors) == 1
        assert "placeholders differ for: extra, login" in errors[0]

    def test_converts_unexpected_exception_to_error(self):
        errors = check_ui_field_behaviour_for_entry(_entry(None), YAML_PATH, _raise)
        assert len(errors) == 1
        assert "Failed to call" in errors[0]
