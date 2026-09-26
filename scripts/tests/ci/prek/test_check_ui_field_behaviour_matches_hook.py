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
    @pytest.mark.parametrize(
        "yaml_behaviour, get_behaviour",
        [
            pytest.param(None, _hook(None), id="skip-hook-without-get-ui-field-behaviour"),
            pytest.param(
                {
                    "hidden-fields": ["port", "schema"],
                    "relabeling": {"host": "Server URL"},
                    "placeholders": {"extra": '{"a": 1, "b": 2}'},
                },
                _hook(
                    {
                        "hidden_fields": ["schema", "port"],
                        "relabeling": {"host": "Server URL"},
                        "placeholders": {"extra": '{\n  "b": 2,\n  "a": 1\n}\n'},
                    }
                ),
                id="matching-behaviour-with-formatting-differences",
            ),
        ],
    )
    def test_no_errors(self, yaml_behaviour, get_behaviour):
        assert check_ui_field_behaviour_for_entry(_entry(yaml_behaviour), YAML_PATH, get_behaviour) == []

    @pytest.mark.parametrize(
        "yaml_behaviour, get_behaviour, expected_in_error",
        [
            pytest.param(
                None, _hook({"hidden_fields": ["port"]}), "no such section", id="missing-yaml-section"
            ),
            pytest.param(
                {"hidden-fields": ["port"]},
                _hook({"hidden_fields": ["port", "schema"]}),
                "only in the hook: ['schema']",
                id="hidden-fields-drift",
            ),
            pytest.param(
                {"relabeling": {"host": "Server URL"}},
                _hook({"relabeling": {"host": "Server URL (optional)"}}),
                "relabeling differ for: host",
                id="relabeling-drift",
            ),
            pytest.param(
                {"placeholders": {"extra": '{"model": "old"}'}},
                _hook({"placeholders": {"extra": '{"model": "new"}', "login": "user"}}),
                "placeholders differ for: extra, login",
                id="placeholders-drift",
            ),
            pytest.param(None, _raise, "boom", id="unexpected-exception-message"),
        ],
    )
    def test_one_error_containing(self, yaml_behaviour, get_behaviour, expected_in_error):
        errors = check_ui_field_behaviour_for_entry(_entry(yaml_behaviour), YAML_PATH, get_behaviour)
        assert len(errors) == 1
        assert expected_in_error in errors[0]
