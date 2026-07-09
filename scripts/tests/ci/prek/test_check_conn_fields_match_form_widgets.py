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
    build_mismatch_error,
    check_conn_fields_for_entry,
)

YAML_PATH = "providers/my_provider/provider.yaml"
HOOK_CLASS = "my_provider.hooks.my_hook.MyHook"
CONN_TYPE = "my_conn_type"


def _entry(conn_fields: list[str] | None) -> dict:
    entry: dict = {"hook-class-name": HOOK_CLASS, "connection-type": CONN_TYPE}
    if conn_fields is not None:
        entry["conn-fields"] = {k: {} for k in conn_fields}
    return entry


def _get_keys(*keys: str):
    """Return a get_widget_keys callable that always returns the given keys."""
    return lambda _hook_class_name: set(keys)


def _skip(_hook_class_name: str) -> None:
    """get_widget_keys callable that signals 'skip silently'."""
    return None


def _raise(_hook_class_name: str) -> None:
    raise RuntimeError("boom")


class TestBuildMismatchError:
    @pytest.mark.parametrize(
        "yaml_keys, hook_keys",
        [
            pytest.param({"a", "b"}, {"a", "b"}, id="matching-keys"),
            pytest.param(set(), set(), id="empty-sets"),
            # Hook may have more keys than conn-fields — that is intentional (subset allowed).
            pytest.param({"a"}, {"a", "extra_hook"}, id="hook-has-extra-keys-no-error"),
        ],
    )
    def test_no_mismatch_returns_none(self, yaml_keys, hook_keys):
        assert build_mismatch_error(yaml_keys, hook_keys, CONN_TYPE, YAML_PATH, HOOK_CLASS) is None

    @pytest.mark.parametrize(
        "yaml_keys, hook_keys, expected_in_error, not_expected_in_error",
        [
            pytest.param(
                {"a", "extra_yaml"},
                {"a"},
                ["extra_yaml", "NOT in get_connection_form_widgets"],
                ["NOT in provider.yaml conn-fields"],
                id="extra-in-yaml",
            ),
            # only_in_hook no longer triggers an error — only only_in_yaml does
            pytest.param(
                {"a", "only_yaml"},
                {"a", "only_hook"},
                [
                    "only_yaml",
                    "NOT in get_connection_form_widgets",
                ],
                ["only_hook", "NOT in provider.yaml conn-fields"],
                id="both-sides-only-yaml-reported",
            ),
        ],
    )
    def test_mismatch_error_content(self, yaml_keys, hook_keys, expected_in_error, not_expected_in_error):
        error = build_mismatch_error(yaml_keys, hook_keys, CONN_TYPE, YAML_PATH, HOOK_CLASS)
        assert error is not None
        for expected in expected_in_error:
            assert expected in error
        for not_expected in not_expected_in_error:
            assert not_expected not in error


class TestCheckConnFieldsForEntry:
    @pytest.mark.parametrize(
        "conn_fields, get_keys",
        [
            pytest.param(["a", "b"], _get_keys("a", "b"), id="matching-keys"),
            pytest.param([], _get_keys(), id="empty-on-both-sides"),
            pytest.param(["a"], _skip, id="skip-hook-without-get-connection-form-widgets"),
            pytest.param(None, _skip, id="skip-missing-conn-fields-when-hook-has-no-widgets"),
            # Hook with widgets but no conn-fields is allowed: new UI intentionally omits custom fields.
            pytest.param(None, _get_keys("field_a"), id="no-conn-fields-with-hook-widgets-is-ok"),
            # Hook has extra keys not in conn-fields — allowed (conn-fields is a valid subset).
            pytest.param(["a"], _get_keys("a", "extra_hook"), id="hook-extra-keys-no-error"),
        ],
    )
    def test_no_errors(self, conn_fields, get_keys):
        assert check_conn_fields_for_entry(_entry(conn_fields), YAML_PATH, get_keys) == []

    @pytest.mark.parametrize(
        "conn_fields, get_keys, expected_in_error",
        [
            pytest.param(["a", "extra"], _get_keys("a"), "extra", id="extra-key-in-yaml"),
            pytest.param(["a"], _raise, "boom", id="unexpected-exception-message"),
            pytest.param(["a"], _raise, HOOK_CLASS, id="unexpected-exception-mentions-hook-class"),
        ],
    )
    def test_one_error_containing(self, conn_fields, get_keys, expected_in_error):
        errors = check_conn_fields_for_entry(_entry(conn_fields), YAML_PATH, get_keys)
        assert len(errors) == 1
        assert expected_in_error in errors[0]
