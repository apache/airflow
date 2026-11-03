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

from airflow.providers.common.ai.utils.tool_prefix import ToolPrefixMixin, validate_tool_prefix


class _Prefixed(ToolPrefixMixin):
    def __init__(self, tool_prefix: str = "") -> None:
        self._set_tool_prefix(tool_prefix)


class TestValidateToolPrefix:
    @pytest.mark.parametrize("tool_prefix", ["a-b", "9lives", "has space"])
    def test_rejects_a_prefix_that_is_not_an_identifier(self, tool_prefix):
        with pytest.raises(
            ValueError, match=f"tool_prefix must be a valid Python identifier, got {tool_prefix!r}"
        ):
            validate_tool_prefix(tool_prefix)

    @pytest.mark.parametrize("tool_prefix", ["", "pg", "_pg", "pg2"])
    def test_accepts_an_empty_or_identifier_prefix(self, tool_prefix):
        validate_tool_prefix(tool_prefix)


class TestToolPrefixMixin:
    def test_set_tool_prefix_rejects_and_keeps_the_previous_prefix(self):
        toolset = _Prefixed("pg")

        with pytest.raises(ValueError, match="valid Python identifier"):
            toolset._set_tool_prefix("a-b")

        assert toolset._tool_prefix == "pg"

    def test_the_prefix_defaults_to_empty(self):
        assert ToolPrefixMixin._tool_prefix == ""

    @pytest.mark.parametrize(("tool_prefix", "expected"), [("", "query"), ("pg", "pg_query")])
    def test_build_tool_name(self, tool_prefix, expected):
        assert _Prefixed(tool_prefix)._build_tool_name("query") == expected

    @pytest.mark.parametrize(
        ("tool_prefix", "name", "expected"),
        [
            ("pg", "pg_query", "query"),
            ("pg", "query", "query"),
            ("pg", "pg_pg_query", "pg_query"),
            ("", "query", "query"),
        ],
    )
    def test_strip_tool_prefix_is_lenient_about_a_name_without_the_prefix(self, tool_prefix, name, expected):
        assert _Prefixed(tool_prefix)._strip_tool_prefix(name) == expected

    @pytest.mark.parametrize(
        ("tool_prefix", "name", "expected"),
        [
            ("pg", "pg_query", "query"),
            ("pg", "query", None),
            ("pg", "pgquery", None),
            ("", "query", "query"),
        ],
    )
    def test_find_base_tool_name_is_strict_about_a_name_without_the_prefix(self, tool_prefix, name, expected):
        assert _Prefixed(tool_prefix)._find_base_tool_name(name) == expected

    @pytest.mark.parametrize(
        ("tool_prefix", "separator", "expected"),
        [("", "-", ""), ("", "_", ""), ("pg", "-", "-pg"), ("pg", "_", "_pg")],
    )
    def test_get_tool_prefix_id_suffix(self, tool_prefix, separator, expected):
        assert _Prefixed(tool_prefix)._get_tool_prefix_id_suffix(separator) == expected

    def test_get_tool_prefix_id_suffix_defaults_to_a_dash(self):
        assert _Prefixed("pg")._get_tool_prefix_id_suffix() == "-pg"
