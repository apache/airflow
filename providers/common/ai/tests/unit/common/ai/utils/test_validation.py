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

from airflow.providers.common.ai.utils.validation import validate_prompt


class TestValidatePrompt:
    @pytest.mark.parametrize(
        "value",
        ["hello", "  hello  "],
        ids=["plain", "with-surrounding-whitespace"],
    )
    def test_accepts_non_empty_strings(self, value):
        validate_prompt(value, decorator_name="@task.agent")

    @pytest.mark.parametrize(
        "value",
        ["", "   ", "\n\t"],
        ids=["empty", "spaces", "newlines-tabs"],
    )
    def test_rejects_blank_strings(self, value):
        with pytest.raises(TypeError, match="non-empty string or a non-empty Sequence"):
            validate_prompt(value, decorator_name="@task.agent")

    @pytest.mark.parametrize(
        "value",
        [
            ["text", object()],
            ("text", object()),
            [object()],
        ],
        ids=["list-multi", "tuple-multi", "list-single"],
    )
    def test_accepts_non_empty_sequence(self, value):
        validate_prompt(value, decorator_name="@task.agent")

    @pytest.mark.parametrize(
        "value",
        [[], ()],
        ids=["empty-list", "empty-tuple"],
    )
    def test_rejects_empty_sequence(self, value):
        with pytest.raises(TypeError, match="non-empty string or a non-empty Sequence"):
            validate_prompt(value, decorator_name="@task.agent")

    @pytest.mark.parametrize(
        ("value", "match"),
        [
            (None, "got NoneType"),
            (42, "got int"),
            ({"key": "value"}, "got dict"),
        ],
        ids=["none", "int", "dict"],
    )
    def test_rejects_unsupported_scalar(self, value, match):
        with pytest.raises(TypeError, match=match):
            validate_prompt(value, decorator_name="@task.agent")

    @pytest.mark.parametrize(
        ("value", "name"),
        [
            (b"bytes", "bytes"),
            (bytearray(b"x"), "bytearray"),
        ],
        ids=["bytes", "bytearray"],
    )
    def test_rejects_bytes_like(self, value, name):
        with pytest.raises(TypeError, match=f"not {name}"):
            validate_prompt(value, decorator_name="@task.agent")

    def test_decorator_name_appears_in_error(self):
        with pytest.raises(TypeError, match=r"@task\.llm_sql"):
            validate_prompt(42, decorator_name="@task.llm_sql")

    @pytest.mark.parametrize(
        "value",
        [[b"x"], ["ok", bytearray(b"y")]],
        ids=["bytes-item", "bytearray-mixed"],
    )
    def test_rejects_bytes_like_in_sequence(self, value):
        with pytest.raises(TypeError, match="raw bytes are not a valid UserContent"):
            validate_prompt(value, decorator_name="@task.agent")
