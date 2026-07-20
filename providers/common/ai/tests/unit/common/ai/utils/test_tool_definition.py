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
from unittest.mock import patch

import pytest
from pydantic_core import ValidationError

from airflow.providers.common.ai.utils import tool_definition
from airflow.providers.common.ai.utils.tool_definition import build_args_validator, return_schema_kwargs


def test_returns_kwarg_when_supported():
    with patch.object(tool_definition, "_SUPPORTS_RETURN_SCHEMA", True):
        assert return_schema_kwargs({"type": "string"}) == {"return_schema": {"type": "string"}}


def test_returns_empty_when_unsupported():
    with patch.object(tool_definition, "_SUPPORTS_RETURN_SCHEMA", False):
        assert return_schema_kwargs({"type": "string"}) == {}


TOOL_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "count": {"type": "integer"},
        "ratio": {"type": "number"},
        "enabled": {"type": "boolean"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "options": {"type": "object"},
        "nullable_name": {"type": ["string", "null"]},
        "payload": {"anyOf": [{"type": "string"}, {"type": "object"}, {"type": "null"}]},
        "anything": {},
    },
    "required": ["name"],
}


NESTED_SCHEMA = {
    "type": "object",
    "properties": {
        "config": {
            "type": "object",
            "properties": {"port": {"type": "integer"}},
            "required": ["port"],
        },
    },
    "required": ["config"],
}


def _validate(validator, args: dict, use_json: bool):
    if use_json:
        return validator.validate_json(json.dumps(args))
    return validator.validate_python(args)


@pytest.mark.parametrize("use_json", [False, True], ids=["python", "json"])
class TestBuildArgsValidator:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            ({"name": "a"}, {"name": "a"}),
            (
                {
                    "name": "a",
                    "count": 2,
                    "ratio": 0.5,
                    "enabled": True,
                    "tags": ["x"],
                    "options": {"k": "v"},
                    "nullable_name": None,
                    "payload": {"key": "value"},
                    "anything": [1, "b"],
                },
                {
                    "name": "a",
                    "count": 2,
                    "ratio": 0.5,
                    "enabled": True,
                    "tags": ["x"],
                    "options": {"k": "v"},
                    "nullable_name": None,
                    "payload": {"key": "value"},
                    "anything": [1, "b"],
                },
            ),
            ({"name": "a", "count": "5"}, {"name": "a", "count": 5}),
        ],
    )
    def test_valid_args_accepted(self, args, expected, use_json):
        validator = build_args_validator(TOOL_SCHEMA)
        assert _validate(validator, args, use_json) == expected

    @pytest.mark.parametrize(
        "args",
        [
            {"count": 1},
            {"name": "a", "count": "not-an-int"},
            {"name": "a", "tags": "not-a-list"},
            {"name": "a", "nullable_name": 1},
            {"name": "a", "payload": []},
        ],
    )
    def test_invalid_args_rejected(self, args, use_json):
        validator = build_args_validator(TOOL_SCHEMA)
        with pytest.raises(ValidationError):
            _validate(validator, args, use_json)

    def test_extra_keys_dropped(self, use_json):
        validator = build_args_validator(TOOL_SCHEMA)
        assert _validate(validator, {"name": "a", "junk": 1}, use_json) == {"name": "a"}

    def test_additional_properties_preserved(self, use_json):
        schema = {**TOOL_SCHEMA, "additionalProperties": True}
        validator = build_args_validator(schema)
        assert _validate(validator, {"name": "a", "extra": 1}, use_json) == {
            "name": "a",
            "extra": 1,
        }

    def test_empty_properties_accepts_empty_args(self, use_json):
        validator = build_args_validator({"type": "object", "properties": {}, "required": []})
        assert _validate(validator, {}, use_json) == {}

    def test_nested_object_validated_recursively(self, use_json):
        validator = build_args_validator(NESTED_SCHEMA)
        assert _validate(validator, {"config": {"port": "5432"}}, use_json) == {"config": {"port": 5432}}

    @pytest.mark.parametrize(
        "args",
        [
            {"config": {"port": "not-an-int"}},
            {"config": {}},
        ],
    )
    def test_nested_object_invalid_rejected(self, args, use_json):
        validator = build_args_validator(NESTED_SCHEMA)
        with pytest.raises(ValidationError):
            _validate(validator, args, use_json)

    def test_untyped_nested_object_passthrough(self, use_json):
        schema = {"type": "object", "properties": {"payload": {"type": "object"}}}
        validator = build_args_validator(schema)
        args = {"payload": {"any": 1, "deep": {"k": "v"}}}
        assert _validate(validator, args, use_json) == args
