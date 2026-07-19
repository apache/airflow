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
"""Version-tolerant helpers for building pydantic-ai ``ToolDefinition`` objects."""

from __future__ import annotations

import dataclasses
from typing import Any, Literal

from pydantic_ai.tools import ToolDefinition
from pydantic_core import SchemaValidator, core_schema

# ``ToolDefinition.return_schema`` is newer than the provider's pydantic-ai
# floor. Detect it once so callers can include the kwarg only when supported,
# rather than raising ``TypeError`` on older installs.
_SUPPORTS_RETURN_SCHEMA = any(f.name == "return_schema" for f in dataclasses.fields(ToolDefinition))


def return_schema_kwargs(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Return ``{"return_schema": schema}`` when pydantic-ai supports the field, else ``{}``.

    ``return_schema`` lets CodeMode (the Monty sandbox) render a typed function
    signature for a tool (``-> str``) instead of ``-> Any``, which helps the
    model write correct code. It has no effect outside code mode.

    :param schema: A JSON Schema fragment describing the tool's return value.
    """
    if _SUPPORTS_RETURN_SCHEMA:
        return {"return_schema": schema}
    return {}


def _fragment_to_core_schema(fragment: dict[str, Any]) -> core_schema.CoreSchema:
    any_of = fragment.get("anyOf")
    if isinstance(any_of, list):
        choices: list[core_schema.CoreSchema | tuple[core_schema.CoreSchema, str]] = [
            _fragment_to_core_schema(choice) for choice in any_of if isinstance(choice, dict)
        ]
        return core_schema.union_schema(choices) if choices else core_schema.any_schema()

    schema_type = fragment.get("type")
    if isinstance(schema_type, list):
        choices = [
            _fragment_to_core_schema({**fragment, "type": item})
            for item in schema_type
            if isinstance(item, str)
        ]
        return core_schema.union_schema(choices) if choices else core_schema.any_schema()

    match schema_type:
        case "string":
            return core_schema.str_schema()
        case "integer":
            return core_schema.int_schema()
        case "number":
            return core_schema.float_schema()
        case "boolean":
            return core_schema.bool_schema()
        case "null":
            return core_schema.none_schema()
        case "array":
            items = fragment.get("items")
            return core_schema.list_schema(
                _fragment_to_core_schema(items) if isinstance(items, dict) else None
            )
        case "object":
            return core_schema.dict_schema()
        case _:
            return core_schema.any_schema()


def build_args_validator(parameters_json_schema: dict[str, Any]) -> SchemaValidator:
    """Build an argument validator from the schema advertised to the model."""
    required = set(parameters_json_schema.get("required", []))
    fields = {
        name: core_schema.typed_dict_field(_fragment_to_core_schema(prop), required=name in required)
        for name, prop in parameters_json_schema.get("properties", {}).items()
    }
    extra_behavior: Literal["allow", "ignore"] = (
        "allow" if parameters_json_schema.get("additionalProperties") is True else "ignore"
    )
    return SchemaValidator(core_schema.typed_dict_schema(fields, extra_behavior=extra_behavior))
