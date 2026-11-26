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

import copy
import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pytest
import yaml
from jsonschema import validate, validators
from jsonschema.exceptions import best_match

CHART_DIR = Path(__file__).parents[4] / "chart"
VALUES_SCHEMA = json.loads((CHART_DIR / "values.schema.json").read_text())
VALUES_SCHEMA_SCHEMA = json.loads((CHART_DIR / "values_schema.schema.json").read_text())
COMMON_REQUIRED_FIELDS = tuple(VALUES_SCHEMA_SCHEMA["$defs"]["commonRequired"]["required"])
REF_INHERITED_FIELDS = (*COMMON_REQUIRED_FIELDS, "type", "x-docsSection")


def _get_local_ref_target(schema: dict[str, Any], reference: str) -> dict[str, Any]:
    if not reference.startswith("#/"):
        raise ValueError(f"Only local JSON Pointer references are supported: {reference}")

    target: Any = schema
    for token in reference[2:].split("/"):
        decoded_token = unquote(token).replace("~1", "/").replace("~0", "~")
        if isinstance(target, dict) and decoded_token in target:
            target = target[decoded_token]
        elif isinstance(target, list) and decoded_token.isdigit() and int(decoded_token) < len(target):
            target = target[int(decoded_token)]
        else:
            raise ValueError(f"Cannot resolve schema reference: {reference}")
    if not isinstance(target, dict):
        raise ValueError(f"Schema reference does not resolve to an object: {reference}")
    return target


def _resolve_schema_metadata(schema: dict[str, Any]) -> dict[str, Any]:
    """Copy required metadata from local references before meta-schema validation."""
    resolved_schema = copy.deepcopy(schema)

    def resolve_schema_node(node: Any) -> None:
        if not isinstance(node, dict):
            return

        reference = node.get("$ref")
        if isinstance(reference, str):
            seen_references = set()
            while reference not in seen_references:
                seen_references.add(reference)
                target = _get_local_ref_target(schema, reference)
                for field in REF_INHERITED_FIELDS:
                    if field not in node and field in target:
                        node[field] = copy.deepcopy(target[field])
                reference = target.get("$ref")
                if not isinstance(reference, str):
                    break
            node.pop("$ref")

        for keyword in ("properties", "patternProperties"):
            for child in (node.get(keyword) or {}).values():
                resolve_schema_node(child)
        for keyword in ("items", "additionalProperties"):
            children = node.get(keyword)
            if isinstance(children, dict):
                resolve_schema_node(children)
            elif isinstance(children, list):
                for child in children:
                    resolve_schema_node(child)

    resolve_schema_node(resolved_schema)
    return resolved_schema


def _get_schema_metadata_errors(schema: dict[str, Any], validator):
    return list(validator.iter_errors(_resolve_schema_metadata(schema)))


def _iter_schemas_with_examples(schema: dict[str, Any], path: str = ""):
    if schema.get("examples"):
        yield path, schema
    for name, child in (schema.get("properties") or {}).items():
        yield from _iter_schemas_with_examples(child, f"{path}.{name}" if path else name)
    for key, suffix in (("items", "[]"), ("additionalProperties", ".*")):
        if isinstance(schema.get(key), dict):
            yield from _iter_schemas_with_examples(schema[key], f"{path}{suffix}")


SCHEMAS_WITH_EXAMPLES = dict(_iter_schemas_with_examples(VALUES_SCHEMA))


class TestChartQuality:
    """Tests chart quality."""

    def test_values_validate_schema(self):
        values = yaml.safe_load((CHART_DIR / "values.yaml").read_text())
        schema = json.loads((CHART_DIR / "values.schema.json").read_text())

        # Add extra restrictions just for the tests to make sure
        # we don't forget to update the schema if we add a new property
        schema["additionalProperties"] = False
        schema["minProperties"] = len(schema["properties"].keys())

        # shouldn't raise
        validate(instance=values, schema=schema)

    def test_values_schema_validates_against_meta_schema(self):
        validator_class = validators.validator_for(VALUES_SCHEMA_SCHEMA)
        validator_class.check_schema(VALUES_SCHEMA_SCHEMA)
        validator = validator_class(VALUES_SCHEMA_SCHEMA)

        errors = list(validator.iter_errors(VALUES_SCHEMA))
        assert not errors, [(list(error.absolute_path), error.message) for error in errors]

        errors = _get_schema_metadata_errors(VALUES_SCHEMA, validator)
        assert not errors, [(list(error.absolute_path), error.message) for error in errors]

    @pytest.mark.parametrize("required_field", (*COMMON_REQUIRED_FIELDS, "x-docsSection"))
    def test_values_schema_inherits_metadata_from_ref(self, required_field):
        validator_class = validators.validator_for(VALUES_SCHEMA_SCHEMA)
        validator = validator_class(VALUES_SCHEMA_SCHEMA)
        schema = {
            "description": "Root schema",
            "type": "object",
            "x-docsSectionOrder": [],
            "properties": {
                "referenced": {
                    "$ref": "#/definitions/shared",
                    "x-docsSection": "General",
                }
            },
            "definitions": {
                "shared": {
                    "description": "Referenced property",
                    "default": "value",
                    "type": "string",
                    "x-docsSection": "General",
                }
            },
        }

        schema["properties"]["referenced"].pop(required_field, None)
        assert not _get_schema_metadata_errors(schema, validator)

        schema["definitions"]["shared"].pop(required_field)
        errors = _get_schema_metadata_errors(schema, validator)
        assert any(list(error.absolute_path) == ["properties", "referenced"] for error in errors)

    @pytest.mark.parametrize("required_field", COMMON_REQUIRED_FIELDS)
    def test_values_schema_requires_metadata_for_nested_leafs(self, required_field):
        validator_class = validators.validator_for(VALUES_SCHEMA_SCHEMA)
        validator = validator_class(VALUES_SCHEMA_SCHEMA)

        assert not _get_schema_metadata_errors(VALUES_SCHEMA, validator)

        schema_without_metadata = copy.deepcopy(VALUES_SCHEMA)
        del schema_without_metadata["properties"]["scheduler"]["properties"]["replicas"][required_field]

        assert any(
            list(error.absolute_path) == ["properties", "scheduler", "properties", "replicas"]
            for error in _get_schema_metadata_errors(schema_without_metadata, validator)
        )

    @pytest.mark.parametrize("required_field", COMMON_REQUIRED_FIELDS)
    def test_values_schema_requires_metadata_for_top_level_properties(self, required_field):
        validator_class = validators.validator_for(VALUES_SCHEMA_SCHEMA)
        validator = validator_class(VALUES_SCHEMA_SCHEMA)

        assert not _get_schema_metadata_errors(VALUES_SCHEMA, validator)

        schema_without_metadata = copy.deepcopy(VALUES_SCHEMA)
        del schema_without_metadata["properties"]["scheduler"][required_field]

        assert any(
            list(error.absolute_path) == ["properties", "scheduler"]
            for error in _get_schema_metadata_errors(schema_without_metadata, validator)
        )

    @pytest.mark.parametrize("path", SCHEMAS_WITH_EXAMPLES)
    def test_schema_examples_validate_against_their_own_schema(self, path):
        """Examples are rendered verbatim into the parameters reference, so they must be valid values."""
        schema = SCHEMAS_WITH_EXAMPLES[path]
        validator = validators.validator_for(VALUES_SCHEMA)(
            {**schema, "definitions": VALUES_SCHEMA["definitions"]}
        )
        for example in schema["examples"]:
            # chart/docs/conf.py renders each example of an array parameter as a single list element
            instance = [example] if schema.get("type") == "array" else example
            error = best_match(validator.iter_errors(instance))
            assert error is None, f"{path}: {error.message}"
