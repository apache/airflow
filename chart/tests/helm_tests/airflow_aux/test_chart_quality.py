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
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import validate, validators
from jsonschema.exceptions import best_match

CHART_DIR = Path(__file__).parents[4] / "chart"
VALUES_SCHEMA = json.loads((CHART_DIR / "values.schema.json").read_text())


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
