#!/usr/bin/env python

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

"""
Check that defaults in Schema JSON match the server-side SerializedBaseOperator defaults.

This ensures that the schema accurately reflects the actual default values
used by the server-side serialization layer.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def load_schema_defaults() -> dict[str, Any]:
    """Load default values from the JSON schema."""
    schema_path = Path("airflow-core/src/airflow/serialization/schema.json")

    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        sys.exit(1)

    with open(schema_path) as f:
        schema = json.load(f)

    # Extract defaults from the operator definition
    operator_def = schema.get("definitions", {}).get("operator", {})
    properties = operator_def.get("properties", {})

    defaults = {}
    for field_name, field_def in properties.items():
        if "default" in field_def:
            defaults[field_name] = field_def["default"]

    return defaults


def get_server_side_defaults() -> dict[str, Any]:
    """Get default values from server-side SerializedBaseOperator class."""
    try:
        from airflow.serialization.serialized_objects import SerializedBaseOperator

        # Get all serializable fields
        serialized_fields = SerializedBaseOperator.get_serialized_fields()

        # Field name mappings from external API names to internal class attribute names
        field_mappings = {
            "weight_rule": "_weight_rule",
        }

        server_defaults = {}
        for field_name in serialized_fields:
            # Use the mapped internal name if it exists, otherwise use the field name
            attr_name = field_mappings.get(field_name, field_name)

            if hasattr(SerializedBaseOperator, attr_name):
                default_value = getattr(SerializedBaseOperator, attr_name)
                # Only include actual default values, not methods/properties/descriptors
                if not callable(default_value) and not isinstance(default_value, (property, type)):
                    if isinstance(default_value, (set, tuple)):
                        # Convert to list since schema.json is pure JSON
                        default_value = list(default_value)
                    server_defaults[field_name] = default_value

        return server_defaults

    except ImportError as e:
        print(f"Error importing SerializedBaseOperator: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error getting server-side defaults: {e}")
        sys.exit(1)


def compare_defaults() -> list[str]:
    """Compare schema defaults with server-side defaults and return discrepancies."""
    schema_defaults = load_schema_defaults()
    server_defaults = get_server_side_defaults()
    errors = []

    print(f"Found {len(schema_defaults)} schema defaults")
    print(f"Found {len(server_defaults)} server-side defaults")

    # Check each server default against schema
    for field_name, server_value in server_defaults.items():
        schema_value = schema_defaults.get(field_name)

        # Check if field exists in schema
        if field_name not in schema_defaults:
            # Some server fields might not need defaults in schema (like None values)
            if server_value is not None and server_value not in [[], {}, (), set()]:
                errors.append(
                    f"Server field '{field_name}' has default {server_value!r} but no schema default"
                )
            continue

        # Direct comparison - no complex normalization needed
        if schema_value != server_value:
            errors.append(
                f"Field '{field_name}': schema default is {schema_value!r}, "
                f"server default is {server_value!r}"
            )

    # Check for schema defaults that don't have corresponding server defaults
    for field_name, schema_value in schema_defaults.items():
        if field_name not in server_defaults:
            # Some schema fields are structural and don't need server defaults
            schema_only_fields = {
                "task_type",
                "_task_module",
                "task_id",
                "_task_display_name",
                "_is_mapped",
                "_is_sensor",
            }
            if field_name not in schema_only_fields:
                errors.append(
                    f"Schema has default for '{field_name}' = {schema_value!r} but no corresponding server default"
                )

    return errors


def main():
    """Main function to run the schema defaults check."""
    print("Checking schema defaults against server-side SerializedBaseOperator...")

    errors = compare_defaults()

    if errors:
        print("❌ Found discrepancies between schema and server defaults:")
        for error in errors:
            print(f"  • {error}")
        print()
        print("To fix these issues:")
        print("1. Update airflow-core/src/airflow/serialization/schema.json to match server defaults, OR")
        print(
            "2. Update airflow-core/src/airflow/serialization/serialized_objects.py class defaults to match schema"
        )
        sys.exit(1)
    else:
        print("✅ All schema defaults match server-side defaults!")


if __name__ == "__main__":
    main()
