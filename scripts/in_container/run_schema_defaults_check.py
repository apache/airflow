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
import traceback
from datetime import timedelta
from pathlib import Path
from typing import Any


def load_schema_defaults(object_type: str = "operator") -> dict[str, Any]:
    """Load default values from the JSON schema."""
    schema_path = Path("airflow-core/src/airflow/serialization/schema.json")

    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        sys.exit(1)

    with open(schema_path) as f:
        schema = json.load(f)

    # Extract defaults from the specified object type definition
    object_def = schema.get("definitions", {}).get(object_type, {})
    properties = object_def.get("properties", {})

    defaults = {}
    for field_name, field_def in properties.items():
        if "default" in field_def:
            defaults[field_name] = field_def["default"]

    return defaults


def get_server_side_operator_defaults() -> dict[str, Any]:
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
                    elif isinstance(default_value, timedelta):
                        default_value = default_value.total_seconds()
                    server_defaults[field_name] = default_value

        return server_defaults

    except ImportError as e:
        print(f"Error importing SerializedBaseOperator: {e}")
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"Error getting server-side defaults: {e}")
        traceback.print_exc()
        sys.exit(1)


def get_server_side_dag_defaults() -> dict[str, Any]:
    """Get default values from server-side SerializedDAG class."""
    try:
        from airflow.serialization.definitions.dag import SerializedDAG

        # DAG defaults are set in __init__, so we create a temporary instance
        temp_dag = SerializedDAG(dag_id="temp")

        # Get all serializable DAG fields from the server-side class
        serialized_fields = SerializedDAG.get_serialized_fields()

        server_defaults = {}
        for field_name in serialized_fields:
            if hasattr(temp_dag, field_name):
                default_value = getattr(temp_dag, field_name)
                # Only include actual default values that are not None, callables, or descriptors
                if not callable(default_value) and not isinstance(default_value, (property, type)):
                    if isinstance(default_value, (set, tuple)):
                        # Convert to list since schema.json is pure JSON
                        default_value = list(default_value)
                    server_defaults[field_name] = default_value

        return server_defaults

    except ImportError as e:
        print(f"Error importing SerializedDAG: {e}")
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print(f"Error getting server-side DAG defaults: {e}")
        traceback.print_exc()
        sys.exit(1)


def compare_operator_defaults() -> list[str]:
    """Compare operator schema defaults with server-side defaults and return discrepancies."""
    schema_defaults = load_schema_defaults("operator")
    server_defaults = get_server_side_operator_defaults()
    errors = []

    print(f"Found {len(schema_defaults)} operator schema defaults")
    print(f"Found {len(server_defaults)} operator server-side defaults")

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


def compare_dag_defaults() -> list[str]:
    """Compare DAG schema defaults with server-side defaults and return discrepancies."""
    schema_defaults = load_schema_defaults("dag")
    server_defaults = get_server_side_dag_defaults()
    errors = []

    print(f"Found {len(schema_defaults)} DAG schema defaults")
    print(f"Found {len(server_defaults)} DAG server-side defaults")

    # Check each server default against schema
    for field_name, server_value in server_defaults.items():
        schema_value = schema_defaults.get(field_name)

        # Check if field exists in schema
        if field_name not in schema_defaults:
            # Some server fields don't need defaults in schema (like None values, empty collections, or computed fields)
            if (
                server_value is not None
                and server_value not in [[], {}, (), set()]
                and field_name not in ["dag_id", "dag_display_name"]
            ):
                errors.append(
                    f"DAG server field '{field_name}' has default {server_value!r} but no schema default"
                )
            continue

        # Direct comparison
        if schema_value != server_value:
            errors.append(
                f"DAG field '{field_name}': schema default is {schema_value!r}, "
                f"server default is {server_value!r}"
            )

    # Check for schema defaults that don't have corresponding server defaults
    for field_name, schema_value in schema_defaults.items():
        if field_name not in server_defaults:
            # Some schema fields are computed properties (like has_on_*_callback)
            computed_properties = {
                "has_on_success_callback",
                "has_on_failure_callback",
            }
            if field_name not in computed_properties:
                errors.append(
                    f"DAG schema has default for '{field_name}' = {schema_value!r} but no corresponding server default"
                )

    return errors


def main():
    """Main function to run the schema defaults check."""
    print("Checking schema defaults against server-side serialization classes...")

    # Check Operator defaults
    print("\n1. Checking Operator defaults...")
    operator_errors = compare_operator_defaults()

    # Check Dag defaults
    print("\n2. Checking Dag defaults...")
    dag_errors = compare_dag_defaults()

    all_errors = operator_errors + dag_errors

    if all_errors:
        print("\n❌ Found discrepancies between schema and server defaults:")
        for error in all_errors:
            print(f"  • {error}")
        print()
        print("To fix these issues:")
        print("1. Update airflow-core/src/airflow/serialization/schema.json to match server defaults, OR")
        print(
            "2. Update airflow-core/src/airflow/serialization/serialized_objects.py class/init defaults to match schema"
        )
        sys.exit(1)
    else:
        print("\n✅ All schema defaults match server-side defaults!")


if __name__ == "__main__":
    main()
