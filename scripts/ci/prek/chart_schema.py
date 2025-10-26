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
from __future__ import annotations

import json
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

CHART_DIR = AIRFLOW_ROOT_PATH / "chart"
KNOWN_INVALID_TYPES = {
    # I don't know the data structure for this type with 100 certainty. We have no tests.
    "$['properties']['ingress']['properties']['web']['properties']['precedingPaths']",
    # I don't know the data structure for this type with 100 certainty. We have no tests.
    "$['properties']['ingress']['properties']['web']['properties']['succeedingPaths']",
    # The value of this parameter is passed to statsd_exporter, which does not have a strict type definition.
    "$['properties']['statsd']['properties']['extraMappings']",
    "$['properties']['statsd']['properties']['overrideMappings']",
}
VENDORED_PATHS = {
    # We don't want to check the upstream k8s definitions
    "$['definitions']['io.k8s",
}

SCHEMA = json.loads((CHART_DIR / "values.schema.json").read_text())


def display_definitions_list(definitions_list):
    print("Invalid definitions: ")
    for no_d, (schema_type, schema_path) in enumerate(definitions_list, start=1):
        print(f"{no_d}: {schema_path}")
        print(json.dumps(schema_type, indent=2))


def walk(value, path="$"):
    yield value, path
    if isinstance(value, dict):
        for k, v in value.items():
            yield from walk(v, path + f"[{k!r}]")
    elif isinstance(value, (list, set, tuple)):
        for no, v in enumerate(value):
            yield from walk(v, path + f"[{no}]")


def is_vendored_path(path: str) -> bool:
    for prefix in VENDORED_PATHS:
        if path.startswith(prefix):
            return True
    return False


def validate_object_types():
    all_object_types = ((d, p) for d, p in walk(SCHEMA) if isinstance(d, dict) and d.get("type") == "object")
    all_object_types_with_a_loose_definition = [
        (d, p)
        for d, p in all_object_types
        if "properties" not in d
        and "$ref" not in d
        and not isinstance(d.get("additionalProperties"), dict)
        and p not in KNOWN_INVALID_TYPES
        and not is_vendored_path(p)
    ]
    to_display_invalid_types = [
        (d, p) for d, p in all_object_types_with_a_loose_definition if p not in KNOWN_INVALID_TYPES
    ]
    if to_display_invalid_types:
        print(
            "Found object type definitions with too loose a definition. "
            "Make sure that the type meets one of the following conditions:"
        )
        print(" - has a `properties` key")
        print(" - has a `$ref` key")
        print(" - has a `additionalProperties` key, which content is an object")
        display_definitions_list(to_display_invalid_types)
    return all_object_types_with_a_loose_definition


def validate_array_types():
    all_array_types = ((d, p) for d, p in walk(SCHEMA) if isinstance(d, dict) and d.get("type") == "array")
    all_array_types_with_a_loose_definition = [
        (d, p) for (d, p) in all_array_types if not isinstance(d.get("items"), dict)
    ]
    to_display_invalid_types = [
        (d, p) for d, p in all_array_types_with_a_loose_definition if p not in KNOWN_INVALID_TYPES
    ]

    if to_display_invalid_types:
        print(
            "Found array type definitions with too loose a definition. "
            "Make sure the object has the items key."
        )
        display_definitions_list(to_display_invalid_types)
    return all_array_types_with_a_loose_definition


invalid_object_types_path = {p for _, p in validate_object_types()}
invalid_array_types_path = {p for _, p in validate_array_types()}
fixed_types = KNOWN_INVALID_TYPES - invalid_object_types_path - invalid_array_types_path
invalid_paths = (invalid_object_types_path.union(invalid_array_types_path)) - KNOWN_INVALID_TYPES

if fixed_types:
    current_file = Path(__file__).resolve()
    print(
        f"Some types that were known to be invalid have been fixed. Can you update the variable "
        f"`known_invalid_types` in file {current_file!r}? You just need to delete the following items:"
    )
    print("\n".join(fixed_types))

if fixed_types or invalid_paths:
    sys.exit(1)
else:
    print("No problems")
