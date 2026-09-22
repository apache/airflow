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

from common_prek_utils import AIRFLOW_ROOT_PATH

_VALUES_SCHEMA_FILE = AIRFLOW_ROOT_PATH / "chart/values.schema.json"
_VALUES_SCHEMA_SCHEMA_FILE = AIRFLOW_ROOT_PATH / "chart/values_schema.schema.json"


if __name__ != "__main__":
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
    )


def _list_all_field_types(data: dict | list) -> list:
    type_fields = []

    if isinstance(data, dict):
        if "type" in data:
            if isinstance(data["type"], list):
                type_fields.append(tuple(data["type"]))
            elif not isinstance(data["type"], dict):
                type_fields.append(data["type"])

        # Skip potential `type` fields inside `default` fields as defaults are not
        # part of the schema file verification process
        temp = list((v for k, v in data.items() if k != "default"))
    elif isinstance(data, list):
        temp = data

    for val in (val for val in temp if isinstance(val, (dict, list))):
        type_fields.extend(_list_all_field_types(val))

    return type_fields


def _sort(data: str | tuple[str, ...]) -> str:
    if isinstance(data, tuple):
        return "".join(data)

    return data


def _get_one_of_for_schema(fields: list) -> list:
    one_of: list = []

    for field_type in fields:
        one_of.append({"properties": {"type": {"const": field_type}}})

        if "array" in field_type:
            one_of[-1]["properties"]["items"] = False
        elif "object" in field_type:
            one_of[-1]["properties"]["properties"] = False

    return one_of


def main() -> int:
    with open(_VALUES_SCHEMA_FILE, encoding="utf-8") as schema_file:
        schema = json.loads(schema_file.read())

    field_types = sorted(set(_list_all_field_types(schema)), key=_sort)

    with open(_VALUES_SCHEMA_SCHEMA_FILE, encoding="utf-8") as schema_file:
        schema = json.loads(schema_file.read())

    schema["definitions"]["leafs"]["if"]["oneOf"] = _get_one_of_for_schema(field_types)

    with open(_VALUES_SCHEMA_SCHEMA_FILE, "w", encoding="utf-8") as schema_file:
        json.dump(schema, schema_file, indent=4)
        schema_file.write("\n")

    return 0


sys.exit(main())
