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


if __name__ != "__main__":
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
    )


def _sort(data: dict | list) -> None:
    "Sort all elements in `type` fields in json file"
    if isinstance(data, dict):
        if "type" in data and isinstance(data["type"], list):
            data["type"].sort()

        # Skip potential `type` fields inside `default` fields as defaults are not
        # part of the schema file verification process
        temp = list((v for k, v in data.items() if k != "default"))
    elif isinstance(data, list):
        temp = data

    for val in (val for val in temp if isinstance(val, (dict, list))):
        _sort(val)


def main() -> int:
    with open(_VALUES_SCHEMA_FILE, encoding="utf-8") as schema_file:
        schema = json.loads(schema_file.read())

    _sort(schema)

    with open(_VALUES_SCHEMA_FILE, "w", encoding="utf-8") as schema_file:
        json.dump(schema, schema_file, indent=4)
        schema_file.write("\n")

    return 0


sys.exit(main())
