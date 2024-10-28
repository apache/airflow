#!/usr/bin/env python3
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

import re
from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()


def sort_tags(filename: Path):
    with filename.open() as f:
        lines = f.readlines()

    # Find the start index of the tags list
    tag_start_pattern = re.compile(r"tags:")
    start_index = next(
        (index + 1 for index, line in enumerate(lines) if tag_start_pattern.match(line)),
        None,
    )

    if start_index is None:
        print("Tags list not found in the YAML file.")
        return

    # Find the end index of the tags list
    end_index = start_index
    tag_pattern = re.compile(r"\s*-\s*name:")
    while end_index < len(lines) and tag_pattern.match(lines[end_index]):
        end_index += 1

    # Extract and sort the tags
    tags_lines = lines[start_index:end_index]
    sorted_tags_lines = sorted(tags_lines, key=lambda x: x.strip().split(": ")[-1])

    # Replace the sorted tags in the original content
    lines[start_index:end_index] = sorted_tags_lines

    with filename.open("w") as f:
        f.write("".join(lines))


if __name__ == "__main__":
    openapi_spec_filepath = (
        Path(AIRFLOW_SOURCES) / "airflow" / "api_connexion" / "openapi" / "v1.yaml"
    )
    sort_tags(openapi_spec_filepath)
