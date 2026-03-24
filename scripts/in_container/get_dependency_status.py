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
from collections.abc import Iterable


def read_input():
    input_data = sys.stdin.read()
    return input_data


def get_dep_level(line: str) -> tuple[str, int] | None:
    split_line = line.split()
    if len(split_line) < 2:
        return None
    level = len(split_line) - 2
    package = split_line[level]
    if package in {"-", "├──", "└──", "│"}:
        return None
    return package, level


def build_dependency_depth_map(lines: Iterable[str]) -> dict[str, int]:
    deps_depth: dict[str, int] = {}
    for line in lines:
        parsed_line = get_dep_level(line)
        if parsed_line is None:
            continue
        package, level = parsed_line
        if package not in deps_depth:
            deps_depth[package] = level
    return deps_depth


if __name__ == "__main__":
    input_data = read_input()
    lines = input_data.splitlines()
    print(json.dumps(build_dependency_depth_map(lines), indent=4))
