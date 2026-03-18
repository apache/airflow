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


def read_input():
    input_data = sys.stdin.read()
    return input_data


DEPS_DEPTH: dict[str, int] = {}


def get_dep_level(line: str):
    split_line = line.split()
    level = len(split_line) - 2
    package = split_line[level]
    if package not in DEPS_DEPTH:
        DEPS_DEPTH[package] = level


if __name__ == "__main__":
    input_data = read_input()
    lines = input_data.splitlines()
    for line in lines:
        get_dep_level(line)

    print(json.dumps(DEPS_DEPTH, indent=4))
