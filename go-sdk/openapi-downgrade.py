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

import argparse
import json
import sys


def visit_paths(paths):
    for methods in paths.values():
        for spec in methods.values():
            for param in spec.get("parameters", []):
                visit_parameter(param)

            for resp in spec.get("responses", {}).values():
                for ct in resp.get("content", {}).values():
                    visit_parameter(ct)


def visit_components(components):
    for schema in components["schemas"].values():
        visit_schema(schema)


def visit_parameter(param):
    if schema := param.get("schema"):
        visit_schema(schema)


def visit_schema(schema):
    if (one_of := schema.get("oneOf")) and one_of[-1] == {"type": "null"}:
        one_of.pop()
        schema["nullable"] = True
        if len(one_of) == 1:
            del schema["oneOf"]
            schema.update(one_of.pop())
    elif schema.get("type") == "null":
        del schema["type"]
        schema["nullable"] = True

    for prop in schema.get("properties", {}).values():
        visit_schema(prop)


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", nargs="?", type=argparse.FileType("r"), default=sys.stdin)

    args = parser.parse_args(args)

    spec = json.load(args.infile)

    for kind, section in spec.items():
        if kind == "paths":
            visit_paths(section)
        elif kind == "components":
            visit_components(section)
    spec["openapi"] = "3.0.0"

    print(json.dumps(spec, indent=2))


if __name__ == "__main__":
    main()
