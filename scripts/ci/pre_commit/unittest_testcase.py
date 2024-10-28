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

import ast
import pathlib
import sys


def check_test_file(file: str) -> int:
    node = ast.parse(pathlib.Path(file).read_text("utf-8"), file)

    found = 0
    classes = [c for c in node.body if isinstance(c, ast.ClassDef)]
    known_classes = {"TestCase"}
    for c in classes:
        # Some classes are returned as an ast.Attribute, some as an ast.Name object. Not quite sure why
        if any(
            (isinstance(base, ast.Attribute) and base.attr in known_classes)
            or (isinstance(base, ast.Name) and base.id in known_classes)
            for base in c.bases
        ):
            found += 1
            prefix = f"{file}:{c.lineno}:"
            print(
                f"{prefix} The class {c.name!r} inherits from TestCase, please use pytest instead"
            )
            known_classes.add(
                c.name
            )  # Also use to found inherited classes in the same module
    return found


def main(*args: str) -> int:
    errors = sum(check_test_file(file) for file in args[1:])
    if not errors:
        return 0
    print(f"Found {errors} error{'s' if errors > 1 else ''}.")
    return 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv))
