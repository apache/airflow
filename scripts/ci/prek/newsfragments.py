#!/usr/bin/env python
#
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
Check things about newsfragments:
  - Only a single line, except for `significant` changes which can have a blank line, then the body
"""

from __future__ import annotations

import sys
from pathlib import Path

VALID_CHANGE_TYPES = {"significant", "feature", "improvement", "bugfix", "doc", "misc"}


def validate_newsfragment(filename: str, lines: list[str]) -> list[str]:
    """Validate a single newsfragment file. Returns a list of error messages."""
    errors: list[str] = []
    num_lines = len(lines)

    name_parts = Path(filename).name.split(".")
    if len(name_parts) != 3:
        errors.append(
            f"Newsfragment {filename} has an unexpected filename. Should be {{pr_number}}.{{type}}.rst."
        )
        return errors

    change_type = name_parts[1]
    if change_type not in VALID_CHANGE_TYPES:
        errors.append(
            f"Newsfragment {filename} has an unexpected type. Should be one of {VALID_CHANGE_TYPES}."
        )
        return errors

    if change_type != "significant":
        if num_lines != 1:
            errors.append(f"Newsfragment {filename} can only have a single line.")
    else:
        # significant newsfragment
        if num_lines == 1:
            pass  # OK
        elif num_lines == 2:
            errors.append(f"Newsfragment {filename} can have 1, or 3+ lines.")
        elif lines[1] != "":
            errors.append(f"Newsfragment {filename} must have an empty second line.")

    return errors


def main(filenames: list[str]) -> int:
    failed = False
    for filename in filenames:
        with open(filename) as f:
            lines = [line.strip() for line in f.readlines()]
        errors = validate_newsfragment(filename, lines)
        for error in errors:
            print(error)
        if errors:
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
