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
Make sure the format of the changelogs is correct
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def check_changelog_format(filename):
    with open(filename) as f:
        lines = [line.strip() for line in f if line.strip()]
    version_regex = re.compile(r"^\d+\.\d+\.\d+$")
    for i, line in enumerate(lines):
        if line == "....." and i + 1 < len(lines):
            if re.match(version_regex, lines[i + 1]):
                print(
                    f"{filename}:{i + 1} Invalid format in version is followed by version"
                )
                print(f"Check for lines with {lines[i - 1]} and {lines[i + 1]}")
                return False
    return True


if __name__ == "__main__":
    files = sys.argv[1:]
    failed = any(
        not check_changelog_format(filename)
        for filename in files
        if Path(filename).stem == "CHANGELOG"
    )

    if failed:
        sys.exit(1)
