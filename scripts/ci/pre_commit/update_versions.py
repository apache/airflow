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

import re
import sys
from pathlib import Path

sys.path.insert(
    0, str(Path(__file__).parent.resolve())
)  # make sure common_precommit_utils is importable

from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH, read_airflow_version


def update_version(pattern: re.Pattern, v: str, file_path: Path):
    print(f"Checking {pattern} in {file_path}")
    with file_path.open("r+") as f:
        file_content = f.read()
        if not pattern.search(file_content):
            raise RuntimeError(f"Pattern {pattern!r} doesn't found in {file_path!r} file")
        new_content = pattern.sub(rf"\g<1>{v}\g<2>", file_content)
        if file_content == new_content:
            return
        print("    Updated.")
        f.seek(0)
        f.truncate()
        f.write(new_content)


REPLACEMENTS = {
    r"^(FROM apache\/airflow:).*($)": "docs/docker-stack/docker-examples/extending/*/Dockerfile",
    r"(apache\/airflow:)[^-]*(\-)": "docs/docker-stack/entrypoint.rst",
    r"(`apache/airflow:(?:slim-)?)[0-9].*?((?:-pythonX.Y)?`)": "docs/docker-stack/README.md",
    r"(\(Assuming Airflow version `).*(`\))": "docs/docker-stack/README.md",
}


if __name__ == "__main__":
    version = read_airflow_version()
    print(f"Current version: {version}")
    for regexp, p in REPLACEMENTS.items():
        text_pattern = re.compile(regexp, flags=re.MULTILINE)
        files = list(AIRFLOW_SOURCES_ROOT_PATH.glob(p))
        if not files:
            print(f"ERROR! No files matched on {p}")
        for file in files:
            update_version(text_pattern, version, file)
