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

import hashlib
from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
BREEZE_SOURCES_ROOT = AIRFLOW_SOURCES_ROOT / "dev" / "breeze"


def get_package_setup_metadata_hash() -> str:
    """
    Retrieves hash of setup.py and setup.cfg files.

    This is used in order to determine if we need to upgrade Breeze, because some
    setup files changed. Blake2b algorithm will not be flagged by security checkers
    as insecure algorithm (in Python 3.9 and above we can use `usedforsecurity=False`
    to disable it, but for now it's better to use more secure algorithms.
    """
    try:
        the_hash = hashlib.new("blake2b")
        the_hash.update((BREEZE_SOURCES_ROOT / "setup.py").read_bytes())
        the_hash.update((BREEZE_SOURCES_ROOT / "setup.cfg").read_bytes())
        the_hash.update((BREEZE_SOURCES_ROOT / "pyproject.toml").read_bytes())
        return the_hash.hexdigest()
    except FileNotFoundError as e:
        return f"Missing file {e.filename}"


def process_breeze_readme():
    breeze_readme = BREEZE_SOURCES_ROOT / "README.md"
    lines = breeze_readme.read_text().splitlines(keepends=True)
    result_lines = []
    for line in lines:
        if line.startswith("Package config hash:"):
            line = f"Package config hash: {get_package_setup_metadata_hash()}\n"
        result_lines.append(line)
    breeze_readme.write_text("".join(result_lines))


if __name__ == "__main__":
    process_breeze_readme()
