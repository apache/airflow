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
"""Require a rationale comment directly above provider compatibility exceptions."""

from __future__ import annotations

import sys
from collections.abc import Iterable
from pathlib import Path

from common_prek_utils import get_all_provider_yaml_files

EXCEPTION_KEYS = ("min-python-version", "excluded-python-versions", "excluded-platforms")


def find_missing_exception_rationales(provider_yaml_path: Path) -> list[str]:
    """Return exception keys without a contiguous comment block directly above them."""
    lines = provider_yaml_path.read_text().splitlines()
    return [
        key
        for index, line in enumerate(lines)
        for key in EXCEPTION_KEYS
        if line.startswith(f"{key}:") and (index == 0 or not lines[index - 1].startswith("#"))
    ]


def main(provider_yaml_paths: Iterable[Path] | None = None) -> int:
    errors = [
        f"{path}: {key} requires a comment block directly above the key explaining why."
        for path in provider_yaml_paths or get_all_provider_yaml_files()
        for key in find_missing_exception_rationales(path)
    ]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
