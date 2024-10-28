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
Test for an order of dependencies in setup.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from rich import print

errors: list[str] = []

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
HATCH_BUILD_PATH = AIRFLOW_ROOT_PATH / "hatch_build.py"

sys.path.insert(
    0, str(Path(__file__).parent.resolve())
)  # make sure common_precommit_utils is imported
from common_precommit_utils import check_list_sorted

sys.path.insert(0, str(AIRFLOW_ROOT_PATH))  # make sure airflow root is imported
from hatch_build import ALL_DYNAMIC_EXTRA_DICTS

if __name__ == "__main__":
    file_contents = HATCH_BUILD_PATH.read_text()

    for extra_dict, description in ALL_DYNAMIC_EXTRA_DICTS:
        for extra, extra_list in extra_dict.items():
            check_list_sorted(
                extra_list, f"Order of extra: {description}:{extra}", errors
            )
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
