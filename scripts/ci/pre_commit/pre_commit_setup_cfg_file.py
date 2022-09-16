#!/usr/bin/env python3
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

import os
import sys
from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )


AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()
SETUP_CFG_PATH = AIRFLOW_SOURCES / "setup.cfg"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
sys.path.insert(0, str(AIRFLOW_SOURCES))  # make sure setup is imported from Airflow
# flake8: noqa: F401

from common_precommit_utils import insert_documentation  # isort: skip


def stable_sort(x):
    return x.casefold(), x


if __name__ == '__main__':
    all_licenses = []
    for license in Path(AIRFLOW_SOURCES / "licenses").rglob("LICENSE*"):
        if license.name != "LICENSES-ui.txt":
            all_licenses.append(os.fspath(license.relative_to(AIRFLOW_SOURCES)).replace("\\", "/"))
    sorted_licenses = sorted(all_licenses, key=stable_sort)
    print(sorted_licenses)
    insert_documentation(
        SETUP_CFG_PATH,
        ["   " + license + "\n" for license in sorted_licenses],
        header="# Start of licenses generated automatically by update-setup-cfg-file pre-commit",
        footer="# End of licences generated automatically by update-setup-cfg-file pre-commit",
    )
