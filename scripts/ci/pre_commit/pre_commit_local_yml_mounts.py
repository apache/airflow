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

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported

from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH  # isort: skip

sys.path.insert(0, str(AIRFLOW_SOURCES_ROOT_PATH))  # make sure setup is imported from Airflow
sys.path.insert(
    0, str(AIRFLOW_SOURCES_ROOT_PATH / "dev" / "breeze" / "src")
)  # make sure setup is imported from Airflow
# flake8: noqa: F401
from airflow_breeze.utils.docker_command_utils import VOLUMES_FOR_SELECTED_MOUNTS  # isort: skip

from common_precommit_utils import insert_documentation  # isort: skip

sys.path.append(str(AIRFLOW_SOURCES_ROOT_PATH))

MOUNTS_HEADER = (
    "        # START automatically generated volumes from "
    "VOLUMES_FOR_SELECTED_MOUNTS in docker_command_utils.py"
)
MOUNTS_FOOTER = (
    "        # END automatically generated volumes from "
    "VOLUMES_FOR_SELECTED_MOUNTS in docker_command_utils.py"
)

if __name__ == "__main__":
    local_mount_file_path = AIRFLOW_SOURCES_ROOT_PATH / "scripts" / "ci" / "docker-compose" / "local.yml"
    PREFIX = "      "
    volumes = []
    for (src, dest) in VOLUMES_FOR_SELECTED_MOUNTS:
        volumes.extend(
            [
                PREFIX + "- type: bind\n",
                PREFIX + f"  source: ../../../{src}\n",
                PREFIX + f"  target: {dest}\n",
            ]
        )
    insert_documentation(local_mount_file_path, volumes, MOUNTS_HEADER, MOUNTS_FOOTER)
