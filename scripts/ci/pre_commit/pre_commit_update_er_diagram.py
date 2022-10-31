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

import os
import sys
from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()
GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "apache/airflow")
os.environ["SKIP_GROUP_OUTPUT"] = "true"

if __name__ == "__main__":
    sys.path.insert(0, str(AIRFLOW_SOURCES / "dev" / "breeze" / "src"))
    from airflow_breeze.global_constants import MOUNT_SELECTED
    from airflow_breeze.utils.docker_command_utils import (
        get_extra_docker_flags,
        update_expected_environment_variables,
    )
    from airflow_breeze.utils.run_utils import get_ci_image_for_pre_commits, run_command

    env = os.environ.copy()
    env["DB_RESET"] = "true"
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////root/airflow/airflow.db"
    update_expected_environment_variables(env)
    airflow_image = get_ci_image_for_pre_commits()
    cmd_result = run_command(
        [
            "docker",
            "run",
            "-t",
            *get_extra_docker_flags(MOUNT_SELECTED),
            "-e",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "--pull",
            "never",
            airflow_image,
            "-c",
            "python3 /opt/airflow/scripts/in_container/run_prepare_er_diagram.py",
        ],
        env=env,
        check=False,
    )
    sys.exit(cmd_result.returncode)
