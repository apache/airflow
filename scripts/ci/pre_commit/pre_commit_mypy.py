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
    sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
    from common_precommit_utils import filter_out_providers_on_non_main_branch

    sys.path.insert(0, str(AIRFLOW_SOURCES / "dev" / "breeze" / "src"))
    from airflow_breeze.global_constants import MOUNT_SELECTED
    from airflow_breeze.utils.console import get_console
    from airflow_breeze.utils.docker_command_utils import get_extra_docker_flags
    from airflow_breeze.utils.path_utils import create_mypy_volume_if_needed
    from airflow_breeze.utils.run_utils import get_ci_image_for_pre_commits, run_command

    files_to_test = filter_out_providers_on_non_main_branch(sys.argv[1:])
    if files_to_test == ["--namespace-packages"]:
        print("No files to tests. Quitting")
        sys.exit(0)
    airflow_image = get_ci_image_for_pre_commits()
    create_mypy_volume_if_needed()
    cmd_result = run_command(
        [
            "docker",
            "run",
            "-t",
            *get_extra_docker_flags(MOUNT_SELECTED, include_mypy_volume=True),
            "-e",
            "SKIP_ENVIRONMENT_INITIALIZATION=true",
            "-e",
            "BACKEND=sqlite",
            "--pull",
            "never",
            airflow_image,
            "/opt/airflow/scripts/in_container/run_mypy.sh",
            *files_to_test,
        ],
        check=False,
    )
    if cmd_result.returncode != 0:
        get_console().print(
            "[warning]If you see strange stacktraces above, "
            "run `breeze ci-image build --python 3.7` and try again."
        )
    sys.exit(cmd_result.returncode)
