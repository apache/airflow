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

    os.environ["SKIP_BREEZE_SELF_UPGRADE_CHECK"] = "true"
    from common_precommit_utils import filter_out_providers_on_non_main_branch

    sys.path.insert(0, str(AIRFLOW_SOURCES / "dev" / "breeze" / "src"))
    from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION, MOUNT_SELECTED
    from airflow_breeze.params.shell_params import ShellParams
    from airflow_breeze.utils.console import get_console  # isort: skip
    from airflow_breeze.utils.docker_command_utils import get_extra_docker_flags  # isort: skip
    from airflow_breeze.utils.path_utils import create_mypy_volume_if_needed  # isort: skip
    from airflow_breeze.utils.run_utils import (
        get_ci_image_for_pre_commits,
        run_command,
    )

    shell_params = ShellParams(python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION, backend="none")

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
            *get_extra_docker_flags(mount_sources=MOUNT_SELECTED, include_mypy_volume=True),
            "-e",
            "SKIP_ENVIRONMENT_INITIALIZATION=true",
            "--pull",
            "never",
            airflow_image,
            "/opt/airflow/scripts/in_container/run_mypy.sh",
            *files_to_test,
        ],
        check=False,
        env=shell_params.env_variables_for_docker_commands,
    )
    if cmd_result.returncode != 0:
        upgrading = os.environ.get("UPGRADE_TO_NEWER_DEPENDENCIES", "false") != "false"
        if upgrading:
            get_console().print(
                "[warning]You are running mypy with the image that has dependencies upgraded automatically."
            )
        flag = " --upgrade-to-newer-dependencies" if upgrading else ""
        get_console().print(
            "[warning]If you see strange stacktraces above, "
            f"run `breeze ci-image build --python 3.8{flag}` and try again. "
            "You can also run `breeze down --cleanup-mypy-cache` to clean up the cache used. "
            "Still sometimes diff heuristic in mypy is behaving abnormal, to double check you can "
            "call `breeze static-checks --type mypy-[dev|core|providers|docs] --all-files` "
            'and then commit via `git commit --no-verify -m "commit message"`. CI will do a full check.'
        )
    sys.exit(cmd_result.returncode)
