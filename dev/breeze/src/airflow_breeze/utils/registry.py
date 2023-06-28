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

from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.run_utils import run_command


def login_to_github_docker_registry(github_token: str | None, output: Output | None) -> tuple[int, str]:
    """
    In case of CI environment, we need to login to GitHub Registry.

    :param github_token: Github token to use
    :param output: Output to redirect to

    :return  tuple of error code and message
    """
    if os.environ.get("CI"):
        if not github_token:
            get_console(output=output).print(
                "\n[info]Skip logging in to GitHub Registry. No Token available!"
            )
            return 0, "Docker login skipped as no token available"
        run_command(
            ["docker", "logout", "ghcr.io"],
            output=output,
            text=False,
            check=False,
        )
        command_result = run_command(
            [
                "docker",
                "login",
                "ghcr.io",
                "--username",
                "$",
                "--password-stdin",
            ],
            output=output,
            text=True,
            input=github_token,
            check=False,
        )
        return command_result.returncode, "Docker login"
    return 0, "Docker login skipped"
