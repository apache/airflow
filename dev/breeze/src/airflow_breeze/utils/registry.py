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
import os
import subprocess
from typing import Optional, Tuple

from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.run_utils import run_command


def login_to_github_docker_registry(
    image_params: CommonBuildParams, output: Optional[Output], dry_run: bool, verbose: bool
) -> Tuple[int, str]:
    """
    In case of CI environment, we need to login to GitHub Registry.

    :param image_params: parameters to use for Building prod image
    :param output: Output to redirect to
    :param dry_run: whether we are in dry_run mode
    :param verbose: whether to show commands.
    """
    if os.environ.get("CI"):
        if len(image_params.github_token) == 0:
            get_console(output=output).print(
                "\n[info]Skip logging in to GitHub Registry. No Token available!"
            )
        elif len(image_params.github_token) > 0:
            run_command(
                ['docker', 'logout', 'ghcr.io'],
                dry_run=dry_run,
                verbose=verbose,
                stdout=output.file if output else None,
                stderr=subprocess.STDOUT,
                text=False,
                check=False,
            )
            command_result = run_command(
                [
                    'docker',
                    'login',
                    '--username',
                    image_params.github_username,
                    '--password-stdin',
                    'ghcr.io',
                ],
                verbose=verbose,
                stdout=output.file if output else None,
                stderr=subprocess.STDOUT,
                text=True,
                input=image_params.github_token,
                check=False,
            )
            return command_result.returncode, "Docker login"
        else:
            get_console().print('\n[info]Skip Login to GitHub Container Registry as token is missing')
    return 0, "Docker login skipped"
