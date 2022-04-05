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
from typing import Any

from airflow_breeze.utils.console import console
from airflow_breeze.utils.run_utils import run_command


def login_to_docker_registry(image_params: Any):
    """
    In case of CI environment, we need to login to GitHub Registry if we want to prepare cache.
    This method logs in using the params specified.

    :param image_params: parameters to use for Building prod image
    """
    if os.environ.get("CI"):
        if len(image_params.github_token) == 0:
            console.print("\n[bright_blue]Skip logging in to GitHub Registry. No Token available!")
        elif image_params.airflow_login_to_github_registry != "true":
            console.print(
                "\n[bright_blue]Skip logging in to GitHub Registry.\
                    AIRFLOW_LOGIN_TO_GITHUB_REGISTRY is set as false"
            )
        elif len(image_params.github_token) > 0:
            run_command(['docker', 'logout', 'ghcr.io'], verbose=True, text=True)
            run_command(
                [
                    'docker',
                    'login',
                    '--username',
                    image_params.github_username,
                    '--password-stdin',
                    'ghcr.io',
                ],
                verbose=True,
                text=True,
                input=image_params.github_token,
            )
        else:
            console.print('\n[bright_blue]Skip Login to GitHub Container Registry as token is missing')
