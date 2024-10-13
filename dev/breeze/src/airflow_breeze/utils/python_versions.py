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

from airflow_breeze.global_constants import ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS
from airflow_breeze.utils.console import get_console


def get_python_version_list(python_versions: str) -> list[str]:
    """
    Retrieve and validate space-separated list of Python versions and return them in the form of list.
    :param python_versions: space separated list of Python versions
    :return: List of python versions
    """
    python_version_list = python_versions.split(" ")
    errors = False
    for python in python_version_list:
        if python not in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS:
            get_console().print(
                f"[error]The Python version {python} passed in {python_versions} is wrong.[/]"
            )
            errors = True
    if errors:
        get_console().print(
            f"\nSome of the Python versions passed are not in the "
            f"list: {ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS}. Quitting.\n"
        )
        sys.exit(1)
    return python_version_list


def check_python_version(release_provider_packages: bool = False):
    if not sys.version_info < (3, 12) and release_provider_packages:
        get_console().print("[error]Python 3.12 is not supported.\n")
        get_console().print(
            "[warning]Please reinstall Breeze using Python 3.9 - 3.11 environment because not all "
            "provider packages support Python 3.12 yet.[/]\n\n"
            "For example:\n\n"
            "pipx uninstall apache-airflow-breeze\n"
            "pipx install --python $(which python3.9) -e ./dev/breeze --force\n"
        )
        sys.exit(1)
