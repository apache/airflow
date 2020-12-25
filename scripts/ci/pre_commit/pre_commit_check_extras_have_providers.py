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

# Check if the extras have providers defined.
import os
import sys
from os.path import dirname
from pathlib import Path
from typing import List

from rich import print

AIRFLOW_SOURCES_DIR = os.path.abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir))

sys.path.insert(0, AIRFLOW_SOURCES_DIR)
# flake8: noqa: F401
# pylint: disable=wrong-import-position
from setup import ALL_PROVIDERS  # noqa

sys.path.append(AIRFLOW_SOURCES_DIR)

errors: List[str] = []

PROVIDERS_DIR = os.path.join(AIRFLOW_SOURCES_DIR, "airflow", "providers")


def get_provider_directory(provider: str) -> str:
    """Returns provider directory derived from name"""
    return os.path.join(PROVIDERS_DIR, *provider.split('.'))


def check_all_providers_listed_have_directory() -> None:
    for provider in ALL_PROVIDERS:
        provider_directory = get_provider_directory(provider)
        if not os.path.isdir(provider_directory):
            errors.append(
                f"The provider {provider} is defined in setup.py: [bold]PROVIDERS_REQUIREMENTS[/] but it "
                + f"has missing {provider_directory} directory: [red]NOK[/]"
            )
            continue
        if not os.path.exists(os.path.join(provider_directory, "__init__.py")):
            errors.append(
                f"The {provider} does not have the __init__.py "
                + f"file in the {provider_directory} directory [red]NOK[/]"
            )
        if not os.path.exists(os.path.join(provider_directory, "provider.yaml")):
            errors.append(
                f"The provider {provider} does not have the provider.yaml "
                + f"in the {provider_directory} directory: [red]NOK[/]"
            )


def check_all_providers_are_listed_in_setup_py() -> None:
    for path in Path(PROVIDERS_DIR).rglob('provider.yaml'):
        provider_name = str(path.parent.relative_to(PROVIDERS_DIR)).replace(os.sep, ".")
        if provider_name not in ALL_PROVIDERS:
            errors.append(
                f"The provider {provider_name} is missing in setup.py "
                + "[bold]PROVIDERS_REQUIREMENTS[/]: [red]NOK[/]"
            )


if __name__ == '__main__':
    check_all_providers_listed_have_directory()
    check_all_providers_are_listed_in_setup_py()
    if errors:
        for message in errors:
            print(message, file=sys.stderr)
        sys.exit(1)
    else:
        print("All providers are correctly defined in setup.py [green]OK[/]")
