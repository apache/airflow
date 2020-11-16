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
from typing import List

AIRFLOW_SOURCES_DIR = os.path.abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir))

sys.path.insert(0, AIRFLOW_SOURCES_DIR)
# flake8: noqa: F401
# pylint: disable=wrong-import-position
from setup import EXTRAS_PROVIDERS_PACKAGES  # noqa

sys.path.append(AIRFLOW_SOURCES_DIR)


def get_provider_directory(provider: str):
    return os.path.join(AIRFLOW_SOURCES_DIR, "airflow", "providers", *provider.split('.'))


def check_all_providers() -> List[str]:
    errors: List[str] = []
    for extra, providers in EXTRAS_PROVIDERS_PACKAGES.items():
        for provider in providers:
            provider_directory = get_provider_directory(provider)
            if not os.path.isdir(provider_directory):
                errors.append(
                    f"The {extra} has provider {provider} that has missing {provider_directory} directory"
                )
                continue
            if not os.path.exists(os.path.join(provider_directory, "__init__.py")):
                errors.append(
                    f"The {extra} has provider {provider} that has"
                    f" missing __init__.py in the {provider_directory} directory"
                )
            if not os.path.exists(os.path.join(provider_directory, "README.md")):
                errors.append(
                    f"The {extra} has provider {provider} that has"
                    f" missing README.md in the {provider_directory} directory"
                )
    return errors


if __name__ == '__main__':
    errors = check_all_providers()
    if errors:
        for message in errors:
            print(message, file=sys.stderr)
        sys.exit(1)
