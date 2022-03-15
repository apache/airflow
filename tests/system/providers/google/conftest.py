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
from pathlib import Path

import pytest

PROVIDER = Path(__file__).parent.name
REQUIRED_ENV = ("SYSTEM_TESTS_GCP_PROJECT",)


def is_env_not_ready():
    """Check if all required environments are present. If  return error message to be used in skip marker"""
    for env in REQUIRED_ENV:
        if env not in os.environ:
            return f"Missing required environment variable {env}"
    return ""


def is_system_mark_present(item):
    """Don't mark with missing env variables if system mark is missing"""
    selected_systems = item.config.getoption("--system")
    return selected_systems and (PROVIDER in selected_systems or "all" in selected_systems)


def pytest_collection_modifyitems(config, items):
    """
    Skip tests if environment is not configured
    """
    not_ready = is_env_not_ready()
    if not not_ready:
        return
    skip = pytest.mark.skip(reason=not_ready)
    for item in items:
        system_mark_present = is_system_mark_present(item)
        if system_mark_present and not_ready:
            item.add_marker(skip)
