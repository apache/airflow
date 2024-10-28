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

import itertools
import os
import re

import pytest

REQUIRED_ENV_VARS = ("SYSTEM_TESTS_ENV_ID",)


@pytest.fixture
def provider_env_vars():
    """Override this fixture in provider's conftest.py"""
    return ()


@pytest.fixture(autouse=True)
def skip_if_env_var_not_set(provider_env_vars: list[str]) -> None:
    for env in itertools.chain(REQUIRED_ENV_VARS, provider_env_vars):
        if env not in os.environ:
            pytest.skip(f"Missing required environment variable {env}")
            return


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Add @pytest.mark.system(provider_name) for every system test."""
    rootdir = config.rootpath
    for item in items:
        rel_path = item.path.relative_to(rootdir)

        # Provider system tests
        match = re.match(".+/system/providers/([^/]+)", str(rel_path))
        if match:
            provider = match.group(1)
            item.add_marker(pytest.mark.system(provider))

        # Core system tests
        match = re.match(".+/system/[^/]+", str(rel_path))
        if match:
            item.add_marker(pytest.mark.system("core"))
