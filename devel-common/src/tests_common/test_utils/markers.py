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

import importlib.util
import os

import pytest

skip_if_force_lowest_dependencies_marker = pytest.mark.skipif(
    os.environ.get("FORCE_LOWEST_DEPENDENCIES", "") == "true",
    reason="When lowest dependencies are set only some providers are loaded",
)


skip_if_not_on_main = pytest.mark.skipif(
    os.environ.get("DEFAULT_BRANCH", "main") != "main",
    reason="This test is only run on main branch in CI",
)


def skip_if_not_installed(*modules: str):
    """
    Skip the decorated test while one of the given modules is not installed.

    For integration tests with a distribution the tested one does not depend on, such as a provider that
    lowest-dependency runs leave out.
    """
    missing = [module for module in modules if not _is_installed(module)]
    return pytest.mark.skipif(bool(missing), reason=f"{', '.join(missing)} is not installed")


def _is_installed(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False
