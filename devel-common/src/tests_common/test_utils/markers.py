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
import sys

import pytest

skip_if_force_lowest_dependencies_marker = pytest.mark.skipif(
    os.environ.get("FORCE_LOWEST_DEPENDENCIES", "") == "true",
    reason="When lowest dependencies are set only some providers are loaded",
)


skip_if_not_on_main = pytest.mark.skipif(
    os.environ.get("DEFAULT_BRANCH", "main") != "main",
    reason="This test is only run on main branch in CI",
)


def xfail_on_python_315(reason: str):
    """
    Strictly expect a failure on Python 3.15, where some providers and their dependencies are not installed.

    Strict, so the test must be unmarked as soon as the missing provider is installed again.
    """
    return pytest.mark.xfail(sys.version_info >= (3, 15), reason=reason, strict=True)


def skip_on_python_315_without(*modules: str):
    """
    Skip the decorated test on Python 3.15 while one of the given modules is not installed.

    Prefer this over the module-level variant when only some tests need the missing dependency:
    skipping the module takes the rest of the file's coverage with it.
    """
    if sys.version_info < (3, 15):
        return pytest.mark.skipif(False, reason="")
    missing = [module for module in modules if not _is_installed(module)]
    return pytest.mark.skipif(bool(missing), reason=f"{', '.join(missing)} is not installed on Python 3.15")


def _is_installed(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def skip_module_on_python_315_without(*modules: str) -> None:
    """
    Skip the calling test module on Python 3.15 while one of the given modules is not installed.

    A provider excluded on 3.15 makes the module under test fail at import, which an ``xfail`` cannot cover.
    Call this before the failing import; the tests run again once the module is installed.
    """
    if sys.version_info < (3, 15):
        return
    for module in modules:
        pytest.importorskip(module, reason=f"{module} is not installed on Python 3.15")
