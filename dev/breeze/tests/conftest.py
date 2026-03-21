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

import pytest

from airflow_breeze.utils.functools_cache import clear_all_cached_functions


def pytest_configure(config):
    import sys

    sys._called_from_test = True

    # Register custom marker for integration tests
    config.addinivalue_line(
        "markers",
        "integration_tests: mark test as integration test requiring external resources (SVN, network)",
    )


def pytest_collection_modifyitems(config, items):
    """
    Automatically skip integration tests unless explicitly requested via -m flag.

    This allows running `pytest` without any flags to run only unit tests,
    while `pytest -m integration` will run integration tests.
    """
    # Check if user explicitly requested integration tests via -m
    markexpr = config.getoption("-m", default="")
    if markexpr and "integration_tests" in markexpr:
        # User explicitly requested integration tests, don't skip
        return

    skip_integration = pytest.mark.skip(
        reason="Did not run. This is tntegration test. Please run with: pytest -m integration_tests"
    )

    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


def pytest_unconfigure(config):
    import sys  # This was missing from the manual

    del sys._called_from_test


@pytest.fixture(autouse=True)
def clear_clearable_cache():
    clear_all_cached_functions()


@pytest.fixture
def json_decode_error():
    """Provide a requests.exceptions.JSONDecodeError for mocking API failures."""
    import requests

    return requests.exceptions.JSONDecodeError("", "", 0)
