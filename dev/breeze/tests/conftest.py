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
