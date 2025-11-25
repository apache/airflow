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

from pathlib import Path
from unittest import mock

import pytest

from airflow_breeze.utils.cache import (
    check_if_cache_exists,
    check_if_values_allowed,
    delete_cache,
    read_from_cache_file,
)

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()


@pytest.mark.parametrize(
    "parameter, value, result, exception",
    [
        ("backend", "mysql", (True, ["sqlite", "mysql", "postgres", "none"]), None),
        ("backend", "xxx", (False, ["sqlite", "mysql", "postgres", "none"]), None),
        ("python_major_minor_version", "3.9", (False, ["3.10", "3.11", "3.12"]), None),
        ("python_major_minor_version", "3.8", (False, ["3.10", "3.11", "3.12"]), None),
        ("python_major_minor_version", "3.7", (False, ["3.10", "3.11", "3.12"]), None),
        ("missing", "value", None, AttributeError),
    ],
)
def test_allowed_values(parameter, value, result, exception):
    if exception:
        with pytest.raises(expected_exception=exception):
            check_if_values_allowed(parameter, value)
    else:
        assert result == check_if_values_allowed(parameter, value)


@mock.patch("airflow_breeze.utils.cache.Path")
def test_check_if_cache_exists(path):
    check_if_cache_exists("test_param")
    path.assert_called_once_with(AIRFLOW_SOURCES / ".build")


@pytest.mark.parametrize(
    "param",
    [
        "test_param",
        "mysql_version",
        "executor",
    ],
)
def test_read_from_cache_file(param):
    param_value = read_from_cache_file(param.upper())
    if param_value is None:
        assert param_value is None
    else:
        allowed, param_list = check_if_values_allowed(param, param_value)
        if allowed:
            assert param_value in param_list


@mock.patch("airflow_breeze.utils.cache.Path")
@mock.patch("airflow_breeze.utils.cache.check_if_cache_exists")
def test_delete_cache_exists(mock_check_if_cache_exists, mock_path):
    param = "MYSQL_VERSION"
    mock_check_if_cache_exists.return_value = True
    cache_deleted = delete_cache(param)
    mock_path.assert_called_with(AIRFLOW_SOURCES / ".build")
    assert cache_deleted


@mock.patch("airflow_breeze.utils.cache.Path")
@mock.patch("airflow_breeze.utils.cache.check_if_cache_exists")
def test_delete_cache_not_exists(mock_check_if_cache_exists, mock_path):
    param = "TEST_PARAM"
    mock_check_if_cache_exists.return_value = False
    cache_deleted = delete_cache(param)
    assert not cache_deleted
