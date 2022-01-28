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

from pathlib import Path
from unittest import mock

import pytest

from airflow_breeze.cache import check_if_cache_exists, check_if_values_allowed, read_from_cache_file

AIRFLOW_SOURCES = Path(__file__).parent.parent.parent.parent


@pytest.mark.parametrize(
    'parameter, value, result, exception',
    [
        ("backends", "mysql", (True, ['sqlite', 'mysql', 'postgres', 'mssql']), None),
        ("backends", "xxx", (False, ['sqlite', 'mysql', 'postgres', 'mssql']), None),
        ("python_major_minor_version", "3.8", (True, ['3.6', '3.7', '3.8', '3.9']), None),
        ("python_major_minor_version", "3.5", (False, ['3.6', '3.7', '3.8', '3.9']), None),
        ("missing", "value", None, AttributeError),
    ],
)
def test_allowed_values(parameter, value, result, exception):
    if exception:
        with pytest.raises(expected_exception=exception):
            check_if_values_allowed(parameter, value)
    else:
        assert result == check_if_values_allowed(parameter, value)


@mock.patch("airflow_breeze.cache.Path")
def test_check_if_cache_exists(path):
    check_if_cache_exists("test_param")
    path.assert_called_once_with(AIRFLOW_SOURCES / ".build")


@pytest.mark.parametrize(
    'param',
    [
        "test_param",
        "mysql_version",
        "executor",
    ],
)
def test_read_from_cache_file(param):
    param_value = read_from_cache_file(param.upper())
    if param_value is None:
        assert None is param_value
    else:
        allowed, param_list = check_if_values_allowed(param + 's', param_value)
        if allowed:
            assert param_value in param_list
