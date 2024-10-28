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

import sys

import pytest

import airflow
from airflow.exceptions import AirflowException

TEST_CASES = {
    "PY36": sys.version_info >= (3, 6),
    "PY37": sys.version_info >= (3, 7),
    "PY38": sys.version_info >= (3, 8),
    "PY39": sys.version_info >= (3, 9),
    "PY310": sys.version_info >= (3, 10),
    "PY311": sys.version_info >= (3, 11),
    "PY312": sys.version_info >= (3, 12),
}


@pytest.mark.parametrize("py_attr, expected", TEST_CASES.items())
def test_lazy_load_py_versions(py_attr, expected):
    with pytest.warns(
        DeprecationWarning, match=f"Python version constraint '{py_attr}' is deprecated"
    ):
        # If there is no warning, then most possible it imported somewhere else.
        assert getattr(airflow, py_attr) is expected


@pytest.mark.parametrize("py_attr", ["PY35", "PY313"])
def test_wrong_py_version(py_attr):
    with pytest.raises(AttributeError, match=f"'airflow' has no attribute '{py_attr}'"):
        getattr(airflow, py_attr)


def test_deprecated_exception():
    warning_pattern = (
        "Import 'AirflowException' directly from the airflow module is deprecated"
    )
    with pytest.warns(DeprecationWarning, match=warning_pattern):
        # If there is no warning, then most possible it imported somewhere else.
        assert getattr(airflow, "AirflowException") is AirflowException
