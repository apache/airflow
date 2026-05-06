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
import sys
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[4]
    / "scripts"
    / "ci"
    / "testing"
    / "get_min_airflow_version_for_python.py"
)


@pytest.fixture
def min_airflow_module():
    module_name = "test_get_min_airflow_version_for_python_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("python_version", "expected_airflow_version"),
    [
        ("3.10", "2.11.0"),
        ("3.12", "2.11.0"),
        ("3.13", "3.1.0"),
        ("3.14", "3.2.0"),
        ("3.15", "3.2.0"),
    ],
)
def test_get_min_airflow_version_for_python_is_monotonic(
    min_airflow_module, python_version, expected_airflow_version
):
    assert min_airflow_module.get_min_airflow_version_for_python(python_version) == expected_airflow_version


def test_get_min_airflow_version_for_python_raises_for_older_python(min_airflow_module):
    with pytest.raises(ValueError, match="No minimum Airflow version defined for Python 3.9"):
        min_airflow_module.get_min_airflow_version_for_python("3.9")
