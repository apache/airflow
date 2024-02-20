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

from unittest.mock import patch

import pytest

from airflow.utils.env_versions import _get_lib_major_version, is_sqlalchemy_v1


@pytest.mark.parametrize(
    "version_string, expected_major_version",
    [
        ("1.4.22", 1),  # Test 1: "1.4.22" parsed as 1
        ("10.4.22", 10),  # Test 2: "10.4.22" not parsed as 1
        ("invalid", None),  # Test 3: Invalid version string
        ("3.x.x", None),  # Test 4: Malformed version
    ],
)
def test_get_lib_major_version(version_string, expected_major_version):
    with patch("airflow.utils.env_versions.version") as mock_version:
        mock_version.return_value = version_string
        if expected_major_version is not None:
            assert _get_lib_major_version("dummy_module") == expected_major_version
        else:
            with pytest.raises(ValueError):
                _get_lib_major_version("dummy_module")


@pytest.mark.parametrize(
    "major_version, expected_result",
    [
        (1, True),  # Test 1: v1 identified as v1
        (2, False),  # Test 2: v2 not identified as v1
    ],
)
def test_is_sqlalchemy_v1(major_version, expected_result):
    with patch("airflow.utils.env_versions._get_lib_major_version") as mock_get_major_version:
        mock_get_major_version.return_value = major_version
        assert is_sqlalchemy_v1() == expected_result
