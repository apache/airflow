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

from airflow.sdk.module_loading import is_valid_dotpath


class TestModuleLoading:
    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            pytest.param("valid_path", True, id="module_no_dots"),
            pytest.param("valid.dot.path", True, id="standard_dotpath"),
            pytest.param("package.sub_package.module", True, id="dotpath_with_underscores"),
            pytest.param("MyPackage.MyClass", True, id="mixed_case_path"),
            pytest.param("invalid..path", False, id="consecutive_dots_fails"),
            pytest.param(".invalid.path", False, id="leading_dot_fails"),
            pytest.param("invalid.path.", False, id="trailing_dot_fails"),
            pytest.param("1invalid.path", False, id="leading_number_fails"),
            pytest.param(42, False, id="not_a_string"),
        ],
    )
    def test_is_valid_dotpath(self, path, expected):
        assert is_valid_dotpath(path) == expected
