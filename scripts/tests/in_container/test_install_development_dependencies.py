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
from install_development_dependencies import get_providers_devel_dependencies

PROVIDERS_DEPENDENCIES = {
    "common.ai": {"devel-deps": ["langchain>=1.0.0"], "excluded-python-versions": ["3.15"]},
    "example": {"devel-deps": ["example-dep>=1.0"], "excluded-python-versions": ["3.10.0"]},
    "mysql": {"devel-deps": ["mysql-dep>=2.0"], "excluded-python-versions": []},
    "legacy": {"devel-deps": ["legacy-dep"]},
}


@pytest.mark.parametrize(
    ("python_version", "expected"),
    [
        pytest.param(
            "3.15.0",
            ["example-dep>=1.0", "mysql-dep>=2.0", "legacy-dep"],
            id="minor-exclusion",
        ),
        pytest.param(
            "3.10.0",
            ["langchain>=1.0.0", "mysql-dep>=2.0", "legacy-dep"],
            id="patch-exclusion",
        ),
        pytest.param(
            "3.10.1",
            ["langchain>=1.0.0", "example-dep>=1.0", "mysql-dep>=2.0", "legacy-dep"],
            id="adjacent-patch-kept",
        ),
        pytest.param(
            "3.1.5",
            ["langchain>=1.0.0", "example-dep>=1.0", "mysql-dep>=2.0", "legacy-dep"],
            id="prefix-is-not-a-match",
        ),
    ],
)
def test_get_providers_devel_dependencies_skips_excluded_providers(python_version, expected):
    assert get_providers_devel_dependencies(PROVIDERS_DEPENDENCIES, python_version) == expected
