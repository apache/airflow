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

import re

import pytest

from airflow_breeze.global_constants import GITHUB_REPO_BRANCH_PATTERN, PR_NUMBER_PATTERN


@pytest.mark.parametrize(
    ("value", "should_match_pr", "should_match_repo"),
    [
        pytest.param("57219", True, False, id="pr_number"),
        pytest.param("12345", True, False, id="another_pr_number"),
        pytest.param("jason810496/airflow:ci/breeze/compile-ui-assets", False, True, id="repo_branch"),
        pytest.param("apache/airflow:main", False, True, id="apache_repo"),
        pytest.param("2.7.3", False, False, id="version_number"),
        pytest.param("wheel", False, False, id="wheel"),
        pytest.param("sdist", False, False, id="sdist"),
        pytest.param("none", False, False, id="none"),
        pytest.param("57219abc", False, False, id="pr_with_chars"),
        pytest.param("abc57219", False, False, id="chars_with_number"),
    ],
)
def test_pr_number_pattern(value, should_match_pr, should_match_repo):
    """Test that PR number pattern correctly matches PR numbers only."""
    pr_match = re.match(PR_NUMBER_PATTERN, value)
    repo_match = re.match(GITHUB_REPO_BRANCH_PATTERN, value)

    if should_match_pr:
        assert pr_match is not None, f"Expected {value} to match PR pattern"
    else:
        assert pr_match is None, f"Expected {value} to NOT match PR pattern"

    if should_match_repo:
        assert repo_match is not None, f"Expected {value} to match repo pattern"
    else:
        assert repo_match is None, f"Expected {value} to NOT match repo pattern"
