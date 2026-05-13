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

from airflow_breeze.commands.release_management_commands import (
    _is_initial_provider_release,
    _should_include_provider_in_issue,
)


@pytest.mark.parametrize(
    ("provider_yaml_dict", "expected"),
    [
        ({"versions": ["0.1.0"]}, True),
        ({"versions": ["2.1.0", "2.0.0"]}, False),
        ({"versions": []}, False),
        (None, False),
    ],
)
def test_is_initial_provider_release(provider_yaml_dict: dict | None, expected: bool):
    assert _is_initial_provider_release(provider_yaml_dict) is expected


@pytest.mark.parametrize(
    ("provider_yaml_dict", "prs_for_current_release", "prs_after_exclusions", "expected"),
    [
        ({"versions": ["0.1.0"]}, [12345], [12345], True),
        ({"versions": ["0.1.0"]}, [], [], True),
        ({"versions": ["0.2.0", "0.1.0"]}, [], [], False),
        ({"versions": ["0.1.0"]}, [12345], [], False),
    ],
)
def test_should_include_provider_in_issue(
    provider_yaml_dict: dict | None,
    prs_for_current_release: list[int],
    prs_after_exclusions: list[int],
    expected: bool,
):
    assert (
        _should_include_provider_in_issue(
            provider_yaml_dict=provider_yaml_dict,
            prs_for_current_release=prs_for_current_release,
            prs_after_exclusions=prs_after_exclusions,
        )
        is expected
    )
