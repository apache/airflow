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

from unittest import mock

import pytest

from airflow_breeze.utils.ci_group import ci_group


@pytest.fixture
def in_github_actions(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.delenv("SKIP_GROUP_OUTPUT", raising=False)


@pytest.mark.usefixtures("in_github_actions")
@mock.patch("airflow_breeze.utils.ci_group.console_print", autospec=True)
def test_ci_group_without_title_runs_body_in_github_actions(mock_console_print):
    body = mock.Mock()
    with ci_group("title", skip_printing_title=True):
        body()
    body.assert_called_once()
    mock_console_print.assert_not_called()
    assert not getattr(ci_group, "__in_ci_group__", False)
