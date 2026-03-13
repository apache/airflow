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

import logging
from typing import TYPE_CHECKING
from unittest.mock import patch

if TYPE_CHECKING:
    import pytest

from airflow.providers.fab.auth_manager.api_fastapi.parameters import get_effective_limit


class TestParametersDependency:
    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_effective_limit_uses_fallback_when_zero(self, conf_mock):
        conf_mock.getint.side_effect = lambda section, option: {
            "maximum_page_limit": 100,
            "fallback_page_limit": 25,
        }[option]

        limit_dep = get_effective_limit()

        assert limit_dep(limit=0) == 25
        conf_mock.getint.assert_any_call("api", "maximum_page_limit")
        conf_mock.getint.assert_any_call("api", "fallback_page_limit")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_effective_limit_clamps_to_max_and_logs_warning(
        self, conf_mock, caplog: pytest.LogCaptureFixture
    ):
        conf_mock.getint.side_effect = lambda section, option: {
            "maximum_page_limit": 50,
            "fallback_page_limit": 20,
        }[option]

        limit_dep = get_effective_limit()

        caplog.set_level(logging.WARNING, logger="airflow.providers.fab.auth_manager.api_fastapi.parameters")

        result = limit_dep(limit=1000)

        assert result == 50
        assert any("exceeds the configured maximum page limit" in rec.getMessage() for rec in caplog.records)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.parameters.conf")
    def test_get_effective_limit_returns_given_value_when_within_range(self, conf_mock):
        conf_mock.getint.side_effect = lambda section, option: {
            "maximum_page_limit": 200,
            "fallback_page_limit": 10,
        }[option]

        limit_dep = get_effective_limit()

        assert limit_dep(limit=150) == 150
