"""Unit tests for Informatica provider plugin."""

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

import contextlib
import sys

import pytest

from tests_common import RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES
from tests_common.test_utils.config import conf_vars


@pytest.mark.skipif(
    RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES, reason="Plugin initialization is done early in case of packages"
)
class TestInformaticaProviderPlugin:
    def setup_method(self):
        # Remove module under test if loaded already before. This lets us
        # import the same source files for more than one test.
        if "airflow.providers.informatica.plugins.informatica" in sys.modules:
            del sys.modules["airflow.providers.informatica.plugins.informatica"]

    @pytest.mark.parametrize(
        ("mocks", "expected"),
        [
            # 1: not disabled by default
            ([], 1),
            # 0: conf disabled = true
            ([conf_vars({("informatica", "disabled"): "True"})], 0),
            # 0: conf disabled = 1
            ([conf_vars({("informatica", "disabled"): "1"})], 0),
            # 1: conf disabled = false
            ([conf_vars({("informatica", "disabled"): "False"})], 1),
            # 1: conf disabled = 0
            ([conf_vars({("informatica", "disabled"): "0"})], 1),
        ],
    )
    def test_plugin_disablements(self, mocks, expected):
        with contextlib.ExitStack() as stack:
            for mock in mocks:
                stack.enter_context(mock)
            from airflow.providers.informatica.plugins.informatica import InformaticaProviderPlugin

            plugin = InformaticaProviderPlugin()
            assert len(plugin.listeners) == expected
