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
import os
import sys
from unittest.mock import patch

import pytest
from tests_common import RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES
from tests_common.test_utils.config import conf_vars


@pytest.mark.skipif(
    RUNNING_TESTS_AGAINST_AIRFLOW_PACKAGES, reason="Plugin initialization is done early in case of packages"
)
class TestOpenLineageProviderPlugin:
    def setup_method(self):
        # Remove module under test if loaded already before. This lets us
        # import the same source files for more than one test.
        if "airflow.providers.openlineage.plugins.openlineage" in sys.modules:
            del sys.modules["airflow.providers.openlineage.plugins.openlineage"]

    @pytest.mark.parametrize(
        "mocks, expected",
        [
            # 0: not disabled but no configuration found
            ([], 0),
            # 0: env_var disabled = true
            ([patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "true"})], 0),
            # 0: env_var disabled = false but no configuration found
            ([patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "false"})], 0),
            # 0: conf disabled = true
            ([conf_vars({("openlineage", "disabled"): "True"})], 0),
            # 0: conf disabled = false but no configuration found
            ([conf_vars({("openlineage", "disabled"): "False"})], 0),
            # 0: env_var disabled = true and conf disabled = false
            (
                [
                    conf_vars({("openlineage", "disabled"): "F"}),
                    patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "True"}),
                ],
                0,
            ),
            # 0: env_var disabled = false and conf disabled = true
            (
                [
                    conf_vars({("openlineage", "disabled"): "T"}),
                    patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "False"}),
                ],
                0,
            ),
            # 0: env_var disabled = true and some config present
            (
                [
                    conf_vars(
                        {("openlineage", "transport"): '{"type": "http", "url": "http://localhost:5000"}'}
                    ),
                    patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "1"}),
                ],
                0,
            ),
            # 0: conf disabled = true and some config present
            (
                [
                    conf_vars(
                        {
                            ("openlineage", "transport"): '{"type": "http", "url": "http://localhost:5000"}',
                            ("openlineage", "disabled"): "true",
                        }
                    )
                ],
                0,
            ),
            # 1: conf disabled = false and some config present
            (
                [
                    conf_vars(
                        {
                            ("openlineage", "disabled"): "0",
                            ("openlineage", "transport"): '{"type": "http", "url": "http://localhost:5000"}',
                        }
                    )
                ],
                1,
            ),
            # 1: env_var disabled = false and some config present
            (
                [
                    conf_vars(
                        {
                            ("openlineage", "transport"): '{"type": "http", "url": "http://localhost:5000"}',
                        }
                    ),
                    patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "false"}),
                ],
                1,
            ),
            # 1: not explicitly disabled and url present
            ([patch.dict(os.environ, {"OPENLINEAGE_URL": "http://localhost:8080"})], 1),
            # 1: not explicitly disabled and transport present
            (
                [
                    conf_vars(
                        {("openlineage", "transport"): '{"type": "http", "url": "http://localhost:5000"}'}
                    )
                ],
                1,
            ),
        ],
    )
    def test_plugin_disablements(self, mocks, expected):
        with contextlib.ExitStack() as stack:
            for mock in mocks:
                stack.enter_context(mock)
            from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin

            plugin = OpenLineageProviderPlugin()

            assert len(plugin.listeners) == expected
