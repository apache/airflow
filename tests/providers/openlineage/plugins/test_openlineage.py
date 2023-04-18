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


class TestOpenLineageProviderPlugin:
    def setup_method(self):
        self.old_modules = dict(sys.modules)

    def teardown_method(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

    @pytest.mark.parametrize(
        "mocks, expected",
        [([patch.dict(os.environ, {"OPENLINEAGE_DISABLED": "true"}, 0)], 0), ([], 1)],
    )
    def test_plugin_disablements(self, mocks, expected):
        with contextlib.ExitStack() as stack:
            for mock in mocks:
                stack.enter_context(mock)
            from airflow.providers.openlineage.plugins.openlineage import OpenLineageProviderPlugin

            plugin = OpenLineageProviderPlugin()
            assert len(plugin.listeners) == expected
