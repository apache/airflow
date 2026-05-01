#!/usr/bin/env python3
#
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

"""Additional edge-case tests for Breeze context."""

from __future__ import annotations

import os
from unittest.mock import patch

from ci.prek.breeze_context import BreezeContext


class TestEdgeCases:
    """Edge cases around context detection."""

    def test_context_detects_host_without_breeze_var(self) -> None:
        """Should default to host when BREEZE env var is not set."""
        env = {k: v for k, v in os.environ.items() if k != "BREEZE"}
        with patch.dict(os.environ, env, clear=True):
            assert BreezeContext.is_in_breeze() is False

    def test_context_detects_breeze_with_breeze_var(self) -> None:
        """Should detect Breeze when BREEZE env var is set."""
        with patch.dict(os.environ, {"BREEZE": "true"}):
            assert BreezeContext.is_in_breeze() is True

    def test_parameter_substitution_handles_quotes_spaces(self) -> None:
        """Command template substitution should preserve special characters via escaping."""
        import shlex

        test_path = "tests/unit/my file's test.py"
        cmd = BreezeContext.get_command("run-unit-tests", project="airflow-core", test_path=test_path)
        assert shlex.quote(test_path) in cmd
