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

from ci.prek.breeze_context import BreezieContext


class TestEdgeCases:
    """Edge cases around context detection."""

    def test_context_detects_ssh_host(self) -> None:
        """Should default to host when Breeze markers are absent even over SSH."""
        with patch.dict(os.environ, {"SSH_CONNECTION": "1"}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is False

    def test_context_detects_symlink_marker(self) -> None:
        """If dockerenv marker exists (even via symlink), detect Breeze."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists") as mock_exists:
                mock_exists.return_value = True
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is True

    def test_parameter_substitution_handles_quotes_spaces(self) -> None:
        """Command template substitution should preserve special characters."""
        test_path = "tests/unit/my file's test.py"
        cmd = BreezieContext.get_command("run-unit-tests", test_path=test_path)
        assert test_path in cmd
