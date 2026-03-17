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

"""Tests for breeze_context.py - Context detection module."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from scripts.ci.prek.breeze_context import BreezieContext


class TestBreezieContextDetection:
    """Test context detection logic."""

    def test_breeze_detected_by_env_var(self):
        """Should detect Breeze when BREEZE_HOME env var is set."""
        with patch.dict(os.environ, {"BREEZE_HOME": "/opt/airflow"}):
            assert BreezieContext.is_in_breeze() is True

    def test_host_detected_by_default(self):
        """Should detect host environment when no markers present."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is False

    def test_breeze_detected_by_dockerenv(self):
        """Should detect Breeze when /.dockerenv exists."""
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            assert BreezieContext.is_in_breeze() is True

    def test_breeze_detected_by_opt_airflow(self):
        """Should detect Breeze when /opt/airflow directory exists."""
        with patch("os.path.exists", return_value=False):
            with patch("os.path.isdir") as mock_isdir:
                mock_isdir.return_value = True
                assert BreezieContext.is_in_breeze() is True


class TestGetCommand:
    """Test command generation for different contexts."""

    def test_get_static_checks_command_on_host(self):
        """Should return prek command when on host."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    cmd = BreezieContext.get_command("run-static-checks", module="airflow/api")
                    assert "prek" in cmd
                    assert "airflow/api" in cmd

    def test_get_unit_tests_command_on_host(self):
        """Should return uv pytest command when on host."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    cmd = BreezieContext.get_command("run-unit-tests", test_path="tests/api/")
                    assert "uv run" in cmd
                    assert "pytest" in cmd

    def test_get_unit_tests_command_in_breeze(self):
        """Should return breeze exec command when in Breeze."""
        with patch.dict(os.environ, {"BREEZE_HOME": "/opt/airflow"}):
            cmd = BreezieContext.get_command("run-unit-tests", test_path="tests/api/")
            assert "breeze exec" in cmd
            assert "pytest" in cmd

    def test_missing_required_parameter_raises_error(self):
        """Should raise ValueError when required parameter missing."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    with pytest.raises(ValueError, match="test_path"):
                        BreezieContext.get_command("run-unit-tests")

    def test_skill_not_found_raises_error(self):
        """Should raise ValueError when skill doesn't exist."""
        with pytest.raises(ValueError, match="Skill not found"):
            BreezieContext.get_command("nonexistent-skill")
