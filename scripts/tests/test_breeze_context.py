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
from ci.prek.breeze_context import BreezeContext


class TestBreezeContextDetection:
    """Test context detection logic."""

    def test_breeze_detected_by_env_var(self):
        """Should detect Breeze when BREEZE env var is set."""
        with patch.dict(os.environ, {"BREEZE": "true"}):
            assert BreezeContext.is_in_breeze() is True

    def test_host_detected_by_default(self):
        """Should detect host environment when BREEZE env var is not set."""
        env = {k: v for k, v in os.environ.items() if k != "BREEZE"}
        with patch.dict(os.environ, env, clear=True):
            assert BreezeContext.is_in_breeze() is False

    def test_force_context_breeze(self):
        """Should return True when force_context is 'breeze'."""
        assert BreezeContext.is_in_breeze(force_context="breeze") is True

    def test_force_context_host(self):
        """Should return False when force_context is 'host'."""
        assert BreezeContext.is_in_breeze(force_context="host") is False

    def test_invalid_force_context_raises(self):
        """Should raise ValueError for invalid force_context."""
        with pytest.raises(ValueError, match="Invalid force_context"):
            BreezeContext.is_in_breeze(force_context="invalid")


class TestGetCommand:
    """Test command generation for different contexts."""

    def test_get_static_checks_command_on_host(self):
        """Should return prek command when on host."""
        env = {k: v for k, v in os.environ.items() if k != "BREEZE"}
        with patch.dict(os.environ, env, clear=True):
            cmd = BreezeContext.get_command("run-static-checks", from_ref="main")
            assert "prek" in cmd
            assert "--from-ref" in cmd
            assert "main" in cmd
            assert "--stage pre-commit" in cmd

    def test_get_unit_tests_command_on_host(self):
        """Should return uv pytest command when on host."""
        env = {k: v for k, v in os.environ.items() if k != "BREEZE"}
        with patch.dict(os.environ, env, clear=True):
            cmd = BreezeContext.get_command("run-unit-tests", project="airflow-core", test_path="tests/api/")
            assert "uv run" in cmd
            assert "pytest" in cmd

    def test_get_unit_tests_command_in_breeze(self):
        """Should return breeze run command when in Breeze."""
        with patch.dict(os.environ, {"BREEZE": "true"}):
            cmd = BreezeContext.get_command("run-unit-tests", project="airflow-core", test_path="tests/api/")
            assert "breeze run" in cmd
            assert "pytest" in cmd

    def test_missing_required_parameter_raises_error(self):
        """Should raise ValueError when required parameter missing."""
        env = {k: v for k, v in os.environ.items() if k != "BREEZE"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="test_path"):
                BreezeContext.get_command("run-unit-tests", project="airflow-core")

    def test_skill_not_found_raises_error(self):
        """Should raise ValueError when skill doesn't exist."""
        with pytest.raises(ValueError, match="Skill not found"):
            BreezeContext.get_command("nonexistent-skill")
