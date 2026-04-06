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
"""
Tests for environment context detection.

Tests the ability to detect execution environment (WSL, Docker, etc.)
and recommend appropriate commands for Breeze workflows.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.utils.environment_context import (
    EnvironmentContext,
    _detect_wsl,
    _is_docker_available,
    detect_environment_context,
)


class TestDetectWSL:
    """Tests for WSL detection."""

    def test_detect_wsl_via_env_var(self, monkeypatch):
        """Test WSL detection via WSL_DISTRO_NAME environment variable."""
        monkeypatch.setenv("WSL_DISTRO_NAME", "Ubuntu")
        assert _detect_wsl() is True

    def test_detect_wsl_not_present_env_var(self, monkeypatch):
        """Test when WSL_DISTRO_NAME is not set but on non-WSL system."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        # Mock all detection methods to return non-WSL values
        with patch("builtins.open", side_effect=OSError):  # /proc/version not available
            with patch("os.uname") as mock_uname:
                mock_uname.return_value.release = "5.10.16.3-generic"  # Non-microsoft kernel
                result = _detect_wsl()
                assert result is False

    def test_detect_wsl_via_proc_version(self, monkeypatch):
        """Test WSL detection via /proc/version."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        with patch("builtins.open", create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = (
                "Linux version 4.19.128-microsoft (Microsoft@microsoft.com) "
                "(gcc version 8.3.0 (GCC)) #1 SMP..."
            )
            assert _detect_wsl() is True

    def test_detect_wsl_via_uname_release(self, monkeypatch):
        """Test WSL detection via uname release."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        with patch("builtins.open", side_effect=OSError):
            with patch("os.uname") as mock_uname:
                mock_uname.return_value.release = "4.19.128-microsoft-standard"
                assert _detect_wsl() is True


class TestDockerAvailable:
    """Tests for Docker availability detection."""

    def test_docker_available_success(self):
        """Test when Docker is available and running."""
        with patch("subprocess.run") as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_run.return_value = mock_result
            
            assert _is_docker_available() is True
            mock_run.assert_called_once_with(
                ["docker", "info"],
                capture_output=True,
                timeout=5,
                check=False,
            )

    def test_docker_available_failure(self):
        """Test when Docker is not available."""
        with patch("subprocess.run") as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 1
            mock_run.return_value = mock_result
            
            assert _is_docker_available() is False

    def test_docker_not_installed(self):
        """Test when Docker is not installed."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            assert _is_docker_available() is False

    def test_docker_timeout(self):
        """Test when Docker check times out."""
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("docker", 5)):
            assert _is_docker_available() is False


class TestDetectEnvironmentContext:
    """Tests for the main environment context detection."""

    def test_detect_inside_container(self, monkeypatch):
        """Test detection when running inside Docker container."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        with patch("os.path.exists") as mock_exists:
            # Mock /.dockerenv exists (inside container)
            mock_exists.return_value = True
            
            with patch("airflow_breeze.utils.environment_context._detect_wsl") as mock_wsl:
                mock_wsl.return_value = False
                
                with patch("airflow_breeze.utils.environment_context._is_docker_available") as mock_docker:
                    mock_docker.return_value = True
                    
                    context = detect_environment_context()
                    
                    assert context.inside_container is True
                    assert context.is_wsl is False
                    assert context.docker_available is True
                    assert context.recommended_command == "pytest"

    def test_detect_host_linux(self, monkeypatch):
        """Test detection on host Linux system (not WSL)."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        with patch("os.path.exists", return_value=False):
            with patch("airflow_breeze.utils.environment_context._detect_wsl") as mock_wsl:
                mock_wsl.return_value = False
                
                with patch("airflow_breeze.utils.environment_context._is_docker_available") as mock_docker:
                    mock_docker.return_value = True
                    
                    context = detect_environment_context()
                    
                    assert context.inside_container is False
                    assert context.is_wsl is False
                    assert context.docker_available is True
                    assert context.recommended_command == "breeze shell"

    def test_detect_host_wsl(self, monkeypatch):
        """Test detection on Windows Subsystem for Linux."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        with patch("os.path.exists", return_value=False):
            with patch("airflow_breeze.utils.environment_context._detect_wsl") as mock_wsl:
                mock_wsl.return_value = True
                
                with patch("airflow_breeze.utils.environment_context._is_docker_available") as mock_docker:
                    mock_docker.return_value = True
                    
                    context = detect_environment_context()
                    
                    assert context.inside_container is False
                    assert context.is_wsl is True
                    assert context.docker_available is True
                    assert context.recommended_command == "breeze shell"

    def test_detect_host_docker_unavailable(self, monkeypatch):
        """Test detection when Docker is not available."""
        monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
        
        with patch("os.path.exists", return_value=False):
            with patch("airflow_breeze.utils.environment_context._detect_wsl") as mock_wsl:
                mock_wsl.return_value = False
                
                with patch("airflow_breeze.utils.environment_context._is_docker_available") as mock_docker:
                    mock_docker.return_value = False
                    
                    context = detect_environment_context()
                    
                    assert context.inside_container is False
                    assert context.docker_available is False
                    # Should still recommend breeze shell (even if Docker not available)
                    assert context.recommended_command == "breeze shell"


class TestEnvironmentContextDataclass:
    """Tests for the EnvironmentContext dataclass."""

    def test_environment_context_creation(self):
        """Test creating an EnvironmentContext."""
        context = EnvironmentContext(
            is_wsl=False,
            inside_container=True,
            docker_available=True,
            recommended_command="pytest",
        )
        
        assert context.is_wsl is False
        assert context.inside_container is True
        assert context.docker_available is True
        assert context.recommended_command == "pytest"

    def test_environment_context_all_fields(self):
        """Test EnvironmentContext has all expected fields."""
        context = EnvironmentContext(
            is_wsl=True,
            inside_container=False,
            docker_available=True,
            recommended_command="breeze shell",
        )
        
        # Verify all fields are accessible
        assert hasattr(context, "is_wsl")
        assert hasattr(context, "inside_container")
        assert hasattr(context, "docker_available")
        assert hasattr(context, "recommended_command")
