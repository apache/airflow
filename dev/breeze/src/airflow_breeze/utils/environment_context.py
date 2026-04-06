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
Environment context detection helper for Breeze AI workflows.

This module provides utilities to detect the execution environment (WSL, Docker, etc.)
and recommend appropriate commands for AI-assisted Breeze workflows.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass


@dataclass
class EnvironmentContext:
    """Information about the current execution environment."""

    is_wsl: bool
    """True if running under Windows Subsystem for Linux (WSL)."""

    inside_container: bool
    """True if running inside a Docker container."""

    docker_available: bool
    """True if Docker is available and accessible."""

    recommended_command: str
    """Recommended command for the current environment (e.g., 'breeze shell' or 'pytest')."""


def _detect_wsl() -> bool:
    """
    Detect whether we are running under Windows Subsystem for Linux (WSL).

    Uses multiple detection methods:
    1. Check WSL_DISTRO_NAME environment variable (WSL 2 specific)
    2. Check /proc/version for Microsoft/WSL markers (WSL 1 & 2)
    3. Check uname release for microsoft marker (WSL 2)

    Returns:
        True if running under WSL, False otherwise.
    """
    # Method 1: Environment variable (WSL 2 specific)
    if "WSL_DISTRO_NAME" in os.environ:
        return True

    # Method 2: Check /proc/version (WSL 1 & 2)
    try:
        with open("/proc/version", encoding="utf-8") as version_file:
            version = version_file.read()
        if "Microsoft" in version or "WSL" in version:
            return True
    except OSError:
        # /proc may not be available on non-Linux systems
        pass

    # Method 3: Check uname release (WSL 2)
    try:
        release = os.uname().release
        if "microsoft" in release.lower():
            return True
    except (AttributeError, OSError):
        pass

    return False


def _is_docker_available() -> bool:
    """
    Check if Docker is available and accessible.

    Runs 'docker info' to verify Docker daemon is running.

    Returns:
        True if Docker is available, False otherwise.
    """
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
            check=False,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def detect_environment_context() -> EnvironmentContext:
    """
    Detect the current execution environment and return context information.

    Detects:
    - Whether running in WSL
    - Whether running inside a Docker container
    - Whether Docker is available
    - Recommended command based on environment

    Returns:
        EnvironmentContext: Information about the current environment.

    Example:
        >>> context = detect_environment_context()
        >>> print(f"Running in container: {context.inside_container}")
        >>> print(f"Recommended: {context.recommended_command}")
    """
    # Check if inside Docker container
    inside_container = os.path.exists("/.dockerenv")

    # Detect WSL
    is_wsl_env = _detect_wsl()

    # Check Docker availability
    docker_available = _is_docker_available()

    # Determine recommended command
    if inside_container:
        # Inside container, use pytest directly
        recommended_command = "pytest"
    else:
        # Outside container, recommend entering Breeze
        recommended_command = "breeze shell"

    return EnvironmentContext(
        is_wsl=is_wsl_env,
        inside_container=inside_container,
        docker_available=docker_available,
        recommended_command=recommended_command,
    )
