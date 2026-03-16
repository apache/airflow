# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ContextDetectionError(ValueError):
    """Raised when environment context cannot be reliably determined."""


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Runtime environment information for command resolution."""

    environment: str  # "host" | "breeze-container"

    def is_breeze(self) -> bool:
        """Check if running inside Breeze container."""
        return self.environment == "breeze-container"

    def is_host(self) -> bool:
        """Check if running on host machine."""
        return self.environment == "host"


def detect_context() -> ExecutionContext:
    """
    Detect current execution environment with priority checks.

    Priority:
    1. AIRFLOW_BREEZE_CONTAINER environment variable
    2. presence of /.dockerenv
    3. presence of /opt/airflow

    Returns:
        ExecutionContext indicating the detected environment.
    """
    # Priority 1: Check AIRFLOW_BREEZE_CONTAINER env variable
    if os.environ.get("AIRFLOW_BREEZE_CONTAINER") == "true":
        return ExecutionContext(environment="breeze-container")

    # Priority 2: Check for Docker environment marker
    if Path("/.dockerenv").exists():
        return ExecutionContext(environment="breeze-container")

    # Priority 3: Check for Airflow-specific path
    if Path("/opt/airflow").exists():
        return ExecutionContext(environment="breeze-container")

    # Default: assume host environment
    return ExecutionContext(environment="host")
