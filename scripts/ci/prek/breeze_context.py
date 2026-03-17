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

"""Context detection for Breeze environment."""

from __future__ import annotations

import os


class BreezieContext:
    """Detects whether code is running in Breeze container or on host."""

    @staticmethod
    def is_in_breeze() -> bool:
        """
        Detect if running inside Breeze container or on host.

        Priority order:
        1. BREEZE_HOME environment variable (explicit)
        2. /.dockerenv file (Docker container marker)
        3. /opt/airflow directory (Breeze standard path)
        4. Default: assume host

        Returns:
            True if in Breeze container, False if on host
        """
        # Check BREEZE_HOME env var (explicit marker)
        if os.environ.get("BREEZE_HOME"):
            return True

        # Check /.dockerenv (Docker container marker)
        if os.path.exists("/.dockerenv"):
            return True

        # Check /opt/airflow (Breeze standard installation path)
        if os.path.isdir("/opt/airflow"):
            return True

        # Default: assume host
        return False

    @staticmethod
    def get_command(skill_id: str, **kwargs) -> str:
        """
        Get the appropriate command for the current context.

        Args:
            skill_id: Unique skill identifier (e.g., "run-unit-tests")
            **kwargs: Skill-specific parameters

        Returns:
            Command string to execute

        Raises:
            ValueError: If skill_id not found or required parameter missing
        """
        is_breeze = BreezieContext.is_in_breeze()

        # Define all available skills
        skills = {
            "run-static-checks": {
                "host": "prek run {module}",
                "breeze": "python -m pytest --doctest-modules {module}",
                "preferred": "host",
                "params": {"module": {"required": False}},
            },
            "run-unit-tests": {
                "host": "uv run --project airflow pytest {test_path}",
                "breeze": "breeze exec pytest {test_path}",
                "preferred": "host",
                "params": {"test_path": {"required": True}},
            },
        }

        # Validate skill exists
        if skill_id not in skills:
            raise ValueError(f"Skill not found: {skill_id}")

        skill = skills[skill_id]

        # Validate required parameters
        for param_name, param_info in skill["params"].items():
            if param_info.get("required") and param_name not in kwargs:
                raise ValueError(f"Missing required parameter: {param_name}")

        # Choose context
        context = "breeze" if is_breeze else "host"
        command_template = skill[context]

        # Substitute parameters
        command = command_template
        for param_name, param_value in kwargs.items():
            placeholder = "{" + param_name + "}"
            command = command.replace(placeholder, str(param_value))

        # Handle optional parameters that weren't provided
        command = command.replace("{--target module}", "").replace("{module}", "")

        return command


# For backwards compatibility, keep the old function
def detect_context():
    """Legacy function - use BreezieContext.is_in_breeze() instead."""
    return "breeze" if BreezieContext.is_in_breeze() else "host"
