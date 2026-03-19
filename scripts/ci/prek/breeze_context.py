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
Breeze context detection and command selection.

This module exists for agent workflows that must reliably choose between:

- "host" execution (commands intended to run on the contributor machine)
- "breeze" execution (commands intended to run inside the Breeze container)

Architecture
------------
Agents call :meth:`BreezieContext.get_command` with a skill id and parameters.
The helper detects the current runtime context with :meth:`BreezieContext.is_in_breeze`,
optionally using a manual override for debugging/testing.

Detection strategy (priority)
------------------------------
1. ``BREEZE_HOME`` environment variable
2. ``/.dockerenv`` (Docker container marker)
3. ``/.containerenv`` (Podman container marker)
4. ``/opt/airflow`` directory presence (Breeze install path)
5. Default to ``host``

Testing
-------
This module is covered by unit tests:
- `scripts/tests/test_breeze_context.py`
- `scripts/tests/test_edge_cases.py`

For manual debugging you can run:
`python3 scripts/ci/prek/breeze_context.py --force-context host`
or
`python3 scripts/ci/prek/breeze_context.py --force-context breeze`
"""

from __future__ import annotations

import argparse
import logging
import os

logger = logging.getLogger(__name__)


class BreezieContext:
    """Detect whether code is running in Breeze container or on host."""

    @staticmethod
    def is_in_breeze(force_context: str | None = None) -> bool:
        """
        Detect whether the current process is running inside Breeze.

        Parameters
        ----------
        force_context:
            Optional manual override.
            - ``"breeze"`` forces Breeze context.
            - ``"host"`` forces host context.
            - ``None`` uses auto-detection.

        Returns
        -------
        bool
            ``True`` when running in Breeze, otherwise ``False``.

        Raises
        ------
        ValueError
            If ``force_context`` is not one of ``"host"`` or ``"breeze"``.
        """
        if force_context is not None:
            normalized = force_context.strip().lower()
            if normalized == "breeze":
                logger.info("Context forced to breeze via --force-context")
                return True
            if normalized == "host":
                logger.info("Context forced to host via --force-context")
                return False
            raise ValueError(f"Invalid force_context={force_context!r}. Expected 'host' or 'breeze'.")

        # 1) BREEZE_HOME environment variable (explicit marker)
        try:
            breeze_home = os.environ.get("BREEZE_HOME")
        except Exception as e:  # pragma: no cover (defensive)
            logger.exception("Failed to read BREEZE_HOME from environment: %s", e)
            breeze_home = None
        if breeze_home:
            logger.debug("Detected Breeze via BREEZE_HOME=%r", breeze_home)
            return True

        dockerenv_marker = "/.dockerenv"
        containerenv_marker = "/.containerenv"
        opt_airflow_dir = "/opt/airflow"

        # 2) /.dockerenv (Docker container marker)
        try:
            if os.path.exists(dockerenv_marker):
                logger.debug("Detected Breeze via %s marker", dockerenv_marker)
                return True
        except OSError as e:  # pragma: no cover (defensive)
            logger.warning("Failed checking %s: %s", dockerenv_marker, e)

        # 3) /.containerenv (Podman container marker)
        try:
            if os.path.exists(containerenv_marker):
                logger.debug("Detected Breeze via %s (Podman) marker", containerenv_marker)
                return True
        except OSError as e:  # pragma: no cover (defensive)
            logger.warning("Failed checking %s: %s", containerenv_marker, e)

        # 4) /opt/airflow (Breeze standard installation path)
        try:
            if os.path.isdir(opt_airflow_dir):
                logger.debug("Detected Breeze via %s directory", opt_airflow_dir)
                return True
        except OSError as e:  # pragma: no cover (defensive)
            logger.warning("Failed checking %s: %s", opt_airflow_dir, e)

        # Optional: log SSH hint without changing behavior.
        try:
            ssh_connection = os.environ.get("SSH_CONNECTION")
        except Exception:  # pragma: no cover (defensive)
            ssh_connection = None
        if ssh_connection:
            logger.debug("SSH_CONNECTION detected; defaulting to host unless Breeze markers exist")

        # 5) Default: assume host
        logger.debug("No Breeze markers detected; assuming host execution")
        return False

    @staticmethod
    def get_command(skill_id: str, *, force_context: str | None = None, **kwargs) -> str:
        """
        Generate the appropriate command for the given skill and context.

        Args:
            skill_id: Unique skill identifier (e.g., "run-unit-tests")
            **kwargs: Skill-specific parameters

        Returns:
            Command string to execute

        Raises:
            ValueError: If ``skill_id`` is unknown or required parameters are missing.
        """
        is_breeze = BreezieContext.is_in_breeze(force_context=force_context)
        context = "breeze" if is_breeze else "host"

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
            available = ", ".join(sorted(skills.keys()))
            raise ValueError(f"Skill not found: {skill_id}. Available skills: {available}")

        skill = skills[skill_id]

        # Validate required parameters
        for param_name, param_info in skill["params"].items():
            if param_info.get("required") and param_name not in kwargs:
                raise ValueError(f"Missing required parameter: {param_name} for skill {skill_id!r}")

        # Choose context
        command_template = skill[context]

        # Substitute parameters
        command = command_template
        for param_name, param_value in kwargs.items():
            placeholder = "{" + param_name + "}"
            command = command.replace(placeholder, str(param_value))

        # Handle optional parameters that weren't provided
        command = command.replace("{--target module}", "").replace("{module}", "")

        logger.debug(
            "Generated command for skill_id=%r context=%s kwargs=%r: %s",
            skill_id,
            context,
            kwargs,
            command,
        )
        return command


# For backwards compatibility, keep the old function
def detect_context(force_context: str | None = None) -> str:
    """Legacy convenience wrapper that returns ``host`` or ``breeze``.

    Parameters
    ----------
    force_context:
        Optional override (``host`` or ``breeze``). If set, the returned value
        will match this override.

    Returns
    -------
    str
        ``"breeze"`` or ``"host"``.
    """
    return "breeze" if BreezieContext.is_in_breeze(force_context=force_context) else "host"


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for this module."""
    parser = argparse.ArgumentParser(description="Detect Breeze context for agent skills")
    parser.add_argument(
        "--force-context",
        choices=["host", "breeze"],
        default=None,
        help="Override auto-detection and force host or breeze context",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging for context detection steps",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for manual debugging."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    print(detect_context(force_context=args.force_context))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
