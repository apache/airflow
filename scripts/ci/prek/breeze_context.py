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
Agents call ``BreezeContext.get_command`` with a skill id and parameters.
The helper detects the current runtime context with ``BreezeContext.is_in_breeze``,
optionally using a manual override for debugging/testing.

Detection strategy
------------------
Check if the ``BREEZE`` environment variable is set. Breeze sets this variable
when starting the container, so its presence reliably indicates a Breeze environment.
In devcontainers, this variable is typically not set, which is intentional: devcontainers
behave like a host environment where tools like `git` and `uv` are directly available.

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
import shlex
from typing import Any, TypedDict

logger = logging.getLogger(__name__)


class SkillParam(TypedDict):
    required: bool


class SkillDef(TypedDict):
    host: str
    breeze: str | None
    preferred: str
    params: dict[str, SkillParam]


class BreezeContext:
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

        # Check BREEZE env variable — Breeze sets this when starting the container.
        if os.environ.get("BREEZE"):
            logger.debug("Detected Breeze via BREEZE environment variable")
            return True

        logger.debug("BREEZE environment variable not set; assuming host execution")
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
        is_breeze = BreezeContext.is_in_breeze(force_context=force_context)
        context = "breeze" if is_breeze else "host"

        # Define all available skills
        skills: dict[str, Any] = {
            "stage-changes": {
                # git add operates on the host working tree — not inside the container.
                # This is explicitly a host-only operation per the contributor workflow.
                "host": "git add {path}",
                "breeze": None,
                "preferred": "host",
                "params": {"path": {"required": True}},
            },
            "run-static-checks": {
                # prek is available on both host and inside Breeze; same command in both contexts.
                # --from-ref is required; --stage defaults to pre-commit if omitted.
                "host": "prek run --from-ref {from_ref} --stage pre-commit",
                "breeze": "prek run --from-ref {from_ref} --stage pre-commit",
                "preferred": "host",
                "params": {"from_ref": {"required": True}},
            },
            "run-unit-tests": {
                # project is the folder containing pyproject.toml, e.g. airflow-core, providers/amazon.
                "host": "uv run --project {project} pytest {test_path}",
                "breeze": "breeze run pytest {test_path}",
                "preferred": "host",
                "params": {"project": {"required": True}, "test_path": {"required": True}},
            },
        }

        # Validate skill exists
        if skill_id not in skills:
            available = ", ".join(sorted(skills.keys()))
            raise ValueError(f"Skill not found: {skill_id}. Available skills: {available}")

        skill: SkillDef = skills[skill_id]

        # Validate required parameters
        params = skill["params"]
        for param_name, param_info in params.items():
            if param_info["required"] and param_name not in kwargs:
                raise ValueError(f"Missing required parameter: {param_name} for skill {skill_id!r}")

        # Choose context; guard host-only skills
        if context == "breeze":
            command_template = skill["breeze"]
        else:
            command_template = skill["host"]
        if command_template is None:
            raise ValueError(
                f"Skill {skill_id!r} is host-only and cannot be run inside Breeze. "
                f"Run this command from your host terminal before switching to Breeze."
            )

        # Substitute parameters
        command: str = command_template
        for param_name, _ in params.items():
            placeholder = "{" + param_name + "}"
            if param_name in kwargs:
                command = command.replace(placeholder, shlex.quote(str(kwargs[param_name])))
            else:
                command = command.replace(placeholder, "")

        # Clean up extra whitespace that may remain after optional param removal
        command = " ".join(command.split())

        logger.debug(
            "Generated command for skill_id=%r context=%s kwargs=%r: %s",
            skill_id,
            context,
            kwargs,
            command,
        )
        return command


def detect_context(force_context: str | None = None) -> str:
    """Return ``host`` or ``breeze`` based on current environment.

    Parameters
    ----------
    force_context:
        Optional override (``host`` or ``breeze``).

    Returns
    -------
    str
        ``"breeze"`` or ``"host"``.
    """
    return "breeze" if BreezeContext.is_in_breeze(force_context=force_context) else "host"


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
