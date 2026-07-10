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
from __future__ import annotations

import os
import re
import sys

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command

try:
    from packaging import version
except ImportError:
    # Autocomplete can run with only click installed; defer the ImportError until a
    # caller actually needs version comparison.
    version = None  # type: ignore[assignment]


def is_ci_environment():
    """Check if running in CI environment."""
    return os.environ.get("CI", "").lower() in ("true", "1", "yes")


def _read_required_uv_version() -> str | None:
    """Parse ``[tool.uv] required-version = ">=X.Y.Z"`` from the root ``pyproject.toml``.

    Returns the raw ``X.Y.Z`` string, or ``None`` if the declaration is missing or
    unparsable — in which case the caller should skip the check rather than fail the
    environment bootstrap.
    """
    pyproject = (AIRFLOW_ROOT_PATH / "pyproject.toml").read_text()
    # Narrow to the [tool.uv] section so we don't pick up an unrelated required-version.
    section = re.search(r"^\[tool\.uv\]\s*$(?P<body>.*?)(?=^\[|\Z)", pyproject, re.MULTILINE | re.DOTALL)
    if not section:
        return None
    match = re.search(
        r"""^\s*required-version\s*=\s*["']\s*>=\s*(?P<ver>\d+(?:\.\d+){0,2})\s*["']""",
        section.group("body"),
        re.MULTILINE,
    )
    return match.group("ver") if match else None


def check_uv_version(quiet: bool = False):
    """Fail the environment check if ``uv`` on PATH is older than the project minimum.

    The minimum is declared as ``[tool.uv] required-version`` in the root
    ``pyproject.toml``. Lives here (not in ``docker_command_utils``) because the check
    is not docker-specific — it applies to any breeze entry point that invokes ``uv``,
    including flows that never talk to Docker.
    """
    required_uv_version = _read_required_uv_version()
    if required_uv_version is None:
        if not quiet:
            console_print(
                "[warning]Could not read `[tool.uv] required-version` from pyproject.toml — "
                "skipping uv version check.[/]"
            )
        return
    uv_version_result = run_command(
        ["uv", "--version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    if uv_version_result.returncode != 0:
        console_print(
            f"""
[error]`uv` is not installed or not on PATH.[/]
[warning]Breeze needs uv >= {required_uv_version} to manage its environments.[/]
See https://docs.astral.sh/uv/getting-started/installation/ for installation instructions.
"""
        )
        sys.exit(1)
    match = re.search(r"\b(\d+\.\d+(?:\.\d+)?)", uv_version_result.stdout)
    if not match:
        if not quiet:
            console_print(
                f"[warning]Unexpected `uv --version` output: {uv_version_result.stdout!r} — "
                "skipping uv version check.[/]"
            )
        return
    uv_version = match.group(1)
    if version.parse(uv_version) >= version.parse(required_uv_version):
        if not quiet:
            console_print(f"[success]Good version of uv: {uv_version}.[/]")
    else:
        console_print(
            f"""
[error]Your version of uv is too old: {uv_version}.\n[/]
[warning]Please upgrade to at least {required_uv_version}.\n[/]
Run `uv self update` (or reinstall following
https://docs.astral.sh/uv/getting-started/installation/).
"""
        )
        sys.exit(1)
