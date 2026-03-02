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
import subprocess
import sys
from pathlib import Path

from airflow_breeze import NAME
from airflow_breeze.utils.console import get_console


def reinstall_breeze(breeze_sources: Path, re_run: bool = True):
    """
    Re-installs Breeze from specified sources.
    :param breeze_sources: Sources where to install Breeze from.
    :param re_run: whether to re-run the original command that breeze was run with.
    """
    # First check if `breeze` is installed with uv and if it is, reinstall it using uv
    # If not - we assume pipx is used and we reinstall it using pipx
    # Note that we cannot use `pipx upgrade` here because we sometimes install
    # Breeze from different sources than originally installed (i.e. when we reinstall airflow
    # From the current directory.
    get_console().print(f"\n[info]Reinstalling Breeze from {breeze_sources}\n")
    breeze_installed_with_uv = False
    breeze_installed_with_pipx = False
    try:
        result_uv = subprocess.run(["uv", "tool", "list"], text=True, capture_output=True, check=False)
        if result_uv.returncode == 0:
            if "apache-airflow-breeze" in result_uv.stdout:
                breeze_installed_with_uv = True
    except FileNotFoundError:
        pass
    try:
        result_pipx = subprocess.run(["pipx", "list"], text=True, capture_output=True, check=False)
        if result_pipx.returncode == 0:
            if "apache-airflow-breeze" in result_pipx.stdout:
                breeze_installed_with_pipx = True
    except FileNotFoundError:
        pass
    if breeze_installed_with_uv and breeze_installed_with_pipx:
        get_console().print(
            "[error]Breeze is installed both with `uv` and `pipx`. This is not supported.[/]\n"
        )
        get_console().print(
            "[info]Please uninstall Breeze and install it only with one of the methods[/]\n"
            "[info]The `uv` installation method is recommended as it is much faster[/]\n"
        )
        get_console().print(
            "To uninstall Breeze installed with pipx run:\n     pipx uninstall apache-airflow-breeze\n"
        )
        get_console().print(
            "To uninstall Breeze installed with uv run:\n     uv tool uninstall apache-airflow-breeze\n"
        )
        sys.exit(1)
    elif breeze_installed_with_uv:
        subprocess.check_call(
            ["uv", "tool", "install", "--force", "--reinstall", "-e", breeze_sources.as_posix()],
            stderr=subprocess.STDOUT,
        )
    elif breeze_installed_with_pipx:
        subprocess.check_call(
            ["pipx", "install", "-e", breeze_sources.as_posix(), "--force"], stderr=subprocess.STDOUT
        )

    if re_run:
        # Make sure we don't loop forever if the metadata hash hasn't been updated yet (else it is tricky to
        # run prek checks via breeze!)
        os.environ["SKIP_BREEZE_SELF_UPGRADE_CHECK"] = "true"
        os.execl(sys.executable, sys.executable, *sys.argv)
    get_console().print(f"\n[info]Breeze has been reinstalled from {breeze_sources}. Exiting now.[/]\n\n")
    sys.exit(0)


def warn_non_editable():
    get_console().print(
        "\n[error]Breeze is installed in a wrong way.[/]\n"
        "\n[error]It should only be installed in editable mode[/]\n\n"
        "[info]Please go to Airflow sources and run[/]\n\n"
        f"     {NAME} setup self-upgrade --use-current-airflow-sources\n"
        '[warning]If during installation, you see warning with "Ignoring --editable install",[/]\n'
        '[warning]make sure you first downgrade "packaging" package to <23.2, for example by:[/]\n\n'
        f'     pip install "packaging<23.2"\n\n'
    )


def inform_about_self_upgrade():
    get_console().print(
        "\n[info]Breeze dependencies changed since the installation. Reinstalling them!\n\n[/]"
        "You might need to rerun the command in case it fails\n\n"
    )
