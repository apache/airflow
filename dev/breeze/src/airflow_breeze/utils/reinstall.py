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
    Reinstalls Breeze from specified sources.
    :param breeze_sources: Sources where to install Breeze from.
    :param re_run: whether to re-run the original command that breeze was run with.
    """
    # Note that we cannot use `pipx upgrade` here because we sometimes install
    # Breeze from different sources than originally installed (i.e. when we reinstall airflow
    # From the current directory.
    get_console().print(f"\n[info]Reinstalling Breeze from {breeze_sources}\n")
    subprocess.check_call(["pipx", "install", "-e", str(breeze_sources), "--force"])
    if re_run:
        # Make sure we don't loop forever if the metadata hash hasn't been updated yet (else it is tricky to
        # run pre-commit checks via breeze!)
        os.environ["SKIP_UPGRADE_CHECK"] = "1"
        os.execl(sys.executable, sys.executable, *sys.argv)
    get_console().print(f"\n[info]Breeze has been reinstalled from {breeze_sources}. Exiting now.[/]\n\n")
    sys.exit(0)


def warn_non_editable():
    get_console().print(
        "\n[error]Breeze is installed in a wrong way.[/]\n"
        "\n[error]It should only be installed in editable mode[/]\n\n"
        "[info]Please go to Airflow sources and run[/]\n\n"
        f"     {NAME} setup self-upgrade --use-current-airflow-sources\n"
    )


def warn_dependencies_changed():
    get_console().print(
        f"\n[warning]Breeze dependencies changed since the installation![/]\n\n"
        f"[warning]This might cause various problems!![/]\n\n"
        f"If you experience problems - reinstall Breeze with:\n\n"
        f"    {NAME} setup self-upgrade\n"
        "\nThis should usually take couple of seconds.\n"
    )
