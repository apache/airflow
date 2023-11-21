#!/usr/bin/env python3
#
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
import subprocess
import sys
from pathlib import Path
from shutil import rmtree

import rich


def process_summary(success_message: str, error_message: str, completed_process: subprocess.CompletedProcess):
    if completed_process.returncode != 0:
        if os.environ.get("GITHUB_ACTIONS", "") != "":
            print("::endgroup::")
            print(f"::error::{error_message}")
        rich.print(f"[red]{error_message}")
        rich.print(completed_process.stdout)
        rich.print(completed_process.stderr)
        sys.exit(completed_process.returncode)
    else:
        rich.print(f"[green]{success_message}")


AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()
WWW_DIRECTORY = AIRFLOW_SOURCES_ROOT / "airflow" / "www"

rich.print("[bright_blue]Cleaning build directories\n")

for egg_info_file in AIRFLOW_SOURCES_ROOT.glob("*egg-info*"):
    rmtree(egg_info_file, ignore_errors=True)

rmtree(AIRFLOW_SOURCES_ROOT / "build", ignore_errors=True)

rich.print("[green]Cleaned build directories\n\n")

version_suffix = os.environ.get("VERSION_SUFFIX_FOR_PYPI", "")
package_format = os.environ.get("PACKAGE_FORMAT", "wheel")

rich.print(f"[bright_blue]Marking {AIRFLOW_SOURCES_ROOT} as safe directory for git commands.\n")

subprocess.run(
    ["git", "config", "--global", "--unset-all", "safe.directory"],
    cwd=AIRFLOW_SOURCES_ROOT,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    check=False,
)

subprocess.run(
    ["git", "config", "--global", "--add", "safe.directory", AIRFLOW_SOURCES_ROOT],
    cwd=AIRFLOW_SOURCES_ROOT,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    check=True,
)

rich.print(f"[green]Marked {AIRFLOW_SOURCES_ROOT} as safe directory for git commands.\n")

rich.print("[bright_blue]Checking airflow version\n")

airflow_version = subprocess.check_output(
    [sys.executable, "setup.py", "--version"], text=True, cwd=AIRFLOW_SOURCES_ROOT
).strip()

rich.print(f"[green]Airflow version: {airflow_version}\n")

RELEASED_VERSION_MATCHER = re.compile(r"^\d+\.\d+\.\d+$")

command = [sys.executable, "setup.py"]

if version_suffix:
    if RELEASED_VERSION_MATCHER.match(airflow_version):
        rich.print(f"[warning]Adding {version_suffix} suffix to the {airflow_version}")
        command.extend(["egg_info", "--tag-build", version_suffix])
    elif not airflow_version.endswith(version_suffix):
        rich.print(f"[red]Version {airflow_version} does not end with {version_suffix}. Using !")
        sys.exit(1)

if package_format in ["both", "wheel"]:
    command.append("bdist_wheel")
if package_format in ["both", "sdist"]:
    command.append("sdist")

rich.print(f"[bright_blue]Building packages: {package_format}\n")

process = subprocess.run(command, capture_output=True, text=True, cwd=AIRFLOW_SOURCES_ROOT)

process_summary("Airflow packages built successfully", "Error building Airflow packages", process)

if os.environ.get("GITHUB_ACTIONS", "") != "":
    print("::endgroup::")

rich.print("[green]Packages built successfully:\n")
for file in (AIRFLOW_SOURCES_ROOT / "dist").glob("apache*"):
    rich.print(file.name)
rich.print()
