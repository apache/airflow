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
import subprocess
import sys
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp

import yaml
from rich.console import Console

console = Console(color_system="standard", width=200)

AIRFLOW_ROOT_PATH = Path(__file__).parents[2].resolve()
AIRFLOW_DIST_PATH = AIRFLOW_ROOT_PATH / "dist"
REPRODUCIBLE_BUILD_YAML_PATH = AIRFLOW_ROOT_PATH / "reproducible_build.yaml"
AIRFLOW_CORE_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-core"
AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_CORE_ROOT_PATH / "src"
AIRFLOW_INIT_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py"
WWW_DIRECTORY = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "www"
VERSION_SUFFIX = os.environ.get("VERSION_SUFFIX", "")
DISTRIBUTION_FORMAT = os.environ.get("DISTRIBUTION_FORMAT", "wheel")


def clean_build_directory():
    console.print("[bright_blue]Cleaning build directories\n")
    for egg_info_file in AIRFLOW_ROOT_PATH.glob("*egg-info*"):
        rmtree(egg_info_file, ignore_errors=True)
    rmtree(AIRFLOW_ROOT_PATH / "build", ignore_errors=True)
    console.print("[green]Cleaned build directories\n\n")


def mark_git_directory_as_safe():
    console.print(f"[bright_blue]Marking {AIRFLOW_ROOT_PATH} as safe directory for git commands.\n")
    subprocess.run(
        ["git", "config", "--global", "--unset-all", "safe.directory"],
        cwd=AIRFLOW_ROOT_PATH,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    subprocess.run(
        ["git", "config", "--global", "--add", "safe.directory", AIRFLOW_ROOT_PATH],
        cwd=AIRFLOW_ROOT_PATH,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    console.print(f"[green]Marked {AIRFLOW_ROOT_PATH} as safe directory for git commands.\n")


def get_current_airflow_version() -> str:
    console.print("[bright_blue]Checking airflow version\n")
    airflow_version = subprocess.check_output(
        [sys.executable, "-m", "hatch", "version"], text=True, cwd=AIRFLOW_ROOT_PATH
    ).strip()
    console.print(f"[green]Airflow version: {airflow_version}\n")
    return airflow_version


def apply_distribution_format_to_hatch_command(build_command: list[str], distribution_format: str):
    if distribution_format in ["both", "sdist"]:
        build_command.extend(["-t", "sdist"])
    if distribution_format in ["both", "wheel"]:
        build_command.extend(["-t", "wheel"])


def build_airflow_packages(distribution_format: str):
    reproducible_date = yaml.safe_load(REPRODUCIBLE_BUILD_YAML_PATH.read_text())["source-date-epoch"]
    envcopy = os.environ.copy()
    envcopy["SOURCE_DATE_EPOCH"] = str(reproducible_date)
    airflow_core_build_command = [sys.executable, "-m", "hatch", "build", "-t", "custom"]
    apply_distribution_format_to_hatch_command(airflow_core_build_command, distribution_format)
    console.print(f"[bright_blue]Building apache-airflow-core distributions: {distribution_format}\n")
    build_process = subprocess.run(
        airflow_core_build_command,
        check=False,
        capture_output=False,
        cwd=AIRFLOW_CORE_ROOT_PATH,
        env=envcopy,
    )
    if build_process.returncode != 0:
        console.print("[red]Error building Airflow packages")
        sys.exit(build_process.returncode)
    airflow_build_command = [sys.executable, "-m", "hatch", "build"]
    apply_distribution_format_to_hatch_command(airflow_build_command, distribution_format)
    console.print(f"[bright_blue]Building apache-airflow distributions: {distribution_format}\n")
    build_process = subprocess.run(
        airflow_build_command,
        check=False,
        capture_output=False,
        cwd=AIRFLOW_ROOT_PATH,
        env=envcopy,
    )
    if build_process.returncode != 0:
        console.print("[red]Error building Airflow packages")
        sys.exit(build_process.returncode)
    if distribution_format in ["both", "sdist"]:
        console.print("[bright_blue]Checking if sdist packages can be built into wheels")
        for glob_pattern in ["apache_airflow_core-*.tar.gz", "apache_airflow-*.tar.gz"]:
            for sdist_distribution_file in AIRFLOW_DIST_PATH.glob(glob_pattern):
                console.print(f"[bright_blue]Validate build wheel from sdist: {sdist_distribution_file.name}")
                if "-sources.tar.gz" not in sdist_distribution_file.name:
                    # no need to delete - we are in temporary container
                    tmpdir = mkdtemp()
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "pip",
                            "wheel",
                            "--wheel-dir",
                            tmpdir,
                            "--no-deps",
                            "--no-cache",
                            sdist_distribution_file.as_posix(),
                        ],
                        check=False,
                    )
                    if result.returncode != 0:
                        console.print(f"[red]Error installing {sdist_distribution_file.name}")
                        sys.exit(result.returncode)
                    console.print(
                        f"[green]Sdist package {sdist_distribution_file.name} can be built into wheels"
                    )
                console.print("[green]Sdist package is installed successfully.")
    console.print("[green]Airflow packages built successfully")


clean_build_directory()
mark_git_directory_as_safe()

build_airflow_packages(DISTRIBUTION_FORMAT)

for file in AIRFLOW_DIST_PATH.glob("apache_airflow*"):
    console.print(file.name)
console.print()
