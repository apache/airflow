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
from contextlib import contextmanager
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp

import yaml
from packaging.version import Version
from rich.console import Console

console = Console(color_system="standard", width=200)

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()
REPRODUCIBLE_BUILD_FILE = AIRFLOW_SOURCES_ROOT / "airflow" / "reproducible_build.yaml"
AIRFLOW_INIT_FILE = AIRFLOW_SOURCES_ROOT / "airflow" / "__init__.py"
WWW_DIRECTORY = AIRFLOW_SOURCES_ROOT / "airflow" / "www"
VERSION_SUFFIX = os.environ.get("VERSION_SUFFIX_FOR_PYPI", "")
PACKAGE_FORMAT = os.environ.get("PACKAGE_FORMAT", "wheel")


def clean_build_directory():
    console.print("[bright_blue]Cleaning build directories\n")
    for egg_info_file in AIRFLOW_SOURCES_ROOT.glob("*egg-info*"):
        rmtree(egg_info_file, ignore_errors=True)
    rmtree(AIRFLOW_SOURCES_ROOT / "build", ignore_errors=True)
    console.print("[green]Cleaned build directories\n\n")


def mark_git_directory_as_safe():
    console.print(
        f"[bright_blue]Marking {AIRFLOW_SOURCES_ROOT} as safe directory for git commands.\n"
    )
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
        check=False,
    )
    console.print(
        f"[green]Marked {AIRFLOW_SOURCES_ROOT} as safe directory for git commands.\n"
    )


def get_current_airflow_version() -> str:
    console.print("[bright_blue]Checking airflow version\n")
    airflow_version = subprocess.check_output(
        [sys.executable, "-m", "hatch", "version"], text=True, cwd=AIRFLOW_SOURCES_ROOT
    ).strip()
    console.print(f"[green]Airflow version: {airflow_version}\n")
    return airflow_version


def build_airflow_packages(package_format: str):
    build_command = [sys.executable, "-m", "hatch", "build", "-t", "custom"]
    if package_format in ["both", "sdist"]:
        build_command.extend(["-t", "sdist"])
    if package_format in ["both", "wheel"]:
        build_command.extend(["-t", "wheel"])

    reproducible_date = yaml.safe_load(REPRODUCIBLE_BUILD_FILE.read_text())[
        "source-date-epoch"
    ]

    envcopy = os.environ.copy()
    envcopy["SOURCE_DATE_EPOCH"] = str(reproducible_date)
    console.print(f"[bright_blue]Building packages: {package_format}\n")
    build_process = subprocess.run(
        build_command,
        capture_output=False,
        cwd=AIRFLOW_SOURCES_ROOT,
        env=envcopy,
    )

    if build_process.returncode != 0:
        console.print("[red]Error building Airflow packages")
        sys.exit(build_process.returncode)
    else:
        if package_format in ["both", "sdist"]:
            console.print(
                "[bright_blue]Checking if sdist packages can be built into wheels"
            )
            for file in (AIRFLOW_SOURCES_ROOT / "dist").glob(
                "apache_airflow-[0-9]*.tar.gz"
            ):
                console.print(
                    f"[bright_blue]Validate build wheel from sdist: {file.name}"
                )
                if "-sources.tar.gz" not in file.name:
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
                            "--no-binary",
                            ":all:",
                            file.as_posix(),
                        ],
                        check=False,
                    )
                    if result.returncode != 0:
                        console.print(f"[red]Error installing {file.name}")
                        sys.exit(result.returncode)
                    console.print(
                        f"[green]Sdist package {file.name} can be built into wheels"
                    )
                console.print("[green]Sdist package is installed successfully.")
        console.print("[green]Airflow packages built successfully")


def set_package_version(version: str) -> None:
    console.print(f"\n[yellow]Setting {version} for Airflow package\n")
    # replace __version__ with the version passed as argument in python
    init_content = AIRFLOW_INIT_FILE.read_text()
    init_content = re.sub(
        r'__version__ = "[^"]+"', f'__version__ = "{version}"', init_content
    )
    AIRFLOW_INIT_FILE.write_text(init_content)


@contextmanager
def package_version(version_suffix: str):
    release_version_matcher = re.compile(r"^\d+\.\d+\.\d+$")
    airflow_version = get_current_airflow_version()

    update_version = False
    if version_suffix:
        if airflow_version.endswith(f".{version_suffix}"):
            console.print(
                f"[bright_blue]The {airflow_version} already has suffix {version_suffix}. Not updating it.\n"
            )
        elif not release_version_matcher.match(airflow_version):
            console.print(
                f"[warning]You should only pass version suffix if {airflow_version} "
                f"does not have suffix in code. The version in code is: {airflow_version}.\n"
                f"Overriding the version in code with the {version_suffix}."
            )
            airflow_version = Version(airflow_version).base_version
            update_version = True
        else:
            update_version = True
    if update_version:
        set_package_version(f"{airflow_version}.{version_suffix}")
    try:
        yield
    finally:
        # Set the version back to the original version
        if update_version:
            set_package_version(airflow_version)


clean_build_directory()
mark_git_directory_as_safe()

with package_version(VERSION_SUFFIX):
    build_airflow_packages(PACKAGE_FORMAT)

for file in (AIRFLOW_SOURCES_ROOT / "dist").glob("apache*"):
    console.print(file.name)
console.print()
