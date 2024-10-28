#!/usr/bin/env python
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
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

COMMON_PRECOMMIT_PATH = Path(__file__).parent.resolve()
sys.path.insert(
    0, COMMON_PRECOMMIT_PATH.as_posix()
)  # make sure common_precommit_utils is imported
from common_precommit_utils import console

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()
PYPROJECT_TOML_FILE = AIRFLOW_SOURCES / "pyproject.toml"

HATCHLING_MATCH = re.compile(r"hatchling==[0-9.]*")

FILES_TO_REPLACE_HATCHLING_IN = [
    AIRFLOW_SOURCES / ".pre-commit-config.yaml",
    AIRFLOW_SOURCES / "clients" / "python" / "pyproject.toml",
    AIRFLOW_SOURCES / "docker_tests" / "requirements.txt",
]

files_changed = False


if __name__ == "__main__":
    python38_bin = shutil.which("python3.9")
    if not python38_bin:
        print("Python 3.9 is required to run this script.")
        sys.exit(1)
    temp_dir = Path(tempfile.mkdtemp())
    hatchling_spec = ""
    try:
        subprocess.check_call([python38_bin, "-m", "venv", temp_dir.as_posix()])
        venv_python = temp_dir / "bin" / "python"
        subprocess.check_call(
            [venv_python, "-m", "pip", "install", "gitpython", "hatchling"]
        )
        frozen_deps = subprocess.check_output(
            [venv_python, "-m", "pip", "freeze"], text=True
        )
        deps = [
            dep for dep in sorted(frozen_deps.splitlines()) if not dep.startswith("pip==")
        ]
        pyproject_toml_content = PYPROJECT_TOML_FILE.read_text()
        result = []
        skipping = False
        for line in pyproject_toml_content.splitlines():
            if not skipping:
                result.append(line)
            if line == "requires = [":
                skipping = True
                for dep in deps:
                    # Tomli is only needed for Python < 3.11, otherwise stdlib tomllib is used
                    if dep.startswith("tomli=="):
                        dep = dep + "; python_version < '3.11'"
                    result.append(f'    "{dep}",')
                    if dep.startswith("hatchling=="):
                        hatchling_spec = dep
            if skipping and line == "]":
                skipping = False
                result.append(line)
        result.append("")
        new_pyproject_toml_file_content = "\n".join(result)
        if new_pyproject_toml_file_content != pyproject_toml_content:
            if os.environ.get("SKIP_TROVE_CLASSIFIERS_ONLY", "false").lower() == "true":
                diff = set(new_pyproject_toml_file_content.splitlines()) - (
                    set(pyproject_toml_content.splitlines())
                )
                if len(diff) == 1 and "trove-classifiers" in next(iter(diff)):
                    console.print(
                        "\n[yellow]Trove classifiers were changed. Please update them manually.\n"
                    )
                    console.print(
                        "\n[blue]Please run:[/blue]\n\n"
                        "pre-commit run --hook-stage manual update-build-dependencies --all-files\n"
                    )
                    console.print("\n[blue]Then commit the resulting files.\n")
                    sys.exit(0)
            files_changed = True
            PYPROJECT_TOML_FILE.write_text(new_pyproject_toml_file_content)
        for file_to_replace_hatchling in FILES_TO_REPLACE_HATCHLING_IN:
            old_file_content = file_to_replace_hatchling.read_text()
            new_file_content = HATCHLING_MATCH.sub(
                hatchling_spec, old_file_content, re.MULTILINE
            )
            if new_file_content != old_file_content:
                files_changed = True
                file_to_replace_hatchling.write_text(new_file_content)
    finally:
        shutil.rmtree(temp_dir)

    if files_changed:
        console.print(
            "\n[red]Build dependencies have changed. Please update them manually.\n"
        )
        console.print(
            "\n[blue]Please run:[/blue]\n\n"
            "pre-commit run --hook-stage manual update-build-dependencies --all-files\n"
        )
        console.print("\n[blue]Then commit the resulting files.\n")
        sys.exit(1)
