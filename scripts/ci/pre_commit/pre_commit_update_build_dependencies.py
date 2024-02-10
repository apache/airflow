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

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()
PYPROJECT_TOML_FILE = AIRFLOW_SOURCES / "pyproject.toml"

if __name__ == "__main__":
    temp_dir = Path(tempfile.mkdtemp())
    try:
        subprocess.check_call([sys.executable, "-m", "venv", temp_dir.as_posix()])
        venv_python = temp_dir / "bin" / "python"
        subprocess.check_call([venv_python, "-m", "pip", "install", "gitpython", "hatchling"])
        frozen_deps = subprocess.check_output([venv_python, "-m", "pip", "freeze"], text=True)
        deps = [dep for dep in sorted(frozen_deps.splitlines()) if not dep.startswith("pip==")]
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
            if skipping and line == "]":
                skipping = False
                result.append(line)
        result.append("")
        PYPROJECT_TOML_FILE.write_text("\n".join(result))
    finally:
        shutil.rmtree(temp_dir)
