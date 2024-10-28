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

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_precommit_utils import console

# update this version when we switch to a newer version of Python
required_version = tuple(map(int, "3.9".split(".")))
required_version_str = f"{required_version[0]}.{required_version[1]}"
global_version = tuple(
    map(
        int,
        subprocess.run(
            [
                "python3",
                "-c",
                'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")',
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.strip()
        .split("."),
    )
)


if global_version < required_version:
    console.print(
        f"[red]Python {required_version_str} or higher is required to install pre-commit.\n"
    )
    console.print(f"[green]Current version is {global_version}\n")
    console.print(
        "[bright_yellow]Please follow those steps:[/]\n\n"
        f" * make sure that `python3 --version` is at least {required_version_str}\n"
        f" * run `pre-commit clean`\n"
        f" * run `pre-commit install --install-hooks`\n\n"
    )
    console.print(
        "There are various ways you can set `python3` to point to a newer version of Python.\n\n"
        f"For example run `pyenv global {required_version_str}` if you use pyenv, or\n"
        f"you can use venv with python {required_version_str} when you use pre-commit, or\n"
        "you can use `update-alternatives` if you use Ubuntu, or\n"
        "you can set `PATH` to point to the newer version of Python.\n\n"
    )
    sys.exit(1)
else:
    console.print(f"Python version is sufficient: {required_version_str}")
