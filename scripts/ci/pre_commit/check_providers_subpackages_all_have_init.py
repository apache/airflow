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
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parents[3].resolve()
ACCEPTED_NON_INIT_DIRS = ["adr", "doc", "templates", "__pycache__"]


def check_dir_init_file(folders: list[Path]) -> None:
    missing_init_dirs: list[Path] = []
    folders = list(folders)
    for path in folders:
        for root, dirs, files in os.walk(path):
            # Edit it in place, so we don't recurse to folders we don't care about
            dirs[:] = [d for d in dirs if d not in ACCEPTED_NON_INIT_DIRS]

            if "__init__.py" in files:
                continue

            missing_init_dirs.append(Path(root))

    if missing_init_dirs:
        with ROOT_DIR.joinpath(
            "scripts/ci/license-templates/LICENSE.txt"
        ).open() as license:
            license_txt = license.readlines()
        prefixed_licensed_txt = [
            f"# {line}" if line != "\n" else "#\n" for line in license_txt
        ]

        for missing_init_dir in missing_init_dirs:
            (missing_init_dir / "__init__.py").write_text("".join(prefixed_licensed_txt))

        print("No __init__.py file was found in the following provider directories:")
        print(
            "\n".join(
                [missing_init_dir.as_posix() for missing_init_dir in missing_init_dirs]
            )
        )
        print(
            "\nThe missing __init__.py files have been created. Please add these new files to a commit."
        )
        sys.exit(1)


if __name__ == "__main__":
    providers_root = Path(f"{ROOT_DIR}/providers")
    providers_ns = providers_root.joinpath("src", "airflow", "providers")
    providers_tests = providers_root.joinpath("tests")

    providers_pkgs = sorted(map(lambda f: f.parent, providers_ns.rglob("provider.yaml")))
    check_dir_init_file(providers_pkgs)

    check_dir_init_file([providers_root / "tests"])
