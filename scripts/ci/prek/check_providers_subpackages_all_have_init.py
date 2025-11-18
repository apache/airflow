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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import (
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
    KNOWN_SECOND_LEVEL_PATHS,
    console,
)

ACCEPTED_NON_INIT_DIRS = [
    "adr",
    "doc",
    "3rd-party-licenses",
    "templates",
    "__pycache__",
    "static",
    "dist",
    "node_modules",
    "non_python_src",
]

IGNORE_DIR_PATTERNS = [
    "airflow/providers/edge3/plugins",
]

PATH_EXTENSION_STRING = '__path__ = __import__("pkgutil").extend_path(__path__, __name__)'

ALLOWED_SUB_FOLDERS_OF_TESTS = ["unit", "system", "integration"]

should_fail = False
fatal_error = False
missing_init_dirs: list[Path] = []
missing_path_extension_dirs: list[Path] = []


def _what_kind_of_test_init_py_needed(base_path: Path, folder: Path) -> tuple[bool, bool]:
    """Returns a tuple of two booleans indicating need and type of __init__.py file.

    The first boolean is True if __init__.py is needed, False otherwise.
    The second boolean is True if the folder needs path extension (i.e. if we expect that other packages
    have the same folder to import things from this folder), False otherwise.
    """
    depth = len(folder.relative_to(base_path).parts)
    if depth == 0:
        # this is the "tests" folder itself
        return False, False
    if depth == 1:
        # this is one of "unit", "system", "integration" folder
        if folder.name not in ALLOWED_SUB_FOLDERS_OF_TESTS:
            console.print(f"[red]Unexpected folder {folder} in {base_path}[/]")
            console.print(f"[yellow]Only {ALLOWED_SUB_FOLDERS_OF_TESTS} should be sub-folders of tests.[/]")
            global should_fail
            global fatal_error
            should_fail = True
            fatal_error = True
        return False, False
    if depth == 2:
        # For known sub-packages that can occur in several packages we need to add __path__ extension
        return True, folder.name in KNOWN_SECOND_LEVEL_PATHS
    # all other sub-packages should have plain __init__.py
    return True, False


def _determine_init_py_action(need_path_extension: bool, root_path: Path):
    init_py_file = root_path.joinpath("__init__.py")
    if not init_py_file.exists():
        missing_init_dirs.append(root_path)
        console.print(f"Missing __init__.py file {init_py_file}")
        if need_path_extension:
            missing_path_extension_dirs.append(root_path)
            console.print(f"Missing path extension in: {init_py_file}")
    elif need_path_extension:
        text = init_py_file.read_text()
        if PATH_EXTENSION_STRING not in text:
            missing_path_extension_dirs.append(root_path)
            console.print(f"Missing path extension in existing {init_py_file}")


def check_dir_init_test_folders(folders: list[Path]) -> None:
    folders = list(folders)
    for root_distribution_path in folders:
        # We need init folders for all folders and for the common ones we need path extension
        tests_folder = root_distribution_path / "tests"
        print("Checking for __init__.py files in distribution for tests: ", tests_folder)
        for root, dirs, _ in os.walk(tests_folder):
            # Edit it in place, so we don't recurse to folders we don't care about
            dirs[:] = [d for d in dirs if d not in ACCEPTED_NON_INIT_DIRS]
            root_path = Path(root)
            need_init_py, need_path_extension = _what_kind_of_test_init_py_needed(tests_folder, root_path)
            if need_init_py:
                _determine_init_py_action(need_path_extension, root_path)


def check_dir_init_src_folders(folders: list[Path]) -> None:
    folders = list(folders)
    for root_distribution_path in folders:
        # We need init folders for all folders and for the common ones we need path extension
        providers_base_folder = root_distribution_path / "src" / "airflow"
        print("Checking for __init__.py files in distribution for src: ", providers_base_folder)
        for root, dirs, _ in os.walk(providers_base_folder):
            print("Checking: ", root)
            root_path = Path(root)
            # Edit it in place, so we don't recurse to folders we don't care about
            dirs[:] = [
                d
                for d in dirs
                if d not in ACCEPTED_NON_INIT_DIRS
                and not any(pattern in root for pattern in IGNORE_DIR_PATTERNS)
            ]
            relative_root_path = root_path.relative_to(providers_base_folder)
            need_path_extension = (
                root_path == providers_base_folder
                or len(relative_root_path.parts) == 1
                or len(relative_root_path.parts) == 2
                and relative_root_path.parts[1] in KNOWN_SECOND_LEVEL_PATHS
                and relative_root_path.parts[0] == "providers"
            )
            print("Needs path extension: ", need_path_extension)
            _determine_init_py_action(need_path_extension, root_path)


if __name__ == "__main__":
    providers_distributions = sorted(
        map(lambda f: f.parent, AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"))
    )
    check_dir_init_test_folders(providers_distributions)
    check_dir_init_src_folders(providers_distributions)

    if missing_init_dirs:
        with AIRFLOW_ROOT_PATH.joinpath("scripts/ci/license-templates/LICENSE.txt").open() as license:
            license_txt = license.readlines()
        prefixed_licensed_txt = [f"# {line}" if line != "\n" else "#\n" for line in license_txt]
        for missing_init_dir in missing_init_dirs:
            init_file = missing_init_dir / "__init__.py"
            init_file.write_text("".join(prefixed_licensed_txt))
            console.print(f"[yellow]Added missing __init__.py file:[/] {init_file}")
            should_fail = True

    for missing_extension_dir in missing_path_extension_dirs:
        init_file = missing_extension_dir / "__init__.py"
        init_file.write_text(init_file.read_text() + PATH_EXTENSION_STRING + "\n")
        console.print(f"[yellow]Added missing path extension to __init__.py file[/] {init_file}")
        should_fail = True

    if should_fail:
        console.print(
            "\n[yellow]The missing __init__.py files have been created. "
            "Please add these new files to a commit."
        )
        if fatal_error:
            console.print("[red]Also please remove the extra test folders listed above!")
        sys.exit(1)
    console.print("[green]All __init__.py files are present and have necessary extensions.[/]")
