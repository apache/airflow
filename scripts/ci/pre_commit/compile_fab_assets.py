#!/usr/bin/env python3
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

import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

# NOTE!. This script is executed from node environment created by pre-commit and this environment
# Cannot have additional Python dependencies installed. We should not import any of the libraries
# here that are not available in stdlib! You should not import common_precommit_utils.py here because
# They are importing rich library which is not available in the node environment.

AIRFLOW_SOURCES_PATH = Path(__file__).parents[3].resolve()
WWW_HASH_FILE = AIRFLOW_SOURCES_PATH / ".build" / "www" / "hash.txt"


def get_directory_hash(directory: Path, skip_path_regexp: str | None = None) -> str:
    files = sorted(directory.rglob("*"))
    if skip_path_regexp:
        matcher = re.compile(skip_path_regexp)
        files = [file for file in files if not matcher.match(os.fspath(file.resolve()))]
    sha = hashlib.sha256()
    for file in files:
        if file.is_file() and not file.name.startswith("."):
            sha.update(file.read_bytes())
    return sha.hexdigest()


if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

INTERNAL_SERVER_ERROR = "500 Internal Server Error"


def compile_assets(www_directory: Path, www_hash_file_name: str):
    node_modules_directory = www_directory / "node_modules"
    dist_directory = www_directory / "static" / "dist"
    www_hash_file = AIRFLOW_SOURCES_PATH / ".build" / "www" / www_hash_file_name
    www_hash_file.parent.mkdir(exist_ok=True, parents=True)
    if node_modules_directory.exists() and dist_directory.exists():
        old_hash = www_hash_file.read_text() if www_hash_file.exists() else ""
        new_hash = get_directory_hash(www_directory, skip_path_regexp=r".*node_modules.*")
        if new_hash == old_hash:
            print(f"The '{www_directory}' directory has not changed! Skip regeneration.")
            return
    else:
        shutil.rmtree(node_modules_directory, ignore_errors=True)
        shutil.rmtree(dist_directory, ignore_errors=True)
    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"
    for try_num in range(3):
        print(f"### Trying to install yarn dependencies: attempt: {try_num + 1} ###")
        result = subprocess.run(
            ["yarn", "install", "--frozen-lockfile"],
            cwd=os.fspath(www_directory),
            text=True,
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            break
        if try_num == 2 or INTERNAL_SERVER_ERROR not in result.stderr + result.stdout:
            print(result.stdout + "\n" + result.stderr)
            sys.exit(result.returncode)
    subprocess.check_call(["yarn", "run", "build"], cwd=os.fspath(www_directory), env=env)
    new_hash = get_directory_hash(www_directory, skip_path_regexp=r".*node_modules.*")
    www_hash_file.write_text(new_hash)


if __name__ == "__main__":
    # Compile assets for fab provider
    fab_provider_www_directory = (
        AIRFLOW_SOURCES_PATH / "providers" / "fab" / "src" / "airflow" / "providers" / "fab" / "www"
    )
    compile_assets(fab_provider_www_directory, "hash_fab.txt")
