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

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import get_directory_hash  # isort: skip # noqa E402
from common_precommit_black_utils import black_format  # isort: skip # noqa E402

AIRFLOW_SOURCES_PATH = Path(__file__).parents[3].resolve()
WWW_HASH_FILE = AIRFLOW_SOURCES_PATH / ".build" / "www" / "hash.txt"

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    www_directory = AIRFLOW_SOURCES_PATH / "airflow" / "www"
    WWW_HASH_FILE.parent.mkdir(exist_ok=True)
    old_hash = WWW_HASH_FILE.read_text() if WWW_HASH_FILE.exists() else ""
    new_hash = get_directory_hash(www_directory, skip_path_regexp=r".*node_modules.*")
    if new_hash == old_hash:
        print("The WWW directory has not changed! Skip regeneration.")
        sys.exit(0)
    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"
    subprocess.check_call(["yarn", "install", "--frozen-lockfile"], cwd=os.fspath(www_directory))
    subprocess.check_call(["yarn", "run", "build"], cwd=os.fspath(www_directory), env=env)
    WWW_HASH_FILE.write_text(new_hash)
